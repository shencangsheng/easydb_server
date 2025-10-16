use crate::data_source::utils::find_files;
use crate::response::http_error::Exception;
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow_array::{
    ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use calamine::{open_workbook, Data, DataType, HeaderRow, Range, Reader, Xlsx};
use chrono::NaiveDateTime;
use std::sync::Arc;

pub fn from_file_to_record_batch(file_path: &str) -> Result<RecordBatch, Exception> {
    let mut excel: (&str, Option<&str>) = (file_path, None);
    if let Some(index) = file_path.find('#') {
        excel = (&file_path[..index], Some(&file_path[index..]));
    }

    let files = find_files(file_path)?;
    let mut schemas: Option<Vec<Field>> = None;
    let mut records: Vec<Vec<Data>> = Vec::new();

    for file in files {
        let mut xlsx: Xlsx<_> = open_workbook(file)?;
        let sheet = match excel.1 {
            Some(s) => s,
            None => {
                let sheets = xlsx.sheet_names();
                if sheets.is_empty() {
                    return Err(Exception::unprocessable_entity_error("Sheets not found"));
                }
                match sheets.get(0) {
                    Some(s) => &s.clone(),
                    None => return Err(Exception::unprocessable_entity_error("Sheets not found")),
                }
            }
        };

        let r = xlsx
            .with_header_row(HeaderRow::Row(0))
            .worksheet_range(&sheet)?;

        if schemas.is_none() {
            schemas = Some(get_header_schema(&r)?);
            let schema = schemas.as_ref().unwrap();
            for _ in schema {
                records.push(Vec::new());
            }
        }

        for row in r.rows().skip(1) {
            row.iter().enumerate().for_each(|(i, cell)| {
                records.get_mut(i).unwrap().push(cell.clone());
            })
        }
    }

    let schemas = schemas.unwrap();
    let mut arrays: Vec<ArrayRef> = vec![];

    for (i, schema) in schemas.iter().enumerate() {
        let column_data = &records[i];

        let array: ArrayRef = match schema.data_type() {
            ArrowDataType::Utf8 => Arc::new(StringArray::from(
                column_data
                    .iter()
                    .map(|cell| cell.to_string())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            ArrowDataType::Int64 => Arc::new(Int64Array::from(
                column_data
                    .iter()
                    .map(|cell| cell.as_i64())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            ArrowDataType::Float64 => Arc::new(Float64Array::from(
                column_data
                    .iter()
                    .map(|cell| cell.as_f64())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None) => {
                Arc::new(TimestampNanosecondArray::from(
                    column_data
                        .iter()
                        .map(|cell| {
                            let date_str = cell.to_string();
                            NaiveDateTime::parse_from_str(&date_str, "%Y-%m-%d %H:%M:%S")
                                .map(|dt| dt.and_utc().timestamp_nanos_opt())
                                .unwrap_or_default()
                        })
                        .collect::<Vec<_>>(),
                )) as ArrayRef
            }
            _ => unimplemented!(),
        };

        arrays.push(array);
    }

    RecordBatch::try_new(Arc::new(Schema::new(schemas)), arrays).map_err(Exception::from)
}

pub fn get_header_schema(r: &Range<Data>) -> Result<Vec<Field>, Exception> {
    let schema = r.rows().next().map(|row| {
        row.iter()
            .map(|cell| {
                let data_type: ArrowDataType = determine_data_type(cell);
                Field::new(cell.to_string(), data_type, true)
            })
            .collect()
    });

    schema.ok_or(Exception::unprocessable_entity_error("Header not found"))
}

fn determine_data_type(cell: &Data) -> ArrowDataType {
    match cell {
        calamine::Data::String(_) => ArrowDataType::Utf8,
        calamine::Data::Float(_) => ArrowDataType::Float64,
        calamine::Data::Int(_) => ArrowDataType::Int32,
        calamine::Data::DateTime(_) => {
            ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None)
        }
        _ => ArrowDataType::Utf8,
    }
}
