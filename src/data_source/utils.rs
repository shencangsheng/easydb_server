use crate::data_source::schema::DataSourceFormat;

pub fn get_format(file_name: &str) -> Option<DataSourceFormat> {
    let file_name = file_name.trim_end_matches('\'');
    if file_name.ends_with(".csv") {
        Some(DataSourceFormat::CSV)
    } else if file_name.ends_with(".json") {
        Some(DataSourceFormat::JSON)
    } else if file_name.ends_with(".log") {
        Some(DataSourceFormat::NdJson {
            file_extension: String::from(".log"),
        })
    } else if file_name.ends_with(".txt") {
        Some(DataSourceFormat::NdJson {
            file_extension: String::from(".txt"),
        })
    } else if file_name.ends_with(".tsv") {
        Some(DataSourceFormat::TSV)
    } else {
        None
    }
}
