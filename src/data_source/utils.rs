use crate::data_source::schema::DataSourceFormat;
use crate::response::http_error::Exception;
use glob::glob;

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
    } else if file_name.ends_with(".xlsx") {
        Some(DataSourceFormat::XLSX)
    } else {
        None
    }
}

pub fn find_files(pattern: &str) -> Result<Vec<String>, Exception> {
    let mut files = Vec::new();

    for entry in glob(pattern)? {
        let path = entry?;
        if path.is_file() {
            files.push(path.to_str().unwrap().to_owned());
        }
    }
    Ok(files)
}
