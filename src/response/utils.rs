use std::path::Path;
use urlencoding::encode;

pub fn get_encoded_file_name(path: &Path) -> Result<String, String> {
    if let Some(file_name) = path.file_name() {
        if let Some(file_name_str) = file_name.to_str() {
            Ok(encode(file_name_str).to_string())
        } else {
            Err("Failed to convert file name to string.".to_string())
        }
    } else {
        Err("Failed to get file name from path.".to_string())
    }
}