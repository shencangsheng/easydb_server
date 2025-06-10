use crate::controllers::HttpResponseResult;
use actix_web::http::StatusCode;
use actix_web::{HttpResponse, ResponseError};
use chrono::{DateTime, Utc};
use derive_more::{Display, Error};
use rand::distr::Alphanumeric;
use rand::Rng;
use serde::Deserialize;
use std::env;
use std::path::{Path, PathBuf};

#[derive(Debug, Display, Error, Clone)]
pub enum HttpError {
    #[display("File not found: {file_name}")]
    NotFound { file_name: String },
    #[display("Internal server error")]
    InternalServerError { error: String },
}

impl HttpError {
    fn status_code(&self) -> actix_web::http::StatusCode {
        match *self {
            HttpError::NotFound { .. } => StatusCode::NOT_FOUND,
            HttpError::InternalServerError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
    fn log_error(&self) {
        eprintln!("Error: {:?}", self);
    }
}

impl ResponseError for HttpError {
    fn error_response(&self) -> HttpResponse {
        self.log_error();
        let error_response = HttpResponseResult::<String> {
            resp_msg: match *self {
                HttpError::NotFound { ref file_name } => file_name.clone(),
                HttpError::InternalServerError { ref error } => error.clone(),
            },
            data: None,
            resp_code: 1,
        };
        HttpResponse::build(self.status_code()).json(error_response)
    }
}

#[derive(Debug, PartialEq)]
pub enum OperatingSystem {
    Windows,
    Linux,
    MacOS,
}

#[derive(Debug, PartialEq, Deserialize)]
pub enum FileType {
    CSV,
    JSON,
    DnJson,
}

impl OperatingSystem {
    pub fn default_data_dir(&self) -> &'static str {
        match self {
            OperatingSystem::Windows => "C:\\ProgramData\\easydb",
            OperatingSystem::Linux => "/var/lib/easydb",
            OperatingSystem::MacOS => concat!(env!("HOME"), "/Documents/easydb"),
        }
    }

    pub fn tmp_dir(&self) -> &'static str {
        match self {
            OperatingSystem::Windows => "C:\\Windows\\Temp\\",
            OperatingSystem::Linux => "/tmp/easydb/",
            OperatingSystem::MacOS => "/tmp/easydb/",
        }
    }
}

pub fn get_os() -> OperatingSystem {
    if cfg!(target_os = "windows") {
        OperatingSystem::Windows
    } else if cfg!(target_os = "linux") {
        OperatingSystem::Linux
    } else if cfg!(target_os = "macos") {
        OperatingSystem::MacOS
    } else {
        eprintln!("Error: Unknown operating system");
        std::process::exit(1);
    }
}

#[allow(dead_code)]
pub fn check_path_exists(path_str: &str) {
    let path = Path::new(path_str);

    if !path.exists() {
        panic!("路径 '{}' 不存在", path_str);
    }
}

#[allow(dead_code)]
pub fn is_directory(path_str: &str) -> bool {
    let path = Path::new(path_str);
    path.exists() && path.is_dir()
}

#[allow(dead_code)]
pub fn join_paths(base_path: &str, relative_path: &str) -> PathBuf {
    let base = Path::new(base_path);
    let full_path = base.join(relative_path);

    if !full_path.exists() {
        panic!("路径 '{}' 不存在", full_path.display());
    }

    full_path
}

pub fn is_relative_path(path: &str) -> bool {
    Path::new(path).is_relative()
}

pub fn get_file_type(file_name: &str) -> Option<FileType> {
    let file_name = file_name.trim_end_matches('\'');
    if file_name.ends_with(".csv") {
        Some(FileType::CSV)
    } else if file_name.ends_with(".json") {
        Some(FileType::JSON)
    } else if file_name.ends_with(".log") {
        Some(FileType::DnJson)
    } else {
        None
    }
}

pub fn time_difference_from_now(input_time: DateTime<Utc>) -> String {
    let now = Utc::now();
    let duration = now.signed_duration_since(input_time);

    if duration.num_milliseconds() < 1000 {
        format!("{}ms", duration.num_milliseconds())
    } else if duration.num_seconds() < 60 {
        format!("{}s", duration.num_seconds())
    } else if duration.num_minutes() < 60 {
        format!("{}m", duration.num_minutes())
    } else {
        format!("{}h", duration.num_hours())
    }
}

pub fn generate_random_string(length: usize) -> String {
    let mut rng = rand::rng();
    let random_string: String = (0..length)
        .map(|_| rng.sample(Alphanumeric) as char)
        .collect();
    random_string
}
