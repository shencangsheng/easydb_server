use chrono::{DateTime, Utc};
use rand::distr::Alphanumeric;
use rand::Rng;
use serde::Deserialize;
use std::env;
use std::path::{Path, PathBuf};

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
    TSV
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
