use std::path::{Path, PathBuf};

#[derive(Debug, PartialEq)]
pub enum OperatingSystem {
    Windows,
    Linux,
    MacOS,
}

#[derive(Debug, PartialEq)]
pub enum FileType {
    CSV,
    JSON,
}

impl OperatingSystem {
    pub fn default_data_dir(&self) -> &'static str {
        match self {
            OperatingSystem::Windows => "C:\\ProgramData\\easydb",
            OperatingSystem::Linux => "/var/lib/easydb",
            OperatingSystem::MacOS => "/tmp/easydb",
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
    if file_name.ends_with(".csv") {
        Some(FileType::CSV)
    } else if file_name.ends_with(".json") {
        Some(FileType::JSON)
    } else {
        None
    }
}