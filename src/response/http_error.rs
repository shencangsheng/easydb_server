use crate::response::http_error::HttpError::{BadRequest, FileNotFound};
use actix_web::http::StatusCode;
use actix_web::{HttpResponse, ResponseError};
use datafusion::common::DataFusionError;
use derive_more::{Display, Error};
use serde::Serialize;

#[derive(Serialize)]
pub struct HttpResponseResult<T> {
    pub(crate) resp_msg: String,
    pub(crate) data: Option<T>,
    pub(crate) resp_code: i32,
}

#[derive(Debug)]
struct ExceptionAttributes {
    status_code: StatusCode,
    resp_code: i32,
    resp_msg: String,
}

impl ExceptionAttributes {
    fn new(message: &String, status_code: StatusCode) -> Self {
        ExceptionAttributes {
            resp_code: 1,
            status_code,
            resp_msg: message.clone(),
        }
    }
}

#[derive(Debug, Display, Error, Clone)]
pub enum HttpError {
    #[display("Internal server error {message}")]
    BadRequest { message: String },
    #[display("File not found: {file_name}")]
    FileNotFound { file_name: String },
}

impl ResponseError for HttpError {
    fn error_response(&self) -> HttpResponse {
        self.log_error();
        let attributes = self.attributes();
        let error_response = HttpResponseResult::<String> {
            resp_msg: attributes.resp_msg,
            data: None,
            resp_code: attributes.resp_code,
        };
        HttpResponse::build(attributes.status_code).json(error_response)
    }
}

impl HttpError {
    fn attributes(&self) -> ExceptionAttributes {
        match self {
            BadRequest { message } => ExceptionAttributes::new(message, StatusCode::BAD_REQUEST),
            FileNotFound { file_name } => ExceptionAttributes::new(
                &format!("File not found: {}", file_name),
                StatusCode::NOT_FOUND,
            ),
        }
    }

    fn log_error(&self) {
        eprintln!("Error: {:?}", self);
    }
}

impl From<DataFusionError> for HttpError {
    fn from(error: DataFusionError) -> Self {
        BadRequest {
            message: error.to_string(),
        }
    }
}
