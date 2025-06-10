use serde::Serialize;

#[derive(Serialize)]
pub struct HttpResponseResult<T> {
    pub(crate) resp_msg: String,
    pub(crate) data: Option<T>,
    pub(crate) resp_code: i32,
}

#[derive(Serialize)]
pub struct HttpResponseError {
    pub(crate) resp_msg: String,
    pub(crate) resp_code: i32,
}