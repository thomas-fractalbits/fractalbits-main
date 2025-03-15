mod abort_multipart_upload;
mod delete_object;

pub use abort_multipart_upload::abort_multipart_upload;
pub use delete_object::delete_object;

pub enum DeleteEndpoint {
    AbortMultipartUpload(String),
    DeleteObject,
}
