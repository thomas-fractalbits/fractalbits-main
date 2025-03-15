mod complete_multipart_upload;
mod create_multipart_upload;
mod delete_objects;

pub use complete_multipart_upload::complete_multipart_upload;
pub use create_multipart_upload::create_multipart_upload;
pub use delete_objects::delete_objects;

pub enum PostEndpoint {
    CompleteMultipartUpload(String),
    CreateMultipartUpload,
    DeleteObjects,
}
