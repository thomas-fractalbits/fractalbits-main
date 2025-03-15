mod block_data_stream;
mod put_object;
mod upload_part;

pub use put_object::put_object;
pub use upload_part::upload_part;

pub enum PutEndpoint {
    PutObject,
    UploadPart(u64, String),
}
