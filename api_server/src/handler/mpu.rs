mod abort_multipart_upload;
mod complete_multipart_upload;
mod create_multipart_upload;
mod upload_part;

pub use abort_multipart_upload::abort_multipart_upload;
pub use complete_multipart_upload::complete_multipart_upload;
pub use create_multipart_upload::create_multipart_upload;
pub use upload_part::upload_part;

pub fn get_upload_part_key(mut key: String, part_number: u64) -> String {
    assert_eq!(Some('\0'), key.pop());
    key.push('%');
    // if part number is 0, we treat it as object key
    if part_number != 0 {
        key.push_str(&part_number.to_string());
    }
    key.push('\0');
    key
}
