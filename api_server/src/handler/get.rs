mod get_object;
mod get_object_attributes;
mod list_multipart_uploads;
mod list_objects;
mod list_objects_v2;
mod list_parts;

pub use get_object::get_object;
pub use get_object_attributes::get_object_attributes;
pub use list_multipart_uploads::list_multipart_uploads;
pub use list_objects::list_objects;
pub use list_objects_v2::list_objects_v2;
pub use list_parts::list_parts;

pub enum GetEndpoint {
    GetObject,
    GetObjectAttributes,
    ListMultipartUploads,
    ListObjects,
    ListObjectsV2,
    ListParts,
}
