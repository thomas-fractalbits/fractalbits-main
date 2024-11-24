use crate::BlobId;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Deserialize, Serialize)]
pub struct ObjectLayout {
    pub timestamp: u64,
    pub size: u64,
    pub blob_id: BlobId,
    // TODO: pub state: mpu related states, tombstone state, etc
}

impl ObjectLayout {
    const _SIZE_OK: () = assert!(size_of::<Self>() == 32);
    const _ALIGNMENT_OK: () = assert!(align_of::<Self>() == 8);
    pub const ALIGNMENT: usize = align_of::<Self>();
}
