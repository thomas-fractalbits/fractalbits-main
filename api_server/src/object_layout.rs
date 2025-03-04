use crate::BlobId;
use rkyv::{Archive, Deserialize, Serialize};
use uuid::Uuid;

#[derive(Archive, Deserialize, Serialize, PartialEq)]
pub struct ObjectLayout {
    pub timestamp: u64,
    pub version_id: Uuid, // v4
    pub block_size: u32,
    pub state: ObjectState,
}

pub fn gen_version_id() -> Uuid {
    Uuid::new_v4()
}

impl ObjectLayout {
    pub const DEFAULT_BLOCK_SIZE: u32 = 1024 * 1024 - 256;

    #[inline]
    pub fn blob_id(&self) -> BlobId {
        match self.state {
            ObjectState::Normal(ref data) => data.blob_id,
            ObjectState::Mpu(_) => todo!(),
        }
    }

    #[inline]
    pub fn size(&self) -> u64 {
        match self.state {
            ObjectState::Normal(ref data) => data.size,
            ObjectState::Mpu(_) => todo!(),
        }
    }

    #[inline]
    pub fn etag(&self) -> String {
        match self.state {
            ObjectState::Normal(ref data) => data.etag.clone(),
            ObjectState::Mpu(_) => todo!(),
        }
    }

    #[inline]
    pub fn num_blocks(&self) -> usize {
        self.size().div_ceil(self.block_size as u64) as usize
    }
}

#[derive(Archive, Deserialize, Serialize, PartialEq)]
pub enum ObjectState {
    Normal(ObjectData),
    Mpu(MpuState),
}

#[derive(Archive, Deserialize, Serialize, PartialEq)]
pub enum MpuState {
    Uploading,
    Aborted,
    Completed { size: u64, etag: String },
}

/// Data stored in normal object or mpu parts
#[derive(Archive, Deserialize, Serialize, PartialEq)]
pub struct ObjectData {
    pub size: u64,
    pub etag: String,
    pub blob_id: BlobId,
}
