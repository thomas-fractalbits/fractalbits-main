//! Blob storage server message format.
//! Note if this file is updated, the corresponding message.zig file also needs to be updated!
use bytemuck::{Pod, Zeroable};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use rpc_codec_common::MessageHeaderTrait;

#[repr(C)]
#[derive(Pod, Default, Debug, Clone, Copy, Zeroable)]
pub struct MessageHeader {
    /// A checksum covering only the remainder of this header.
    /// This allows the header to be trusted without having to recv() or read() the associated body.
    checksum: u64,
    /// The current protocol version, note all the members until here should never be changed so
    /// that we can upgrade proto version in the future.
    pub proto_version: u32,
    /// The size of the Header structure (always), plus any associated body.
    pub size: u32,

    /// A checksum covering only the associated body after this header.
    checksum_body: u64,
    /// The protocol command (method) for this message. i32 size, defined as enum type
    pub command: Command,
    /// Every request would be sent with a unique id, so the client can get the right response
    pub id: u32,

    /// Bucket Id
    pub bucket_id: [u8; 16],

    /// Blob Id
    pub blob_id: [u8; 16],

    /// Trace ID for distributed tracing
    pub trace_id: u64,
    /// Version number for quorum protocol
    pub version: u64,

    /// The bss block number
    pub block_number: u32,
    /// Errno which can be converted into `std::io::Error`(`from_raw_os_error()`)
    pub errno: i32,
    /// 4k aligned size (header included), to use for direct-io
    pub aligned_size: u32,
    /// Volume ID for multi-BSS support
    pub volume_id: u16,
    /// Number of retry attempts for this request (0 = first attempt)
    pub retry_count: u8,
    /// Flag to indicate if this is a new metadata blob (vs update)
    pub is_new: u8,

    /// Reserved parts for padding
    // Note rust arrays of sizes from 0 to 32 (inclusive) implement the Default trait if the element
    // type allows it. As a stopgap, trait implementations are statically generated up to size 32.
    // See [doc](https://doc.rust-lang.org/std/primitive.array.html) for more details.
    reserved1: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(i32)]
pub enum Command {
    Invalid = 0,
    Handshake = 1, // Reserved for RPC handshake
    // Application-specific commands start from 16
    PutDataBlob = 16,
    GetDataBlob = 17,
    DeleteDataBlob = 18,
    PutMetadataBlob = 19,
    GetMetadataBlob = 20,
    DeleteMetadataBlob = 21,
}

#[allow(clippy::derivable_impls)]
impl Default for Command {
    fn default() -> Self {
        Command::Invalid
    }
}

// Safety: Command is defined as enum type (i32), and 0 as Invalid. There is also no padding
// as verified from the zig side. With header checksum validation, we can also be sure no invalid
// enum value being interpreted.
unsafe impl Pod for Command {}
unsafe impl Zeroable for Command {}

impl MessageHeader {
    const _SIZE_OK: () = assert!(size_of::<Self>() == 128);
    pub const SIZE: usize = size_of::<Self>();

    pub fn encode(&self, dst: &mut BytesMut) {
        let bytes: &[u8] = bytemuck::bytes_of(self);
        dst.put(bytes);
    }

    pub fn decode_bytes(src: &Bytes) -> Self {
        let header_bytes = &src.chunk()[0..Self::SIZE];
        // TODO: verify header checksum
        bytemuck::pod_read_unaligned::<Self>(header_bytes).to_owned()
    }

    pub fn get_size_bytes(src: &mut BytesMut) -> usize {
        let offset = std::mem::offset_of!(MessageHeader, size);
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&src[offset..offset + 4]);
        u32::from_le_bytes(bytes) as usize
    }
}

impl MessageHeaderTrait for MessageHeader {
    const SIZE: usize = 128;

    fn encode(&self, dst: &mut BytesMut) {
        self.encode(dst)
    }

    fn decode(src: &[u8]) -> Self {
        // TODO: verify header checksum
        bytemuck::pod_read_unaligned::<Self>(&src[..Self::SIZE]).to_owned()
    }

    fn get_size(src: &[u8]) -> usize {
        let offset = std::mem::offset_of!(MessageHeader, size);
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&src[offset..offset + 4]);
        u32::from_le_bytes(bytes) as usize
    }

    fn set_size(&mut self, size: u32) {
        self.size = size;
    }

    fn get_id(&self) -> u32 {
        self.id
    }

    fn set_id(&mut self, id: u32) {
        self.id = id;
    }

    fn get_body_size(&self) -> usize {
        (self.size as usize).saturating_sub(Self::SIZE)
    }

    fn get_retry_count(&self) -> u32 {
        self.retry_count.into()
    }

    fn set_retry_count(&mut self, retry_count: u32) {
        self.retry_count = retry_count as u8;
    }

    fn get_trace_id(&self) -> u64 {
        self.trace_id
    }

    fn set_trace_id(&mut self, trace_id: u64) {
        self.trace_id = trace_id;
    }
    fn set_checksum(&mut self) {}

    fn verify_checksum(&self) -> bool {
        true
    }
}
