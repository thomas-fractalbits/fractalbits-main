use crate::Command;
use bytemuck::{Pod, Zeroable};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use rpc_codec_common::MessageHeaderTrait;
use xxhash_rust::xxh64::xxh64;

#[repr(C)]
#[derive(Pod, Debug, Default, Clone, Copy, Zeroable)]
pub struct MessageHeader {
    /// Trace ID for distributed tracing
    pub trace_id: u64,

    /// A checksum covering only the remainder of this header.
    /// This allows the header to be trusted without having to fetch the associated body.
    /// Using xxhash64 & UINT32_MAX.
    pub checksum: u32,

    /// A checksum covering only the associated body after this header.
    /// Using xxhash64 & UINT32_MAX.
    pub checksum_body: u32,

    /// The size of the Header structure (always), plus any associated body.
    pub size: u32,

    /// Every request would be sent with a unique id, so the client can get the right response
    pub id: u32,

    /// The protocol command (method) for this message.
    /// i32 size, defined as protobuf enum type
    pub command: Command,

    /// Number of retry attempts for this request (0 = first attempt)
    pub retry_count: u8,

    /// Reserved for future use
    reserved: [u8; 3],
}

// Safety: Command is defined as protobuf enum type (i32), and 0 as Invalid. There is also no padding
// as verified from the zig side. With header checksum validation, we can also be sure no invalid
// enum value being interpreted.
unsafe impl Pod for Command {}
unsafe impl Zeroable for Command {}

impl MessageHeader {
    const _SIZE_OK: () = assert!(size_of::<Self>() == 32);
    pub const SIZE: usize = size_of::<Self>();

    pub fn encode(&self, dst: &mut BytesMut) {
        let bytes: &[u8] = bytemuck::bytes_of(self);
        dst.put(bytes);
    }

    pub fn decode_bytes(src: &Bytes) -> Self {
        let header_bytes = &src.chunk()[0..Self::SIZE];
        bytemuck::pod_read_unaligned::<Self>(header_bytes)
    }

    pub fn get_size_bytes(src: &mut BytesMut) -> usize {
        let offset = std::mem::offset_of!(MessageHeader, size);
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&src[offset..offset + 4]);
        u32::from_le_bytes(bytes) as usize
    }

    /// Calculate and set the checksum field for this header.
    /// The checksum covers all header fields after the checksum field itself.
    /// Using xxhash64 & UINT32_MAX.
    pub fn set_checksum(&mut self) {
        let checksum_offset = std::mem::offset_of!(MessageHeader, checksum);
        let bytes: &[u8] = bytemuck::bytes_of(self);
        let bytes_to_hash = &bytes[checksum_offset + size_of::<u32>()..Self::SIZE];
        let hash = xxh64(bytes_to_hash, 0);
        self.checksum = (hash & 0xFFFFFFFF) as u32;
    }

    /// Verify that the checksum field matches the calculated checksum.
    /// Returns true if valid, false if invalid.
    pub fn verify_checksum(&self) -> bool {
        let checksum_offset = std::mem::offset_of!(MessageHeader, checksum);
        let bytes: &[u8] = bytemuck::bytes_of(self);
        let bytes_to_hash = &bytes[checksum_offset + size_of::<u32>()..Self::SIZE];
        let hash = xxh64(bytes_to_hash, 0);
        let calculated = (hash & 0xFFFFFFFF) as u32;
        self.checksum == calculated
    }
}

impl MessageHeaderTrait for MessageHeader {
    const SIZE: usize = 32;

    fn encode(&self, dst: &mut BytesMut) {
        self.encode(dst)
    }

    fn decode(src: &[u8]) -> Self {
        bytemuck::pod_read_unaligned::<Self>(&src[..Self::SIZE])
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

    fn set_checksum(&mut self) {
        self.set_checksum()
    }

    fn verify_checksum(&self) -> bool {
        self.verify_checksum()
    }
}
