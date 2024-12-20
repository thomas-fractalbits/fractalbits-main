use axum::body::BodyDataStream;
use bytes::{Bytes, BytesMut};
use futures::{ready, Stream};
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::Poll;

pin_project! {
    pub struct BlockDataStream {
        #[pin]
        stream: BodyDataStream,
        block_data: BytesMut,
        block_size: u32,
    }
}

impl BlockDataStream {
    pub fn new(stream: BodyDataStream, block_size: u32) -> Self {
        assert!(block_size > 0);

        Self {
            stream,
            block_data: BytesMut::new(),
            block_size,
        }
    }

    fn take_block(self: Pin<&mut Self>) -> Bytes {
        let this = self.project();
        this.block_data.split_to(*this.block_size as usize).freeze()
    }
}

impl Stream for BlockDataStream {
    type Item = Bytes;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();
        loop {
            match ready!(this.stream.as_mut().poll_next(cx)) {
                Some(data) => {
                    this.block_data.extend(data.unwrap());
                    if this.block_data.len() >= *this.block_size as usize {
                        return Poll::Ready(Some(self.take_block()));
                    }
                }
                None => {
                    let last = if this.block_data.is_empty() {
                        None
                    } else {
                        let full_buf = std::mem::take(this.block_data);
                        Some(full_buf.freeze())
                    };

                    return Poll::Ready(last);
                }
            }
        }
    }
}
