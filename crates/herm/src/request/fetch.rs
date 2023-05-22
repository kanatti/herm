use std::fmt::Display;
use thiserror::Error;

use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Error, Debug, PartialEq)]
pub enum FetchCreationError {
    #[error("Topic name is too long")]
    TopicTooLong,
    #[error("Malformed bytes")]
    MalformedBytes,
}

#[derive(Debug)]
pub struct Fetch {
    topic: String,
    partition: u32,
    offset: u64,
    size: u32,
}

impl Fetch {
    pub fn new(
        topic: String,
        partition: u32,
        offset: u64,
        size: u32,
    ) -> Result<Self, FetchCreationError> {
        // Check if topic name is longer than u16::MAX
        // We use u16 prefix to encode topic name length
        if (topic.len() as u32) > u16::MAX as u32 {
            return Err(FetchCreationError::TopicTooLong);
        }

        Ok(Fetch {
            topic,
            partition,
            offset,
            size,
        })
    }

    pub fn from_bytes(mut bytes: Bytes) -> Result<Self, FetchCreationError> {
        // Check if topic length is present
        if bytes.remaining() < 2 {
            return Err(FetchCreationError::MalformedBytes);
        }

        let topic_len = bytes.get_u16() as usize;

        // Check bytes has the right length
        if bytes.len() != topic_len + 4 + 8 + 4 {
            return Err(FetchCreationError::MalformedBytes);
        }

        let topic = String::from_utf8(bytes.slice(0..topic_len).to_vec())
            .map_err(|_| FetchCreationError::MalformedBytes)?;

        // Advance bytes over topic name
        bytes.advance(topic_len);

        Ok(Fetch {
            topic,
            partition: bytes.get_u32(),
            offset: bytes.get_u64(),
            size: bytes.get_u32(),
        })
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.size());

        // Write String with length prefix encoded as u16
        buf.put_u16(self.topic.len() as u16);
        buf.put(self.topic.as_bytes());

        // Write rest of the fields
        buf.put_u32(self.partition);
        buf.put_u64(self.offset);
        buf.put_u32(self.size);

        buf.freeze()
    }

    pub fn size(&self) -> usize {
        2 + self.topic.len() + 4 + 8 + 4
    }
}

impl Display for Fetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FetchRequest(topic:{}, part:{} offset:{} maxSize:{})",
            self.topic, self.partition, self.offset, self.size
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let fetch = Fetch::new("test".to_string(), 0, 0, 1024).unwrap();
        assert_eq!(fetch.topic, "test");
        assert_eq!(fetch.partition, 0);
        assert_eq!(fetch.offset, 0);
        assert_eq!(fetch.size, 1024);
    }

    #[test]
    fn test_new_long_topic_name() {
        let fetch = Fetch::new(
            String::from_utf8(vec![b'X'; (u16::MAX as usize) + 1]).unwrap(),
            0,
            0,
            1024,
        );

        assert!(fetch.is_err());
        assert_eq!(fetch.unwrap_err(), FetchCreationError::TopicTooLong)
    }

    #[test]
    fn test_from_bytes() {
        let fetch = Fetch::new("test".to_string(), 0, 0, 1024).unwrap();
        let fetch_bytes = fetch.to_bytes();
        let fetch_from_bytes = Fetch::from_bytes(fetch_bytes).unwrap();

        assert_eq!(fetch.topic, fetch_from_bytes.topic);
        assert_eq!(fetch.partition, fetch_from_bytes.partition);
        assert_eq!(fetch.offset, fetch_from_bytes.offset);
        assert_eq!(fetch.size, fetch_from_bytes.size);
    }

    #[test]
    fn test_malformed_bytes() {
        // Empty bytes
        let fetch = Fetch::from_bytes(Bytes::new());
        assert!(fetch.is_err());
        assert_eq!(fetch.unwrap_err(), FetchCreationError::MalformedBytes);

        // No u16 prefix
        let fetch = Fetch::from_bytes(Bytes::from_static(&[0x00]));
        assert!(fetch.is_err());
        assert_eq!(fetch.unwrap_err(), FetchCreationError::MalformedBytes);

        // Topic length is 4, but no topic name
        let fetch = Fetch::from_bytes(Bytes::from_static(&[0x00, 0x04]));
        assert!(fetch.is_err());
        assert_eq!(fetch.unwrap_err(), FetchCreationError::MalformedBytes);

        // Missing partition
        let fetch = Fetch::from_bytes(Bytes::from_static(&[
            0x00, 0x04, // Length of topic name
            b't', b'e', b's', b't', // Topic name
        ]));
        assert!(fetch.is_err());
        assert_eq!(fetch.unwrap_err(), FetchCreationError::MalformedBytes);

        // Missing offset
        let fetch = Fetch::from_bytes(Bytes::from_static(&[
            0x00, 0x04, // Length of topic name
            b't', b'e', b's', b't', // Topic name
            0x00, 0x00, 0x00, 0x06, // Partition
        ]));
        assert!(fetch.is_err());
        assert_eq!(fetch.unwrap_err(), FetchCreationError::MalformedBytes);

        // Missing size
        let fetch = Fetch::from_bytes(Bytes::from_static(&[
            0x00, 0x04, // Length of topic name
            b't', b'e', b's', b't', // Topic name
            0x00, 0x00, 0x00, 0x06, // Partition
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, // Offset
        ]));
        assert!(fetch.is_err());
        assert_eq!(fetch.unwrap_err(), FetchCreationError::MalformedBytes);

        // Works
        let fetch = Fetch::from_bytes(Bytes::from_static(&[
            0x00, 0x04, // Length of topic name
            b't', b'e', b's', b't', // Topic name
            0x00, 0x00, 0x00, 0x06, // Partition
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, // Offset
            0x00, 0x00, 0x00, 0x03, // Size
        ]));
        assert!(fetch.is_ok());

        let fetch = fetch.unwrap();
        assert_eq!(fetch.topic, "test");
        assert_eq!(fetch.partition, 6);
        assert_eq!(fetch.offset, 8);
        assert_eq!(fetch.size, 3);
    }

    #[test]
    fn test_size() {
        let fetch = Fetch::new("test".to_string(), 0, 0, 1024).unwrap();
        assert_eq!(fetch.size(), 2 + 4 + 8 + 4 + 4);
    }
}
