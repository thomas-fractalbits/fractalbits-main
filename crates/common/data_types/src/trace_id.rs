use std::fmt;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TraceId(pub u128);

impl TraceId {
    pub fn new() -> Self {
        Self(Uuid::now_v7().as_u128())
    }

    pub fn as_u128(&self) -> u128 {
        self.0
    }
}

impl Default for TraceId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

impl From<u128> for TraceId {
    fn from(id: u128) -> Self {
        Self(id)
    }
}

impl From<TraceId> for u128 {
    fn from(trace_id: TraceId) -> Self {
        trace_id.0
    }
}
