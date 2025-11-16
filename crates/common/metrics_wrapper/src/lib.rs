#[cfg(feature = "metrics")]
#[macro_export]
macro_rules! counter {
    ($($t:tt)*) => { metrics::counter!($($t)*) };
}

#[cfg(not(feature = "metrics"))]
#[macro_export]
macro_rules! counter {
    ($($t:tt)*) => {
        $crate::NoOpCounter
    };
}

#[cfg(feature = "metrics")]
#[macro_export]
macro_rules! histogram {
    ($($t:tt)*) => { metrics::histogram!($($t)*) };
}

#[cfg(not(feature = "metrics"))]
#[macro_export]
macro_rules! histogram {
    ($($t:tt)*) => {
        $crate::NoOpHistogram
    };
}

#[cfg(feature = "metrics")]
#[macro_export]
macro_rules! gauge {
    ($($t:tt)*) => { metrics::gauge!($($t)*) };
}

#[cfg(not(feature = "metrics"))]
#[macro_export]
macro_rules! gauge {
    ($($t:tt)*) => {
        $crate::NoOpGauge
    };
}

#[cfg(feature = "metrics")]
pub use metrics::Gauge;

#[cfg(not(feature = "metrics"))]
pub struct Gauge;

#[cfg(not(feature = "metrics"))]
impl Gauge {
    #[inline(always)]
    pub fn increment(&self, _value: f64) {}

    #[inline(always)]
    pub fn decrement(&self, _value: f64) {}
}

#[cfg(not(feature = "metrics"))]
pub struct NoOpCounter;

#[cfg(not(feature = "metrics"))]
impl NoOpCounter {
    #[inline(always)]
    pub fn increment(self, _value: u64) {}

    #[inline(always)]
    pub fn absolute(self, _value: u64) {}
}

#[cfg(not(feature = "metrics"))]
pub struct NoOpHistogram;

#[cfg(not(feature = "metrics"))]
impl NoOpHistogram {
    #[inline(always)]
    pub fn record(self, _value: f64) {}
}

#[cfg(not(feature = "metrics"))]
pub struct NoOpGauge;

#[cfg(not(feature = "metrics"))]
impl NoOpGauge {
    #[inline(always)]
    pub fn increment(self, _value: f64) {}

    #[inline(always)]
    pub fn decrement(self, _value: f64) {}

    #[inline(always)]
    pub fn set(self, _value: f64) {}
}
