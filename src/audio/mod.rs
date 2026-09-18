//! Audio subsystem: device enumeration, capture, sample conversion, and HTTP streaming.

pub mod audiodevices;
pub(crate) mod flacstream;
pub mod inject_silence;
pub(crate) mod mp3stream;
pub mod rwstream;
pub mod samples_conv;
