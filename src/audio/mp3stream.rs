//! MP3 encoding pipeline for live HTTP streaming.
//!
//! Encoded MPEG Layer III frames are emitted as soon as the encoder makes them
//! available. The stream deliberately is not finalized with a Xing/Info header:
//! those headers belong at the beginning of a completed file, but a live HTTP
//! stream has already transmitted its earlier frames.

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, unbounded};
use rusty_mp3::{Error as Mp3Error, Mp3Encoder, Mp3EncoderConfig};
use std::{
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            Ordering::{Acquire, Release},
        },
    },
    time::Duration,
};

use crate::{
    audio::rwstream::AudioSamples,
    globals::statics::THREAD_STACK,
    utils::ui_logger::{LogCategory, ui_log},
};

const MP3_TARGET_KBPS: f32 = 192.0;
const SILENCE_PERIOD_MS: u64 = 250;

#[derive(Clone)]
pub(crate) struct Mp3Channel {
    samples_rcvr: Receiver<AudioSamples>,
    pub(crate) mp3_in: Receiver<Vec<u8>>,
    mp3_out: Sender<Vec<u8>>,
    active: Arc<AtomicBool>,
    sample_rate: u32,
    channels: u16,
}

impl Mp3Channel {
    #[must_use]
    pub(crate) fn new(
        samples_chan: Receiver<AudioSamples>,
        sample_rate: u32,
        channels: u16,
    ) -> Self {
        let (mp3_out, mp3_in) = unbounded();
        Self {
            samples_rcvr: samples_chan,
            mp3_in,
            mp3_out,
            active: Arc::new(AtomicBool::new(false)),
            sample_rate,
            channels,
        }
    }

    pub(crate) fn run(&self) {
        if self.active.load(Acquire) {
            ui_log(LogCategory::Error, "MP3 encoder is already running.");
            return;
        }

        let samples_rcvr = self.samples_rcvr.clone();
        let mp3_out = self.mp3_out.clone();
        let active = self.active.clone();
        let sample_rate = self.sample_rate;
        let channels = self.channels;

        self.active.store(true, Release);
        let spawn_result = std::thread::Builder::new()
            .name("mp3_encoder".into())
            .stack_size(THREAD_STACK)
            .spawn(move || {
                // rusty_mp3's CBR path enables a bit-reservoir mode at common
                // MPEG-1 bitrates and therefore banks frames until finish(). A live
                // stream never finishes, so use its streaming-friendly VBR path with
                // an average bitrate target instead.
                let mut encoder = Mp3Encoder::new(Mp3EncoderConfig {
                    bitrate_kbps: 0,
                    vbr_quality: Some(MP3_TARGET_KBPS),
                });
                let silence_samples =
                    (sample_rate as usize * channels as usize * SILENCE_PERIOD_MS as usize) / 1000;
                let silence = vec![0.0f32; silence_samples.max(channels as usize)];
                let timeout = Duration::from_millis(SILENCE_PERIOD_MS);

                while active.load(Acquire) {
                    match samples_rcvr.recv_timeout(timeout) {
                        Ok(samples) => {
                            if !encode_and_drain(
                                &mut encoder,
                                &samples,
                                channels,
                                sample_rate,
                                &mp3_out,
                            ) {
                                break;
                            }
                        }
                        Err(RecvTimeoutError::Timeout) if active.load(Acquire) => {
                            if !encode_and_drain(
                                &mut encoder,
                                &silence,
                                channels,
                                sample_rate,
                                &mp3_out,
                            ) {
                                break;
                            }
                        }
                        Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => break,
                    }
                }

                active.store(false, Release);
                ui_log(LogCategory::Info, "MP3 encoder stopped.");
            });

        if let Err(error) = spawn_result {
            self.active.store(false, Release);
            ui_log(
                LogCategory::Error,
                &format!("Unable to start MP3 encoder thread: {error}"),
            );
        }
    }

    pub(crate) fn stop(&self) {
        self.active.store(false, Release);
    }
}

fn encode_and_drain(
    encoder: &mut Mp3Encoder,
    samples: &[f32],
    channels: u16,
    sample_rate: u32,
    output: &Sender<Vec<u8>>,
) -> bool {
    if let Err(error) = encoder.push_pcm_f32(samples, channels, sample_rate) {
        ui_log(
            LogCategory::Error,
            &format!("MP3 encoder input error: {error}"),
        );
        return false;
    }
    drain_encoder(encoder, output)
}

fn drain_encoder(encoder: &mut Mp3Encoder, output: &Sender<Vec<u8>>) -> bool {
    loop {
        match encoder.next_packet() {
            Ok(packet) => {
                if output.send(packet).is_err() {
                    return false;
                }
            }
            Err(Mp3Error::Again) => return true,
            Err(Mp3Error::Eof) => return false,
            Err(error) => {
                ui_log(
                    LogCategory::Error,
                    &format!("MP3 encoder output error: {error}"),
                );
                return false;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_encoder_emits_mpeg_frames_without_finish() {
        let mut encoder = Mp3Encoder::new(Mp3EncoderConfig {
            bitrate_kbps: 0,
            vbr_quality: Some(MP3_TARGET_KBPS),
        });
        let samples = vec![0.0f32; 48000 * 2 / 4];
        encoder.push_pcm_f32(&samples, 2, 48000).unwrap();

        let mut bytes = Vec::new();
        loop {
            match encoder.next_packet() {
                Ok(packet) => bytes.extend(packet),
                Err(Mp3Error::Again) => break,
                Err(error) => panic!("unexpected encoder error: {error}"),
            }
        }

        assert!(bytes.len() >= 4);
        assert_eq!(bytes[0], 0xff);
        assert_eq!(bytes[1] & 0xe0, 0xe0);
    }
}
