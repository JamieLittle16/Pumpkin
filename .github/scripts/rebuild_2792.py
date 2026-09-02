from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if old not in text:
        raise SystemExit(f"missing seam {label}: {old!r}")
    return text.replace(old, new, 1)


def update_encoder() -> None:
    path = Path("crates/pumpkin-protocol/src/java/packet_encoder.rs")
    s = path.read_text()

    marker = "// raw -> compress -> encrypt\n\n"
    addition = r'''// raw -> compress -> encrypt

/// Upper bound on zlib output for `data_len` bytes without reallocating the destination.
const fn deflate_output_capacity(data_len: usize) -> usize {
    data_len.saturating_add(data_len / 16).saturating_add(64)
}

/// A validated Java packet payload: packet ID VarInt followed by packet fields.
///
/// This deliberately excludes outer length/compression framing and connection-local encryption.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SerializedPacket {
    bytes: Bytes,
}

impl SerializedPacket {
    pub fn try_from_bytes(bytes: Bytes) -> Result<Self, PacketEncodeError> {
        if bytes.is_empty() {
            return Err(PacketEncodeError::Message(
                "Serialized packet must contain a packet ID".into(),
            ));
        }
        if bytes.len() > MAX_PACKET_DATA_SIZE {
            return Err(PacketEncodeError::TooLong(bytes.len()));
        }

        let mut packet_id = 0u32;
        let mut complete = false;
        for (index, byte) in bytes.iter().copied().take(5).enumerate() {
            packet_id |= u32::from(byte & 0x7f) << (index * 7);
            if byte & 0x80 == 0 {
                complete = true;
                break;
            }
        }
        if !complete || packet_id > i32::MAX as u32 {
            return Err(PacketEncodeError::Message(
                "Serialized packet has an invalid packet ID VarInt".into(),
            ));
        }

        Ok(Self { bytes })
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &Bytes {
        &self.bytes
    }

    #[must_use]
    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.bytes.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CompressionProfile {
    pub threshold: CompressionThreshold,
    pub level: CompressionLevel,
}

impl From<(CompressionThreshold, CompressionLevel)> for CompressionProfile {
    fn from((threshold, level): (CompressionThreshold, CompressionLevel)) -> Self {
        Self { threshold, level }
    }
}

/// Complete Java packet framing before connection-local encryption.
#[derive(Clone, Debug)]
pub struct PreparedPacket {
    bytes: Bytes,
    compression: Option<CompressionProfile>,
}

impl PreparedPacket {
    #[must_use]
    pub const fn as_bytes(&self) -> &Bytes {
        &self.bytes
    }

    #[must_use]
    pub const fn compression(&self) -> Option<CompressionProfile> {
        self.compression
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.bytes.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

pub fn prepare_packet(
    packet: &SerializedPacket,
    compression: Option<CompressionProfile>,
) -> Result<PreparedPacket, PacketEncodeError> {
    let packet_data = packet.as_bytes();
    let data_len = packet_data.len();
    let data_len_var_int = VarInt::try_from(data_len)
        .map_err(|_| PacketEncodeError::Message("Packet data length exceeds VarInt".into()))?;
    let mut output = Vec::with_capacity(data_len.saturating_add(10));

    if let Some(profile) = compression {
        if data_len >= profile.threshold {
            let mut compressor = Compress::new(Compression::new(profile.level), true);
            let mut compressed = Vec::with_capacity(deflate_output_capacity(data_len));
            let status = compressor
                .compress_vec(packet_data, &mut compressed, FlushCompress::Finish)
                .map_err(|err| PacketEncodeError::CompressionFailed(err.to_string()))?;
            if status != Status::StreamEnd {
                return Err(PacketEncodeError::CompressionFailed(format!(
                    "Unexpected compressor status: {status:?}"
                )));
            }
            let packet_len = VarInt::try_from(data_len_var_int.written_size() + compressed.len())
                .map_err(|_| PacketEncodeError::TooLong(data_len))?;
            packet_len
                .encode(&mut output)
                .map_err(|err| PacketEncodeError::Message(err.to_string()))?;
            data_len_var_int
                .encode(&mut output)
                .map_err(|err| PacketEncodeError::Message(err.to_string()))?;
            output.extend_from_slice(&compressed);
        } else {
            let packet_len = VarInt::try_from(VarInt(0).written_size() + data_len)
                .map_err(|_| PacketEncodeError::TooLong(data_len))?;
            packet_len
                .encode(&mut output)
                .map_err(|err| PacketEncodeError::Message(err.to_string()))?;
            VarInt(0)
                .encode(&mut output)
                .map_err(|err| PacketEncodeError::Message(err.to_string()))?;
            output.extend_from_slice(packet_data);
        }
    } else {
        data_len_var_int
            .encode(&mut output)
            .map_err(|err| PacketEncodeError::Message(err.to_string()))?;
        output.extend_from_slice(packet_data);
    }

    if output.len() > MAX_PACKET_SIZE as usize {
        return Err(PacketEncodeError::TooLong(output.len()));
    }

    Ok(PreparedPacket {
        bytes: output.into(),
        compression,
    })
}

'''
    s = replace_once(s, marker, addition, "encoder type insertion")

    old_reserve = '''        let reserve_hint = packet_data
            .len()
            .saturating_add(packet_data.len() / 16)
            .saturating_add(64);'''
    s = replace_once(
        s,
        old_reserve,
        "        let reserve_hint = deflate_output_capacity(packet_data.len());",
        "compression reserve",
    )

    flush_marker = '''    pub async fn flush(&mut self) -> Result<(), PacketEncodeError> {
'''
    prepared_methods = r'''    #[must_use]
    pub fn compression_profile(&self) -> Option<CompressionProfile> {
        self.compression.map(CompressionProfile::from)
    }

    pub fn frame_prepared_packet(
        &self,
        packet: &PreparedPacket,
        out: &mut Vec<u8>,
    ) -> Result<(), PacketEncodeError> {
        if packet.compression != self.compression_profile() {
            return Err(PacketEncodeError::Message(
                "Prepared packet compression does not match connection".into(),
            ));
        }
        out.reserve(packet.bytes.len());
        out.extend_from_slice(&packet.bytes);
        Ok(())
    }

    pub async fn write_prepared_packet(
        &mut self,
        packet: &PreparedPacket,
    ) -> Result<(), PacketEncodeError> {
        if packet.compression != self.compression_profile() {
            return Err(PacketEncodeError::Message(
                "Prepared packet compression does not match connection".into(),
            ));
        }
        self.write_frame(&packet.bytes).await
    }

    pub async fn flush(&mut self) -> Result<(), PacketEncodeError> {
'''
    s = replace_once(s, flush_marker, prepared_methods, "prepared writer methods")

    test_insert = r'''
    #[test]
    fn serialized_packet_validates_packet_id_boundary() {
        assert!(SerializedPacket::try_from_bytes(Bytes::new()).is_err());
        assert!(SerializedPacket::try_from_bytes(Bytes::from_static(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x00])).is_err());
        assert!(SerializedPacket::try_from_bytes(Bytes::from_static(&[0x00])).is_ok());
    }

    #[test]
    fn prepared_packet_matches_current_framer() -> Result<(), Box<dyn std::error::Error>> {
        for compression in [
            None,
            Some(CompressionProfile { threshold: usize::MAX, level: 4 }),
            Some(CompressionProfile { threshold: 64 * 1024, level: 4 }),
            Some(CompressionProfile { threshold: 0, level: 4 }),
        ] {
            let packet = SerializedPacket::try_from_bytes(Bytes::from(vec![0x5a; 64 * 1024]))?;
            let prepared = prepare_packet(&packet, compression)?;
            let mut encoder = TCPNetworkEncoder::new(tokio::io::sink());
            if let Some(profile) = compression {
                encoder.set_compression((profile.threshold, profile.level));
            }
            let mut expected = Vec::new();
            encoder.frame_packet(packet.as_bytes(), &mut expected)?;
            assert_eq!(prepared.as_bytes().as_ref(), expected);
        }
        Ok(())
    }

    #[test]
    fn prepared_packet_rejects_different_compression_profile() {
        let packet = SerializedPacket::try_from_bytes(Bytes::from_static(b"\x00packet")).unwrap();
        let prepared = prepare_packet(
            &packet,
            Some(CompressionProfile { threshold: 0, level: 4 }),
        )
        .unwrap();
        let encoder = TCPNetworkEncoder::new(tokio::io::sink());
        let mut output = Vec::new();
        assert!(encoder.frame_prepared_packet(&prepared, &mut output).is_err());
        assert!(output.is_empty());
    }
'''
    pos = s.rfind("\n}")
    if pos < 0:
        raise SystemExit("missing encoder test module end")
    s = s[:pos] + test_insert + s[pos:]
    path.write_text(s)


def update_java_client() -> None:
    path = Path("crates/pumpkin/src/net/java/mod.rs")
    s = path.read_text()

    s = replace_once(
        s,
        "        packet_encoder::TCPNetworkEncoder,",
        "        packet_encoder::{PreparedPacket, SerializedPacket, TCPNetworkEncoder},",
        "java encoder import",
    )

    old_block = '''struct OutgoingPacket {
    data: Bytes,
    completion: Option<oneshot::Sender<()>>,
}

const MAX_FRAME_BATCH_DATA_SIZE: usize = MAX_PACKET_SIZE as usize;
'''
    new_block = '''struct OutgoingPacket {
    data: OutgoingPacketData,
    completion: Option<oneshot::Sender<()>>,
}

enum OutgoingPacketData {
    Serialized(SerializedPacket),
    Prepared(Arc<PreparedPacket>),
}

impl OutgoingPacketData {
    fn len(&self) -> usize {
        match self {
            Self::Serialized(packet) => packet.len(),
            Self::Prepared(packet) => packet.len(),
        }
    }

    fn needs_offload(&self, writer: &TCPNetworkEncoder<BufWriter<OwnedWriteHalf>>) -> bool {
        match self {
            Self::Serialized(packet) => writer.is_compressing_packet(packet.as_bytes()),
            Self::Prepared(_) => false,
        }
    }

    fn frame(
        &self,
        writer: &mut TCPNetworkEncoder<BufWriter<OwnedWriteHalf>>,
        out: &mut Vec<u8>,
    ) -> Result<(), PacketEncodeError> {
        match self {
            Self::Serialized(packet) => writer.frame_packet(packet.as_bytes(), out),
            Self::Prepared(packet) => writer.frame_prepared_packet(packet, out),
        }
    }
}

const MAX_FRAME_BATCH_DATA_SIZE: usize = MAX_PACKET_SIZE as usize;
'''
    s = replace_once(s, old_block, new_block, "outgoing data enum")

    s = replace_once(
        s,
        "        if let Err(err) = writer.frame_packet(&packet.data, &mut frame) {",
        "        if let Err(err) = packet.data.frame(&mut writer, &mut frame) {",
        "batch framing",
    )
    s = replace_once(
        s,
        "        .any(|packet| writer.is_compressing_packet(&packet.data));",
        "        .any(|packet| packet.data.needs_offload(&writer));",
        "batch offload predicate",
    )

    old_impl = '''impl OutgoingPacket {
    const fn normal(data: Bytes) -> Self {
        Self {
            data,
            completion: None,
        }
    }

    const fn high_priority(data: Bytes, completion: oneshot::Sender<()>) -> Self {
        Self {
            data,
            completion: Some(completion),
        }
    }
}
'''
    new_impl = '''impl OutgoingPacket {
    const fn normal(data: SerializedPacket) -> Self {
        Self {
            data: OutgoingPacketData::Serialized(data),
            completion: None,
        }
    }

    const fn high_priority(data: SerializedPacket, completion: oneshot::Sender<()>) -> Self {
        Self {
            data: OutgoingPacketData::Serialized(data),
            completion: Some(completion),
        }
    }

    const fn high_priority_prepared(
        data: Arc<PreparedPacket>,
        completion: oneshot::Sender<()>,
    ) -> Self {
        Self {
            data: OutgoingPacketData::Prepared(data),
            completion: Some(completion),
        }
    }
}
'''
    s = replace_once(s, old_impl, new_impl, "outgoing constructors")

    old_enqueue = '''    pub async fn enqueue_packet_data(&self, packet_data: Bytes) {
        if let Err(err) = self
            .outgoing_packet_queue_send
            .send(OutgoingPacket::normal(packet_data))
            .await
        {
            // This is expected to fail if we are closed
            if !self.close_token.is_cancelled() {
                warn!(
                    "Failed to add packet to the outgoing packet queue for client {}: {}",
                    self.id, err
                );
                // We now need to close the connection to the client since the stream is in an
                // unknown state
                self.close();
            }
        }
    }
'''
    new_enqueue = '''    pub async fn enqueue_packet_data(&self, packet_data: Bytes) {
        let packet = match SerializedPacket::try_from_bytes(packet_data) {
            Ok(packet) => packet,
            Err(err) => {
                error!("Failed to validate raw serialized packet: {err}");
                return;
            }
        };
        self.enqueue_serialized_packet(packet).await;
    }

    async fn enqueue_serialized_packet(&self, packet: SerializedPacket) {
        if let Err(err) = self
            .outgoing_packet_queue_send
            .send(OutgoingPacket::normal(packet))
            .await
        {
            if !self.close_token.is_cancelled() {
                warn!(
                    "Failed to add packet to the outgoing packet queue for client {}: {}",
                    self.id, err
                );
                self.close();
            }
        }
    }
'''
    s = replace_once(s, old_enqueue, new_enqueue, "async queue validation")

    old_try = '''    pub fn try_enqueue_packet_data(&self, packet_data: Bytes) {
        if let Err(err) = self
            .outgoing_packet_queue_send
            .try_send(OutgoingPacket::normal(packet_data))
        {
            match err {
                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                    debug!(
                        "Failed to add packet to the outgoing packet queue for client {}: channel full",
                        self.id
                    );
                }
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    if !self.close_token.is_cancelled() {
                        warn!(
                            "Failed to add packet to the outgoing packet queue for client {}: channel closed",
                            self.id
                        );
                        self.close();
                    }
                }
            }
        }
    }
'''
    new_try = '''    pub fn try_enqueue_packet_data(&self, packet_data: Bytes) {
        let packet = match SerializedPacket::try_from_bytes(packet_data) {
            Ok(packet) => packet,
            Err(err) => {
                error!("Failed to validate raw serialized packet: {err}");
                return;
            }
        };
        self.try_enqueue_serialized_packet(packet);
    }

    pub(crate) fn try_enqueue_serialized_packet(&self, packet: SerializedPacket) {
        if let Err(err) = self
            .outgoing_packet_queue_send
            .try_send(OutgoingPacket::normal(packet))
        {
            match err {
                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                    debug!(
                        "Failed to add packet to the outgoing packet queue for client {}: channel full",
                        self.id
                    );
                }
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    if !self.close_token.is_cancelled() {
                        warn!(
                            "Failed to add packet to the outgoing packet queue for client {}: channel closed",
                            self.id
                        );
                        self.close();
                    }
                }
            }
        }
    }
'''
    s = replace_once(s, old_try, new_try, "try queue validation")

    old_send = '''    pub async fn send_packet_now_data(&self, packet: Bytes) {
        let (completion_tx, completion_rx) = oneshot::channel();

        if let Err(err) = self
            .outgoing_packet_priority_send
            .send(OutgoingPacket::high_priority(packet, completion_tx))
            .await
        {
            // It is expected to fail if we are closed
            if !self.close_token.is_cancelled() {
                warn!(
                    "Failed to add high-priority packet to the outgoing packet queue for client {}: {}",
                    self.id, err
                );
                // We now need to close the connection to the client since the stream is in an
                // unknown state
                self.close();
            }
            return;
        }

        if completion_rx.await.is_err() && !self.close_token.is_cancelled() {
            // The outgoing packet task dropped before confirming the write.
            self.close();
        }
    }
'''
    new_send = '''    pub async fn send_packet_now_data(&self, packet: Bytes) {
        let packet = match SerializedPacket::try_from_bytes(packet) {
            Ok(packet) => packet,
            Err(err) => {
                error!("Failed to validate raw serialized packet: {err}");
                return;
            }
        };
        self.send_serialized_packet_now(packet).await;
    }

    async fn send_serialized_packet_now(&self, packet: SerializedPacket) {
        let (completion_tx, completion_rx) = oneshot::channel();

        if let Err(err) = self
            .outgoing_packet_priority_send
            .send(OutgoingPacket::high_priority(packet, completion_tx))
            .await
        {
            if !self.close_token.is_cancelled() {
                warn!(
                    "Failed to add high-priority packet to the outgoing packet queue for client {}: {}",
                    self.id, err
                );
                self.close();
            }
            return;
        }

        if completion_rx.await.is_err() && !self.close_token.is_cancelled() {
            self.close();
        }
    }

    pub async fn send_prepared_packet_now(&self, packet: Arc<PreparedPacket>) {
        let (completion_tx, completion_rx) = oneshot::channel();

        if let Err(err) = self
            .outgoing_packet_priority_send
            .send(OutgoingPacket::high_priority_prepared(packet, completion_tx))
            .await
        {
            if !self.close_token.is_cancelled() {
                warn!(
                    "Failed to add prepared packet to the outgoing queue for client {}: {}",
                    self.id, err
                );
                self.close();
            }
            return;
        }

        if completion_rx.await.is_err() && !self.close_token.is_cancelled() {
            self.close();
        }
    }
'''
    s = replace_once(s, old_send, new_send, "priority typed send")

    path.write_text(s)


update_encoder()
update_java_client()
