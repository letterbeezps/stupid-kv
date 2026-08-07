use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use lz4::{Decoder as Lz4Decoder, EncoderBuilder as Lz4EncoderBuilder};


#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum CompressionMode {
    // No compression
    #[default]
    None,

    // LZ4 compression
    Lz4,
}

pub(crate) struct CompressedWriter {
    inner: Box<dyn Write>
}

impl CompressedWriter {
    pub(crate) fn new<W: Write + 'static> (
        writer: W,
        mode: CompressionMode,
    ) -> std::io::Result<Self> {
        let inner: Box<dyn Write> = match mode {
            CompressionMode::None => {
                Box::new(BufWriter::new(writer))
            }
            CompressionMode::Lz4 => {
                let encoder = Lz4EncoderBuilder::new()
                .level(7)
                .build(writer)?;
                Box::new(encoder)
            }
        };

        Ok(Self { inner })
    }

    pub(crate) fn finish(self) -> io::Result<()> {
        Ok(())
    }
}

impl Write for CompressedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

pub(crate) struct CompressedReader {
    inner: Box<dyn Read>
}

impl CompressedReader {
    pub(crate) fn new<R: Read + 'static> (reader: R) -> io::Result<Self> {
        let mut buf_reader = BufReader::new(reader);
        let compression_mode = {
            // 读取数据到缓冲区，确保至少有 4 字节
            buf_reader.fill_buf()?;
            // 获取内部缓冲区数据
            let buffer = buf_reader.buffer();
            if buffer.len() >= 4 {
                // LZ4 magic number
                // 0x04224D18
                // 0x04: LZ4 version
                // 0x22: LZ4 version
                // 0x4D: LZ4 version
                // 0x18: LZ4 version
                if buffer[0..4] == [0x04, 0x22, 0x4D, 0x18] {
                    CompressionMode::Lz4
                } else {
                    CompressionMode::None
                }
            } else {
                CompressionMode::None
            }
        };
        tracing::debug!("Detected compression mode: {:?}", compression_mode);
        let inner: Box<dyn Read> = match compression_mode {
            CompressionMode::None => {
                Box::new(buf_reader)
            }
            CompressionMode::Lz4 => {
                let decoder = Lz4Decoder::new(buf_reader)?;
                Box::new(decoder)
            }
        };
        Ok(Self { inner })
    }
}

impl Read for CompressedReader {
	fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
		self.inner.read(buf)
	}
}