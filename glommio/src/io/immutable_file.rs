// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::io::{
    dma_open_options::DmaOpenOptions,
    DmaStreamReaderBuilder,
    DmaStreamWriter,
    DmaStreamWriterBuilder,
    IoVec,
    ReadManyResult,
    ReadResult,
};

use std::path::Path;

use futures_lite::{future::poll_fn, io::AsyncWrite};
use std::{
    io,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};
type Result<T> = crate::Result<T, ()>;

#[derive(Debug)]
/// Builds a new [`ImmutableFile`], allowing linear and random access to a
/// Direct I/O [`DmaFile`].
///
/// Working with an [`ImmutableFile`] happens in two steps:
///
/// * First, all the writes are done, through the [`ImmutableFileSink`], which
///   has no read methods.
/// * Then, after the sink is [`seal`]ed, it is no longer possible to write to
///   it
///
/// [`DmaFile`]: struct.DmaFile.html
/// [`ImmutableFileSink`]: ImmutableFileSink
/// [`seal`]: ImmutableFileSink::seal
pub struct ImmutableFileBuilder<P>
where
    P: AsRef<Path>,
{
    max_merged_buffer_size: usize,
    max_read_amp: Option<usize>,
    concurrency: usize,
    buffer_size: usize,
    flush_disabled: bool,
    path: P,
}

#[derive(Debug)]
/// Sink portion of the [`ImmutableFile`]
///
/// To build it, use [`build_sink`] on the [`ImmutableFileBuilder`]
///
/// [`build_sink`]: ImmutableFileBuilder::build_sink
/// [`ImmutableFileBuilder`]: ImmutableFileBuilder
pub struct ImmutableFileSink {
    max_merged_buffer_size: usize,
    max_read_amp: Option<usize>,
    writer: DmaStreamWriter,
}

#[derive(Debug, Clone)]
/// A Direct I/O enabled file abstraction that can not be written to.
///
/// Glommio cannot guarantee that the file is not modified outside the process.
/// But so long as the file is only used within Glommio, using this API
/// guarantees that the file will never change.
///
/// This allows us to employ optimizations like caching and request coalescing.
/// The behavior of this API upon external modifications is undefined.
///
/// It can be read sequentially by building a stream with [`stream_reader`], or
/// randomly through the [`read_at`] (single read) or [`read_many`] (multiple
/// reads) APIs.
///
/// To build an `ImmutableFile`, use [`ImmutableFileBuilder`].
///
/// [`ImmutableFileBuilder`]: ImmutableFileBuilder
/// [`stream_reader`]: ImmutableFile::stream_reader
/// [`read_at`]: ImmutableFile::read_at
/// [`read_many`]: ImmutableFile::read_many
pub struct ImmutableFile {
    max_merged_buffer_size: usize,
    max_read_amp: Option<usize>,
    stream_builder: DmaStreamReaderBuilder,
}

impl<P> ImmutableFileBuilder<P>
where
    P: AsRef<Path>,
{
    /// Creates a new [`ImmutableFileBuilder`].
    ///
    /// If this is a new file, use [`build_sink`] to build the
    /// [`ImmutableFileSink`] stage that will allow writes until [`seal`].
    /// In that case, the filename must not exist. However, no error happens
    /// until [`build_sink`] is attempted.
    ///
    /// If this is an existing file that will be exclusively read, use
    /// [`build_existing`] to create an [`ImmutableFile`] directly, skipping
    /// the [`ImmutableFileSink`] stage
    ///
    /// [`ImmutableFile`]: ImmutableFile
    /// [`ImmutableFileSink`]: ImmutableFileSink
    /// [`seal`]: ImmutableFileSink::seal
    /// [`build_sink`]: ImmutableFileBuilder::build_sink
    /// [`build_existing`]: ImmutableFileBuilder::build_existing
    pub fn new(fname: P) -> Self {
        Self {
            path: fname,
            buffer_size: 128 << 10,
            concurrency: 10,
            max_merged_buffer_size: 1 << 20,
            max_read_amp: Some(128 << 10),
            flush_disabled: false,
        }
    }

    /// Define the number of buffers that will be used by the
    /// [`ImmutableFile`] during sequential access.
    ///
    /// This number is upheld for both the sink and read phases of the
    /// [`ImmutableFile`]. Higher numbers mean more concurrency but also
    /// more memory usage.
    ///
    /// [`ImmutableFile`]: ImmutableFile
    pub fn with_sequential_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Does not issue a sync operation when sealing the file. This is dangerous
    /// and in most cases may lead to data loss.
    ///
    /// Please note that even for `O_DIRECT` files, data may still be present in
    /// your device's internal cache until a sync happens.
    ///
    /// This option is ignored if you are creating an [`ImmutableFile`] directly
    /// and skipping the [`ImmutableFileSink`] step
    ///
    /// [`ImmutableFile`]: ImmutableFile
    /// [`ImmutableFileSink`]: ImmutableFileSink
    pub fn with_sync_on_close_disabled(mut self, flush_disabled: bool) -> Self {
        self.flush_disabled = flush_disabled;
        self
    }

    /// Define the buffer size that will be used by the sequential operations on
    /// this [`ImmutableFile`]
    ///
    /// [`ImmutableFile`]: ImmutableFile
    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// Builds an [`ImmutableFileSink`] with the properties defined by this
    /// builder.
    ///
    /// The resulting sink can be written to until [`seal`] is called.
    ///
    /// [`ImmutableFile`]: ImmutableFileSink
    /// [`new`]: ImmutableFileBuilder::new
    /// [`seal`]: ImmutableFileSink::seal
    pub async fn build_sink(self) -> Result<ImmutableFileSink> {
        println!("siink builder");
        let file = DmaOpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(self.path)
            .await?;
        let writer = DmaStreamWriterBuilder::new(file)
            .with_sync_on_close_disabled(self.flush_disabled)
            .with_buffer_size(self.buffer_size)
            .with_write_behind(self.concurrency)
            .build();

        Ok(ImmutableFileSink {
            writer,
            max_merged_buffer_size: self.max_merged_buffer_size,
            max_read_amp: self.max_read_amp,
        })
    }

    /// Builds an [`ImmutableFile`] with the properties defined by this
    /// builder.
    ///
    /// The resulting file cannot be written to. Glommio may optimize access
    /// patterns by assuming the file is read only. Writing to this file
    /// from an external process leads to undefined behavior.
    ///
    /// [`ImmutableFile`]: ImmutableFileSink
    /// [`new`]: ImmutableFileBuilder::new
    pub async fn build_existing(self) -> Result<ImmutableFile> {
        let file = Rc::new(
            DmaOpenOptions::new()
                .read(true)
                .write(false)
                .open(self.path)
                .await?,
        );
        let stream_builder = DmaStreamReaderBuilder::from_rc(file)
            .with_buffer_size(self.buffer_size)
            .with_read_ahead(self.concurrency);

        Ok(ImmutableFile {
            stream_builder,
            max_merged_buffer_size: self.max_merged_buffer_size,
            max_read_amp: self.max_read_amp,
        })
    }
}

impl ImmutableFileSink {
    /// Seals the file for further writes.
    ///
    /// Once this is called, it is no longer possible to write to the resulting
    /// [`ImmutableFile`]
    pub async fn seal(mut self) -> Result<ImmutableFile> {
        let stream_builder = poll_fn(|cx| self.writer.poll_seal(cx)).await?;

        Ok(ImmutableFile {
            stream_builder,
            max_merged_buffer_size: self.max_merged_buffer_size,
            max_read_amp: self.max_read_amp,
        })
    }
}

impl AsyncWrite for ImmutableFileSink {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.writer).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.writer).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.writer).poll_close(cx)
    }
}

impl ImmutableFile {
    /// Reads into buffer in buf from a specific position in the file.
    ///
    /// It is not necessary to respect the `O_DIRECT` alignment of the file, and
    /// this API will internally convert the positions and sizes to match,
    /// at a cost.
    pub async fn read_at(&self, pos: u64, size: usize) -> Result<ReadResult> {
        self.stream_builder.file.read_at(pos, size).await
    }

    /// Submit many reads and process the results in a stream-like fashion via a
    /// [`ReadManyResult`].
    ///
    /// This API will optimistically coalesce and deduplicate IO requests such
    /// that two overlapping or adjacent reads will result in a single IO
    /// request. This is transparent for the consumer, you will still
    /// receive individual ReadResults corresponding to what you asked for.
    ///
    /// The first argument is an iterator of [`IoVec`]. The last two
    /// arguments control how aggressive the IO coalescing should be:
    /// * `max_merged_buffer_size` controls how large a merged IO request can
    ///   be. A value of 0 disables merging completely.
    /// * `max_read_amp` is optional and defines the maximum read amplification
    ///   you are comfortable with. If two read requests are separated by a
    ///   distance less than this value, they will be merged. A value `None`
    ///   disables all read amplification limitation.
    ///
    /// It is not necessary to respect the `O_DIRECT` alignment of the file, and
    /// this API will internally convert the positions and sizes to match.
    pub fn read_many<V: IoVec, S: Iterator<Item = V>>(&self, iovs: S) -> ReadManyResult<V>
    where
        V: IoVec,
        S: Iterator<Item = V>,
    {
        self.stream_builder.file.clone().read_many(
            iovs,
            self.max_merged_buffer_size,
            self.max_read_amp,
        )
    }

    /// Creates a [`DmaStreamReaderBuilder`] from this `ImmutableFile`.
    ///
    /// The resulting builder can be augmented with any option available to the
    /// [`DmaStreamReaderBuilder`] and used to read this file sequentially.
    pub fn stream_reader(&self) -> DmaStreamReaderBuilder {
        self.stream_builder.clone()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{enclose, io::DmaFile, test_utils::make_tmp_test_directory};
    use futures::{AsyncReadExt, AsyncWriteExt};
    use futures_lite::stream::{self, StreamExt};

    macro_rules! immutable_file_test {
        ( $name:ident, $dir:ident, $code:block) => {
            #[test]
            fn $name() {
                let tmpdir = make_tmp_test_directory(stringify!($name));
                let $dir = tmpdir.path.clone();
                test_executor!(async move { $code });
            }
        };

        ( panic: $name:ident, $dir:ident, $code:block) => {
            #[test]
            #[should_panic]
            fn $name() {
                let tmpdir = make_tmp_test_directory(stringify!($name));
                let $dir = tmpdir.path.clone();
                test_executor!(async move { $code });
            }
        };
    }

    immutable_file_test!(panic: fail_on_already_existent, path, {
        let fname = path.join("testfile");
        DmaFile::create(&fname).await.unwrap();

        ImmutableFileBuilder::new(fname).build_sink().await.unwrap();
    });

    immutable_file_test!(panic: fail_reader_on_non_existent, path, {
        let fname = path.join("testfile");
        DmaFile::create(&fname).await.unwrap();

        ImmutableFileBuilder::new(fname)
            .build_existing()
            .await
            .unwrap_err();
    });

    immutable_file_test!(seal_and_stream, path, {
        let fname = path.join("testfile");
        let mut immutable = ImmutableFileBuilder::new(fname).build_sink().await.unwrap();
        immutable.write(&[0, 1, 2, 3, 4, 5]).await.unwrap();
        let stream = immutable.seal().await.unwrap();
        let mut reader = stream.stream_reader().build();

        let mut buf = [0u8; 128];
        let x = reader.read(&mut buf).await.unwrap();
        assert_eq!(x, 6);
    });

    immutable_file_test!(seal_and_random, path, {
        let fname = path.join("testfile");
        let mut immutable = ImmutableFileBuilder::new(fname).build_sink().await.unwrap();
        immutable.write(&[0, 1, 2, 3, 4, 5]).await.unwrap();
        let stream = immutable.seal().await.unwrap();

        let task1 = Task::local(enclose! { (stream) async move {
            let buf = stream.read_at(0, 6).await.unwrap();
            assert_eq!(&*buf, &[0, 1, 2, 3, 4, 5]);
        }});

        let task2 = Task::local(enclose! { (stream) async move {
            let buf = stream.read_at(0, 6).await.unwrap();
            assert_eq!(&*buf, &[0, 1, 2, 3, 4, 5]);
        }});

        assert_eq!(
            2,
            stream::iter(vec![task1, task2]).then(|x| x).count().await
        );
    });

    immutable_file_test!(seal_ready_many, path, {
        let fname = path.join("testfile");
        let mut immutable = ImmutableFileBuilder::new(fname).build_sink().await.unwrap();
        immutable.write(&[0, 1, 2, 3, 4, 5]).await.unwrap();
        let stream = immutable.seal().await.unwrap();

        let iovs = vec![(0, 1), (3, 1)];
        let mut bufs = stream.read_many(iovs.into_iter());
        let foo = bufs.next().await.unwrap();
        assert_eq!(foo.unwrap().1.len(), 1);
        let foo = bufs.next().await.unwrap();
        assert_eq!(foo.unwrap().1.len(), 1);
    });
}
