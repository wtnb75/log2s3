import lzma
import bz2
import gzip
from logging import getLogger
import io

_log = getLogger(__name__)


class Stream:
    def __init__(self, prev_stream):
        self.prev = prev_stream

    # work as pass-thru stream
    def gen(self):
        """read part generator"""
        yield from self.prev.gen()

    def read_all(self) -> bytes:
        """read all content"""
        buf = io.BytesIO()
        for i in self.gen():
            _log.debug("read %d bytes", len(i))
            buf.write(i)
        _log.debug("finish read")
        return buf.getvalue()

    def text_gen(self):
        """readline generator"""
        rest = b""
        for i in self.gen():
            d = i.rfind(b'\n')
            if d != -1:
                buf0 = io.BytesIO(rest + i[:d+1])
                rest = i[d+1:]
                buf = io.TextIOWrapper(buf0)
                yield from buf
            else:
                rest = rest + i
        if rest:
            buf = io.TextIOWrapper(io.BytesIO(rest))
            yield from buf


class FileReadStream(Stream):
    def __init__(self, file_like: io.RawIOBase, bufsize=1024*1024):
        self.fd = file_like
        self.bufsize = bufsize

    def gen(self):
        while True:
            data = self.fd.read(self.bufsize)
            _log.debug("read file %d", len(data))
            if len(data) == 0:
                break
            yield data


class RawReadStream(Stream):
    def __init__(self, data: bytes, bufsize=1024*1024):
        self.fd = io.BytesIO(data)
        self.bufsize = bufsize

    def gen(self):
        while True:
            data = self.fd.read(self.bufsize)
            _log.debug("read file %d", len(data))
            if len(data) == 0:
                break
            yield data


class FileWriteStream(Stream):
    def __init__(self, prev_stream, file_like: io.RawIOBase, bufsize=1024*1024):
        super().__init__(prev_stream)
        self.fd = file_like
        self.bufsize = bufsize

    def gen(self):
        for i in self.prev.gen():
            yield self.fd.write(i)


class SimpleFilterStream(Stream):
    def __init__(self, prev_stream, filter_fn):
        super().__init__(prev_stream)
        self.filter_fn = filter_fn

    def gen(self):
        yield self.filter_fn(self.prev.read_all())


class ComprFlushStream(Stream):
    def __init__(self, prev_stream, compressor):
        super().__init__(prev_stream)
        self.compr = compressor

    def gen(self):
        for i in self.prev.gen():
            _log.debug("compress %d", len(i))
            yield self.compr.compress(i)
        _log.debug("flush")
        yield self.compr.flush()


class DecompStream(Stream):
    def __init__(self, prev_stream, decompressor):
        super().__init__(prev_stream)
        self.decompr = decompressor

    def gen(self):
        for i in self.prev.gen():
            yield self.decompr.decompress(i)


class XzCompressorStream(ComprFlushStream):
    def __init__(self, prev_stream):
        super().__init__(prev_stream, lzma.LZMACompressor(format=lzma.FORMAT_XZ))


class LzmaCompressorStream(ComprFlushStream):
    def __init__(self, prev_stream):
        super().__init__(prev_stream, lzma.LZMACompressor(format=lzma.FORMAT_ALONE))


class XzDecompressorStream(DecompStream):
    def __init__(self, prev_stream):
        super().__init__(prev_stream, lzma.LZMADecompressor(format=lzma.FORMAT_AUTO))


LzmaDecompressorStream = XzDecompressorStream


class Bz2CompressorStream(ComprFlushStream):
    def __init__(self, prev_stream):
        super().__init__(prev_stream, bz2.BZ2Compressor())


class Bz2DecompressorStream(DecompStream):
    def __init__(self, prev_stream):
        super().__init__(prev_stream, bz2.BZ2Decompressor())


class GzipCompressorStream(SimpleFilterStream):
    def __init__(self, prev_stream):
        super().__init__(prev_stream, gzip.compress)


class GzipDecompressorStream(SimpleFilterStream):
    def __init__(self, prev_stream):
        super().__init__(prev_stream, gzip.decompress)


stream_map = {
    "pass": ("", Stream, Stream),
    "gzip": (".gz", GzipCompressorStream, GzipDecompressorStream),
    "bzip2": (".bz2", Bz2CompressorStream, Bz2DecompressorStream),
    "xz": (".xz", XzCompressorStream, XzDecompressorStream),
    "lzma": (".lzma", LzmaCompressorStream, LzmaDecompressorStream),
}


try:
    import zstd

    class ZstdCompressorStream(SimpleFilterStream):
        def __init__(self, prev_stream):
            super().__init__(prev_stream, zstd.compress)

    class ZstdDecompressorStream(SimpleFilterStream):
        def __init__(self, prev_stream):
            super().__init__(prev_stream, zstd.decompress)

    stream_map["zstd"] = (".zstd", ZstdCompressorStream, ZstdDecompressorStream)

except ImportError:
    pass

try:
    import lz4.frame

    # lz4.frame.LZ4FrameCompressor does not work?
    class Lz4CompressorStream(SimpleFilterStream):
        def __init__(self, prev_stream):
            super().__init__(prev_stream, lz4.frame.compress)

    # lz4.frame.LZ4FrameDecompressor does not work?
    class Lz4DecompressorStream(SimpleFilterStream):
        def __init__(self, prev_stream):
            super().__init__(prev_stream, lz4.frame.decompress)

    stream_map["lz4"] = (".lz4", Lz4CompressorStream, Lz4DecompressorStream)

except ImportError:
    pass

try:
    import brotli

    class BrotliCompressorStream(Stream):
        def __init__(self, prev_stream):
            super().__init__(prev_stream)
            self.compr = brotli.Compressor()

        def gen(self):
            for i in self.prev.gen():
                yield self.compr.process(i)
            yield self.compr.flush()

    class BrotliDecompressorStream(Stream):
        def __init__(self, prev_stream):
            super().__init__(prev_stream)
            self.decompr = brotli.Decompressor()

        def gen(self):
            for i in self.prev.gen():
                yield self.decompr.process(i)

    stream_map["brotli"] = (".brotli", BrotliCompressorStream, BrotliDecompressorStream)

except ImportError:
    pass

try:
    import liblzfse

    class LzfseCompressorStream(SimpleFilterStream):
        def __init__(self, prev_stream):
            super().__init__(prev_stream, liblzfse.compress)

    class LzfseDecompressorStream(SimpleFilterStream):
        def __init__(self, prev_stream):
            super().__init__(prev_stream, liblzfse.decompress)

    stream_map["lzfse"] = (".lzfse", LzfseCompressorStream, LzfseDecompressorStream)

except ImportError:
    pass

try:
    import snappy

    class SnappyCompressorStream(ComprFlushStream):
        def __init__(self, prev_stream):
            super().__init__(prev_stream, snappy.StreamCompressor())

    class SnappyDecompressorStream(DecompStream):
        def __init__(self, prev_stream):
            super().__init__(prev_stream, snappy.StreamDecompressor())

    # does not work?
    # stream_map["snappy"] = (".snappy", SnappyCompressorStream, SnappyDecompressorStream)
except ImportError:
    pass

try:
    import lzo

    class LzoCompressorStream(SimpleFilterStream):
        def __init__(self, prev_stream):
            super().__init__(prev_stream, lzo.compress)

    class LzoDecompressorStream(SimpleFilterStream):
        def __init__(self, prev_stream):
            super().__init__(prev_stream, lzo.decompress)

    stream_map["lzo"] = (".lzo", LzoCompressorStream, LzoDecompressorStream)
except ImportError:
    pass

try:
    import zpaq

    class ZpaqCompressorStream(SimpleFilterStream):
        def __init__(self, prev_stream):
            super().__init__(prev_stream, zpaq.compress)

    class ZpaqDecompressorStream(SimpleFilterStream):
        def __init__(self, prev_stream):
            super().__init__(prev_stream, zpaq.decompress)

    stream_map["zpaq"] = (".zpaq", ZpaqCompressorStream, ZpaqDecompressorStream)

except ImportError:
    pass

try:
    import zopfli.gzip

    class ZopfliCompressorStream(SimpleFilterStream):
        def __init__(self, prev_stream):
            super().__init__(prev_stream, zopfli.gzip.compress)

    stream_map["zopfli"] = ("", ZopfliCompressorStream, GzipDecompressorStream)
except ImportError:
    pass


try:
    import zlib_ng.gzip_ng

    class ZlibNgCompressorStream(SimpleFilterStream):
        def __init__(self, prev_stream):
            super().__init__(prev_stream, zlib_ng.gzip_ng.compress)

    class ZlibNgDecompressorStream(SimpleFilterStream):
        def __init__(self, prev_stream):
            super().__init__(prev_stream, zlib_ng.gzip_ng.decompress)

    stream_map["zlib-ng"] = ("", ZlibNgCompressorStream, ZlibNgDecompressorStream)
except ImportError:
    pass

stream_ext = {v[0]: (k, *v[1:]) for k, v in stream_map.items()}
stream_compress_modes = list(stream_map.keys()) + ["decompress", "raw"]
