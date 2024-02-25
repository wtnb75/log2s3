import lzma
import bz2
import gzip
import pathlib
from typing import Optional, Generator
from logging import getLogger
import io
import os
try:
    from mypy_boto3_s3.client import S3Client as S3ClientType
except ImportError:
    from typing import Any as S3ClientType

_log = getLogger(__name__)


class Stream:
    def __init__(self, prev_stream):
        self.prev = prev_stream

    def init_fp(self):
        _log.debug("use as fp(%s)", self.__class__.__name__)
        self.gen1 = self.prev.gen()
        self.buf = [next(self.gen1)]
        self.eof = False

    # work as pass-thru stream
    def gen(self) -> Generator[bytes, None, None]:
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

    def text_gen(self) -> Generator[str, None, None]:
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

    def read(self, sz: int = -1) -> bytes:
        assert hasattr(self, "eof")
        if self.eof:
            return b""
        if sz == -1:
            _log.debug("read all")
            try:
                while True:
                    self.buf.append(next(self.gen1))
            except StopIteration:
                _log.debug("read %s / %s", len(self.buf), sum([len(x) for x in self.buf]))
            buf = self.buf
            self.buf = []
            self.eof = True
            return b"".join(buf)
        cur = sum([len(x) for x in self.buf])
        try:
            _log.debug("read part cur=%s / sz=%s", cur, sz)
            while cur < sz:
                bt = next(self.gen1)
                _log.debug("read1 %d / cur=%s, sz=%s", len(bt), cur, sz)
                self.buf.append(bt)
                cur += len(bt)
            buf = b"".join(self.buf)
            self.buf = [buf[sz:]]
            _log.debug("return %s, rest=%s", sz, len(self.buf[0]))
            return buf[:sz]
        except StopIteration:
            _log.debug("eof %s / %s", len(self.buf), sum([len(x) for x in self.buf]))
            buf = self.buf
            self.buf = []
            self.eof = True
            return b"".join(buf)


class FileReadStream(Stream):
    def __init__(self, file_like: io.RawIOBase | io.BufferedReader, bufsize=10*1024*1024):
        self.fd = file_like
        self.bufsize = bufsize

    def gen(self):
        while True:
            data = self.fd.read(self.bufsize)
            if data is None or len(data) == 0:
                break
            _log.debug("read file %d", len(data))
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
    def __init__(self, prev_stream, file_like: io.RawIOBase | io.BufferedWriter, bufsize=1024*1024):
        super().__init__(prev_stream)
        self.fd = file_like
        self.bufsize = bufsize

    def gen(self):
        for i in self.prev.gen():
            yield self.fd.write(i)


class S3GetStream(Stream):
    def __init__(self, s3_client: S3ClientType, bucket: str, key: str, bufsize=1024*1024):
        self.obj = s3_client.get_object(Bucket=bucket, Key=key)
        self.bufsize = bufsize

    def gen(self):
        yield from self.obj["Body"].iter_chunks(self.bufsize)


class S3PutStream(Stream):
    def __init__(self, prev_stream, s3_client: S3ClientType, bucket: str, key: str, bufsize=1024*1024):
        super().__init__(prev_stream)
        self.client = s3_client
        self.bucket = bucket
        self.key = key
        self.bufsize = bufsize
        self.init_fp()
        _log.debug("eof is %s", self.eof)

    def gen(self):
        _log.debug("gen: bucket=%s, key=%s", self.bucket, self.key)
        self.client.upload_fileobj(self, self.bucket, self.key)
        yield b""


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


stream_map: dict[str, tuple[str, type[Stream], type[Stream]]] = {
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

    stream_map["brotli"] = (".br", BrotliCompressorStream, BrotliDecompressorStream)

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

stream_ext: dict[str, tuple[str, type[Stream], type[Stream]]] = {v[0]: (k, *v[1:]) for k, v in stream_map.items()}
stream_compress_modes = list(stream_map.keys()) + ["decompress", "raw"]


def auto_compress_stream(ifname: pathlib.Path, mode: str, ifp: Optional[Stream] = None) -> tuple[os.PathLike, Stream]:
    if ifp is None:
        ifp = FileReadStream(ifname.open('br'))
    if mode == "raw":
        return ifname, ifp
    base, ext = os.path.splitext(str(ifname))
    # decompress
    res: Stream = ifp
    if ext in stream_ext:
        imode, _, dst = stream_ext[ext]
        if imode == mode:
            return ifname, res
        _log.debug("input mode: %s", imode)
        res = dst(res)
    else:
        base = str(ifname)
    if mode == "decompress":
        return pathlib.Path(base), res
    # compress
    if mode in stream_map:
        ext, cst, _ = stream_map[mode]
        res = cst(res)
        base = base + ext
    return pathlib.Path(base), res
