import io
from typing import Generator, Sequence
from logging import getLogger

_log = getLogger(__name__)


class Stream:
    """
    stream base class

    work as pass-through stream
    """

    def __init__(self, prev_stream):
        self.prev = prev_stream

    def init_fp(self):
        """prepare self as file-like interface"""
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
        """
        read up to size bytes

        used for file-like interface

        args:
            sz: read size. if size is not specified or -1, read all content.
        """
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


class CatStream:
    def __init__(self, inputs: list[Stream]):
        self.inputs = inputs

    def gen(self) -> Generator[bytes, None, None]:
        for i in self.inputs:
            yield from i.gen()


class MergeStream:
    def __init__(self, inputs: Sequence[Stream], bufsize: int = 4096):
        self.inputs = inputs
        self.bufsize = bufsize

    def gen(self) -> Generator[bytes, None, None]:
        buf = io.BytesIO()
        for i in self.text_gen():
            buf.write(i.encode("utf-8"))
            if buf.tell() > self.bufsize:
                yield buf.getvalue()
                buf.truncate(0)
                buf.seek(0)
        yield buf.getvalue()

    def text_gen(self) -> Generator[str, None, None]:
        txt_gens = [x.text_gen() for x in self.inputs]
        input_files = [[next(x), x] for x in txt_gens]
        input_files.sort(key=lambda f: f[0])
        while len(input_files) != 0:
            yield input_files[0][0]
            try:
                input_files[0][0] = next(input_files[0][1])
                if len(input_files) == 1 or input_files[0][0] < input_files[1][0]:
                    # already sorted
                    continue
            except StopIteration:
                input_files.pop(0)
            input_files.sort(key=lambda f: f[0])
