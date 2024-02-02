import os
import pathlib
import time
import gzip
import bz2
import lzma
import shutil
from logging import getLogger
from typing import Optional
from abc import ABC, abstractmethod
import pytimeparse
import humanfriendly

_log = getLogger(__name__)


class FileProcessor(ABC):
    def __init__(self, config: dict = {}):
        self.config = config

    def check(self, fname: pathlib.Path, stat: Optional[os.stat_result]) -> bool:
        if stat is None:
            stat = fname.stat()
        if self.config.get("older"):
            older = pytimeparse.parse(self.config["older"])
            if stat.st_mtime > time.time()-older:
                return False
        if self.config.get("newer"):
            newer = pytimeparse.parse(self.config["newer"])
            if stat.st_mtime < time.time()-newer:
                return False
        if self.config.get("smaller"):
            smaller = humanfriendly.parse_size(self.config["smaller"], True)
            if stat.st_size > smaller:
                return False
        if self.config.get("bigger"):
            bigger = humanfriendly.parse_size(self.config["bigger"], True)
            if stat.st_size < bigger:
                return False
        return True

    @abstractmethod
    def process(self, fname: pathlib.Path, stat: Optional[os.stat_result]) -> bool:
        raise NotImplementedError()


class DebugProcessor(FileProcessor):
    def process(self, fname: pathlib.Path, stat: Optional[os.stat_result]) -> bool:
        _log.info("debug: fname=%s, stat=%s", fname, stat)
        return False


class DelProcessor(FileProcessor):
    def process(self, fname: pathlib.Path, stat: Optional[os.stat_result]) -> bool:
        if self.config.get("dry", False):
            _log.info("(dry) delete fname=%s, stat=%s", fname, stat)
        else:
            _log.info("(wet) delete fname=%s, stat=%s", fname, stat)
            fname.unlink()
        return True


def auto_compress(fname: pathlib.Path, mode: str = None) -> tuple[os.PathLike, bytes]:
    if mode is None:
        mode = "raw"
    base, ext = os.path.splitext(fname)
    rawdata = fname.read_bytes()
    if mode == "raw":
        bindata = rawdata
        base = base + ext
        ext = ""
    elif ext == ".gz":
        if mode == "gzip":
            return fname, rawdata
        bindata = gzip.decompress(rawdata)
    elif ext == ".bz2":
        if mode == "bzip2":
            return fname, rawdata
        bindata = bz2.decompress(rawdata)
    elif ext == ".xz":
        if mode == "xz":
            return fname, rawdata
        bindata = lzma.decompress(rawdata, lzma.FORMAT_AUTO)
    elif ext == ".lzma":
        if mode == "lzma":
            return fname, rawdata
        bindata = lzma.decompress(rawdata, lzma.FORMAT_AUTO)
    else:
        bindata = rawdata
        base = base + ext
        ext = ""

    if mode == "gzip":
        return base+".gz", gzip.compress(bindata)
    elif mode == "bzip2":
        return base+".bz2", bz2.compress(bindata),
    elif mode == "xz":
        return base+".xz", lzma.compress(bindata, lzma.FORMAT_XZ)
    elif mode == "lzma":
        return base+".lzma", lzma.compress(bindata, lzma.FORMAT_ALONE)
    elif mode == "decompress":
        return base, bindata
    else:
        return base + ext, rawdata


class CompressProcessor(FileProcessor):
    def process(self, fname: pathlib.Path, stat: Optional[os.stat_result]) -> bool:
        compressor = self.config.get("compress", "gzip")
        newname, data = auto_compress(fname, compressor)
        newpath = pathlib.Path(newname)
        if newpath == fname:
            _log.debug("unchanged: fname=%s, stat=%s", fname, stat)
            return False
        if self.config.get("dry", False):
            _log.info("(dry) compress fname=%s->%s, size=%s->%s", fname, newpath, stat.st_size, len(data))
        else:
            newpath.write_bytes(data)
            shutil.copystat(fname, newpath, follow_symlinks=False)
            fname.unlink()
        return True


class S3Processor(FileProcessor):
    def __init__(self, config):
        super().__init__(config)
        self.s3 = config.get("s3")
        self.prefix = config.get("s3_prefix")
        self.bucket = config.get("s3_bucket")
        self.skip_names = config.get("skip_names")
        self.top = config.get("top")

    def process(self, fname: pathlib.Path, stat: Optional[os.stat_result]) -> bool:
        compressor = self.config.get("compress", "gzip")
        newname, data = auto_compress(fname, compressor)
        newpath = pathlib.Path(newname)
        obj_name = self.prefix + str(newpath.relative_to(self.top))
        if obj_name in self.skip_names:
            _log.info("already exists: %s", obj_name)
            return True
        if self.config.get("dry", False):
            _log.info("(dry) upload %s -> %s (%d->%d)", fname, obj_name, stat.st_size, len(data))
        else:
            _log.info("upload %s -> %s (%d->%d)", fname, obj_name, stat.st_size, len(data))
            self.s3.put_object(Body=data, Bucket=self.bucket, Key=obj_name)
        return False


def process_walk(top: pathlib.Path, processors: list[FileProcessor]):
    for root, dirs, files in os.walk(top):
        for f in files:
            p = pathlib.Path(root, f)
            st = p.stat(follow_symlinks=False)
            for proc in processors:
                chk = proc.check(p, st)
                _log.debug("check %s(%s) -> %s", proc.__class__.__name__, p, chk)
                if chk:
                    res = proc.process(p, st)
                    _log.debug("process %s(%s) -> %s", proc.__class__.__name__, p, chk)
                    if res:
                        break