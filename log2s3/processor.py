import os
import pathlib
import time
import shutil
from logging import getLogger
from typing import Optional, Sequence
from abc import ABC, abstractmethod
from .compr_stream import auto_compress_stream, FileWriteStream, S3PutStream
import pytimeparse
import humanfriendly
import datetime
try:
    from mypy_boto3_s3.client import S3Client as S3ClientType
except ImportError:
    from typing import Any as S3ClientType


_log = getLogger(__name__)


class FileProcessor(ABC):
    def __init__(self, config: dict = {}):
        self.config = {k: v for k, v in config.items() if v is not None}
        self.processed = 0
        self.skipped = 0

    def check_date_range(self, mtime: float) -> bool:
        if "older" in self.config:
            older = pytimeparse.parse(self.config["older"])
            if older is not None and mtime > time.time()-older:
                return False
        if "newer" in self.config:
            newer = pytimeparse.parse(self.config["newer"])
            if newer is not None and mtime < time.time()-newer:
                return False
        if "date" in self.config:
            mtime_datetime = datetime.datetime.fromtimestamp(mtime)
            if ".." in self.config["date"]:
                fromdate, todate = [datetime.datetime.fromisoformat(
                    x) for x in self.config["date"].split("..", 1)]
                if not fromdate <= mtime_datetime < todate:
                    return False
            else:
                fromdate = datetime.datetime.fromisoformat(self.config["date"])
                todate = fromdate + datetime.timedelta(days=1)
                if not fromdate <= mtime_datetime < todate:
                    return False
        return True

    def check_size_range(self, size: int) -> bool:
        if "smaller" in self.config:
            smaller = humanfriendly.parse_size(self.config["smaller"], True)
            if size > smaller:
                return False
        if "bigger" in self.config:
            bigger = humanfriendly.parse_size(self.config["bigger"], True)
            if size < bigger:
                return False
        return True

    def check_name(self, fname: pathlib.Path) -> bool:
        if "suffix" in self.config:
            if not str(fname).endswith(self.config["suffix"]):
                return False
        if "prefix" in self.config:
            if not str(fname).startswith(self.config["prefix"]):
                return False
        if "glob" in self.config:
            if not fname.match(self.config["glob"]):
                return False
        if "iglob" in self.config:
            if not fname.match(self.config["iglob"], case_sensitive=False):
                return False
        return True

    def check(self, fname: pathlib.Path, stat: Optional[os.stat_result]) -> bool:
        if stat is None:
            stat = fname.stat()
        res = self.check_date_range(stat.st_mtime) and self.check_size_range(stat.st_size) and \
            self.check_name(fname)
        if res:
            self.processed += 1
        else:
            self.skipped += 1
        return res

    @abstractmethod
    def process(self, fname: pathlib.Path, stat: Optional[os.stat_result]) -> bool:
        raise NotImplementedError()


class DebugProcessor(FileProcessor):
    def check(self, fname: pathlib.Path, stat: Optional[os.stat_result]) -> bool:
        res = super().check(fname, stat)
        _log.debug("debug: fname=%s, stat=%s -> %s / %s", fname, stat, res, self.config)
        return res

    def process(self, fname: pathlib.Path, stat: Optional[os.stat_result]) -> bool:
        _log.info("debug: fname=%s, stat=%s", fname, stat)
        return False


class ListProcessor(FileProcessor):
    def __init__(self, config: dict = {}):
        super().__init__(config)
        self.output: list[tuple[pathlib.Path, Optional[os.stat_result]]] = []

    def process(self, fname: pathlib.Path, stat: Optional[os.stat_result]) -> bool:
        self.output.append((fname, stat))
        return False


class DelProcessor(FileProcessor):
    def process(self, fname: pathlib.Path, stat: Optional[os.stat_result]) -> bool:
        if self.config.get("dry", False):
            _log.info("(dry) delete fname=%s, stat=%s", fname, stat)
        else:
            _log.info("(wet) delete fname=%s, stat=%s", fname, stat)
            fname.unlink()
        return True


class CompressProcessor(FileProcessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.before_total = 0
        self.after_total = 0

    def process(self, fname: pathlib.Path, stat: Optional[os.stat_result]) -> bool:
        compressor = self.config.get("compress", "gzip")
        newname, data = auto_compress_stream(fname, compressor)
        newpath = pathlib.Path(newname)
        if newpath == fname:
            _log.debug("unchanged: fname=%s, stat=%s", fname, stat)
            self.skipped += 1
            self.processed -= 1
            return False
        pfx = os.path.commonprefix([fname, newpath])
        if isinstance(stat, os.stat_result):
            before_sz = stat.st_size
        else:
            before_sz = 0
        if self.config.get("dry", False):
            out_length = sum([len(x) for x in data.gen()])
            self.before_total += before_sz
            self.after_total += out_length
            _log.info("(dry) compress fname=%s{%s->%s}, size=%s->%s", pfx, str(fname)[len(pfx):],
                      str(newpath)[len(pfx):], before_sz, out_length)
        else:
            with newpath.open("wb") as ofp:
                wrs = FileWriteStream(data, ofp)
                for _ in wrs.gen():
                    pass
            out_length = newpath.stat().st_size
            self.before_total += before_sz
            self.after_total += out_length
            _log.info("(wet) compress fname=%s{%s->%s}, size=%s->%s", pfx, str(fname)[len(pfx):],
                      str(newpath)[len(pfx):], before_sz, out_length)
            shutil.copystat(fname, newpath, follow_symlinks=False)
            fname.unlink()
        return True


class S3Processor(FileProcessor):
    def __init__(self, config):
        super().__init__(config)
        self.s3: S3ClientType = config.get("s3")
        self.prefix = config.get("s3_prefix", "")
        self.bucket: str = config["s3_bucket"]
        self.skip_names = config.get("skip_names", [])
        self.top = config["top"]
        self.uploaded = 0

    def process(self, fname: pathlib.Path, stat: Optional[os.stat_result]) -> bool:
        compressor = self.config.get("compress", "gzip")
        newname, data = auto_compress_stream(fname, compressor)
        newpath = pathlib.Path(newname)
        base_name = newpath.relative_to(self.top)
        base_from = fname.relative_to(self.top)
        obj_name = self.prefix + str(base_name)
        if obj_name in self.skip_names:
            _log.debug("already exists: %s", obj_name)
            return True
        common_name = os.path.commonprefix([str(base_name), str(base_from)])
        rest1 = str(base_from)[len(common_name):]
        rest2 = str(base_name)[len(common_name):]
        reststr = ""
        if rest1 != rest2:
            reststr = "{%s,%s}" % (rest1, rest2)
        if isinstance(stat, os.stat_result):
            before_sz = stat.st_size
        else:
            before_sz = 0
        if self.config.get("dry", False):
            out_length = sum([len(x) for x in data.gen()])
            self.uploaded += out_length
            _log.info("(dry) upload {%s,%s}%s%s (%d->%d)",
                      self.top, self.prefix, common_name, reststr,
                      before_sz, out_length)
        else:
            outstr = S3PutStream(data, self.s3, bucket=self.bucket, key=obj_name)
            for _ in outstr.gen():
                pass
            res = self.s3.head_object(Bucket=self.bucket, Key=obj_name)
            out_length = res.get("ContentLength", 0)
            self.uploaded += out_length
            _log.info("(wet) upload {%s,%s}%s%s (%d->%d)",
                      self.top, self.prefix, common_name, reststr,
                      before_sz, out_length)
        return False


def process_walk(top: pathlib.Path, processors: Sequence[FileProcessor]):
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
