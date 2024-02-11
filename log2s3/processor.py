import os
import pathlib
import time
import shutil
from logging import getLogger
from typing import Optional
from abc import ABC, abstractmethod
from .compr import auto_compress, do_chain
import pytimeparse
import humanfriendly
import datetime


_log = getLogger(__name__)


class FileProcessor(ABC):
    def __init__(self, config: dict = {}):
        self.config = {k: v for k, v in config.items() if v is not None}

    def check_date_range(self, mtime: float) -> bool:
        if "older" in self.config:
            older = pytimeparse.parse(self.config["older"])
            if mtime > time.time()-older:
                return False
        if "newer" in self.config:
            newer = pytimeparse.parse(self.config["newer"])
            if mtime < time.time()-newer:
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

    def check_size_range(self, size: int):
        if "smaller" in self.config:
            smaller = humanfriendly.parse_size(self.config["smaller"], True)
            if size > smaller:
                return False
        if "bigger" in self.config:
            bigger = humanfriendly.parse_size(self.config["bigger"], True)
            if size < bigger:
                return False
        return True

    def check(self, fname: pathlib.Path, stat: Optional[os.stat_result]) -> bool:
        if stat is None:
            stat = fname.stat()
        return self.check_date_range(stat.st_mtime) and self.check_size_range(stat.st_size)

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
        self.output: list[tuple[pathlib.Path, os.stat_result]] = []

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
    def process(self, fname: pathlib.Path, stat: Optional[os.stat_result]) -> bool:
        compressor = self.config.get("compress", "gzip")
        newname, data = auto_compress(fname, compressor)
        newpath = pathlib.Path(newname)
        if newpath == fname:
            _log.debug("unchanged: fname=%s, stat=%s", fname, stat)
            return False
        pfx = os.path.commonprefix([fname, newpath])
        wr = do_chain(data)
        if self.config.get("dry", False):
            _log.info("(dry) compress fname=%s{%s->%s}, size=%s->%s", pfx, str(fname)[len(pfx):],
                      str(newpath)[len(pfx):], stat.st_size, len(wr))
        else:
            _log.info("(wet) compress fname=%s{%s->%s}, size=%s->%s", pfx, str(fname)[len(pfx):],
                      str(newpath)[len(pfx):], stat.st_size, len(wr))
            newpath.write_bytes(wr)
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
        wr = do_chain(data)
        if self.config.get("dry", False):
            _log.info("(dry) upload %s -> %s (%d->%d)", fname, obj_name, stat.st_size, len(wr))
        else:
            _log.info("upload %s -> %s (%d->%d)", fname, obj_name, stat.st_size, len(wr))
            self.s3.put_object(Body=wr, Bucket=self.bucket, Key=obj_name)
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
