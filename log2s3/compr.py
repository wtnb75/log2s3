import os
import pathlib
from functools import partial
import gzip
import bz2
import lzma
try:
    import zstd
    have_zstd = True
except ImportError:
    have_zstd = False
try:
    import lz4.frame
    have_lz4 = True
except ImportError:
    have_lz4 = False


def do_chain(funcs: list[callable]) -> bytes:
    res: bytes = funcs[0]()
    for f in funcs[1:]:
        res = f(res)
    return res


def auto_compress(fname: pathlib.Path, mode: str = None) -> tuple[os.PathLike, list[callable]]:
    resfn: list[callable] = []
    if mode is None:
        mode = "raw"
    base, ext = os.path.splitext(fname)
    resfn.append(fname.read_bytes)
    if mode == "raw":
        base = base + ext
        ext = ""
    elif ext == ".gz":
        if mode == "gzip":
            return fname, resfn
        resfn.append(gzip.decompress)
    elif ext == ".bz2":
        if mode == "bzip2":
            return fname, resfn
        resfn.append(bz2.decompress)
    elif ext == ".xz":
        if mode == "xz":
            return fname, resfn
        resfn.append(partial(lzma.decompress, format=lzma.FORMAT_AUTO))
    elif ext == ".lzma":
        if mode == "lzma":
            return fname, resfn
        resfn.append(partial(lzma.decompress, format=lzma.FORMAT_AUTO))
    elif ext == ".zstd" and have_zstd:
        if mode == "zstd":
            return fname, resfn
        resfn.append(zstd.decompress)
    elif ext == ".lz4" and have_zstd:
        if mode == "lz4":
            return fname, resfn
        resfn.append(lz4.frame.decompress)
    else:
        base = base + ext
        ext = ""

    if mode == "gzip":
        resfn.append(gzip.compress)
        return base+".gz", resfn
    elif mode == "bzip2":
        resfn.append(bz2.compress)
        return base+".bz2", resfn
    elif mode == "xz":
        resfn.append(partial(lzma.compress, format=lzma.FORMAT_XZ))
        return base+".xz", resfn
    elif mode == "lzma":
        resfn.append(partial(lzma.compress, format=lzma.FORMAT_ALONE))
        return base+".lzma", resfn
    elif mode == "zstd" and have_zstd:
        resfn.append(zstd.compress)
        return base+".zstd", resfn
    elif mode == "lz4" and have_lz4:
        resfn.append(lz4.frame.compress)
        return base+".lz4", resfn
    elif mode == "decompress":
        return base, resfn
    else:
        return base + ext, resfn
