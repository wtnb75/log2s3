import os
import pathlib
from functools import partial
import gzip
import bz2
import lzma
modecmp_map = {
    "gzip": (".gz", gzip.decompress, gzip.compress),
    "bzip2": (".bz2", bz2.decompress, bz2.compress),
    "xz": (".xz", partial(lzma.decompress, format=lzma.FORMAT_AUTO),
           partial(lzma.compress, format=lzma.FORMAT_XZ)),
    "lzma": (".lzma", partial(lzma.decompress, format=lzma.FORMAT_AUTO),
             partial(lzma.compress, format=lzma.FORMAT_ALONE)),
}
try:
    import zstd
    modecmp_map["zstd"] = (".zstd", zstd.decompress, zstd.compress)
except ImportError:
    pass
try:
    import lz4.frame
    modecmp_map["lz4"] = (".lz4", lz4.frame.decompress, lz4.frame.compress)
except ImportError:
    pass
try:
    import brotli
    modecmp_map["brotli"] = (".br", brotli.decompress, brotli.compress)
except ImportError:
    pass
try:
    import liblzfse
    modecmp_map["lzfse"] = (".lzfse", liblzfse.decompress, liblzfse.compress)
except ImportError:
    pass
try:
    import zopfli.gzip
    modecmp_map["zopfli"] = ("", gzip.decompress, zopfli.gzip.compress)
except ImportError:
    pass
try:
    import snappy
    modecmp_map["snappy"] = (".snappy", snappy.decompress, snappy.compress)
except ImportError:
    pass
try:
    import py_snappy
    modecmp_map["py_snappy"] = ("", py_snappy.decompress, py_snappy.compress)
except ImportError:
    pass
try:
    import lzo
    modecmp_map["lzo"] = (".lzo", lzo.decompress, lzo.compress)
except ImportError:
    pass
try:
    import zlib_ng.gzip_ng
    modecmp_map["zlib-ng"] = ("", zlib_ng.gzip_ng.decompress, zlib_ng.gzip_ng.compress)
except ImportError:
    pass
try:
    import zpaq
    modecmp_map["zpaq"] = (".zpaq", zpaq.decompress, zpaq.compress)
except ImportError:
    pass

extcmp_map = {v[0]: (k, *v[1:]) for k, v in modecmp_map.items()}
compress_modes = list(modecmp_map.keys()) + ["decompress", "raw"]


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
    elif ext != "" and ext in extcmp_map:
        if mode == extcmp_map[ext][0]:
            return fname, resfn
        resfn.append(extcmp_map[ext][1])
    else:
        base = base + ext
        ext = ""

    if mode in modecmp_map:
        resfn.append(modecmp_map[mode][2])
        return base+modecmp_map[mode][0], resfn
    elif mode == "decompress":
        return base, resfn
    else:
        return base + ext, resfn
