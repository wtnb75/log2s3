from logging import getLogger
import os
import sys
import datetime
import shlex
import subprocess
import functools
import click
import json
import pathlib
import boto3
import io
from typing import Union, Generator, Optional
from .version import VERSION
from .common_stream import Stream, MergeStream
from .compr_stream import S3GetStream, S3PutStream, \
    auto_compress_stream, stream_compress_modes
try:
    from mypy_boto3_s3.client import S3Client as S3ClientType
except ImportError:
    from typing import Any as S3ClientType

_log = getLogger(__name__)


mask_prefix = ["s3_"]


def arg_mask(d: Union[list, dict]) -> Union[list, dict]:
    if isinstance(d, list):
        return [arg_mask(x) for x in d]
    if isinstance(d, dict):
        res = d.copy()
        for p in mask_prefix:
            for k, v in d.items():
                if v is None:
                    res.pop(k)
                elif isinstance(v, str) and k.startswith(p):
                    res[k] = "*"*len(v)
                elif isinstance(v, (dict, list)):
                    res[k] = arg_mask(v)
                elif k.startswith(p):
                    res[k] = "****"
        return res
    return d


@click.group(invoke_without_command=True)
@click.version_option(VERSION)
@click.pass_context
def cli(ctx):
    if ctx.invoked_subcommand is None:
        print(ctx.get_help())


def s3_option(func):
    @click.option("--s3-access-key", envvar="AWS_ACCESS_KEY_ID", help="AWS Access Key")
    @click.option("--s3-secret-key", envvar="AWS_SECRET_ACCESS_KEY", help="AWS Secret Key")
    @click.option("--s3-region", envvar="AWS_DEFAULT_REGION", help="AWS Region")
    @click.option("--s3-endpoint", envvar="AWS_ENDPOINT_URL_S3", help="AWS Endpoint URL for S3")
    @click.option("--s3-bucket", envvar="AWS_S3_BUCKET", help="AWS S3 Bucket name")
    @click.option("--dotenv/--no-dotenv", default=False, help="load .env for S3 client config")
    @functools.wraps(func)
    def _(s3_endpoint, s3_access_key, s3_secret_key, s3_region, s3_bucket, dotenv, **kwargs):
        if dotenv:
            from dotenv import load_dotenv
            load_dotenv()
            if not s3_bucket:
                s3_bucket = os.getenv("AWS_S3_BUCKET")
        args = {
            'aws_access_key_id': s3_access_key,
            'aws_secret_access_key': s3_secret_key,
            'region_name': s3_region,
            'endpoint_url': s3_endpoint,
        }
        empty_keys = {k for k, v in args.items() if v is None}
        for k in empty_keys:
            args.pop(k)
        s3 = boto3.client('s3', **args)
        return func(s3=s3, bucket_name=s3_bucket, **kwargs)
    return _


compress_option = click.option(
    "--compress", default="gzip", type=click.Choice(stream_compress_modes),
    help="compress type", show_default=True)


def filetree_option(func):
    @click.option("--top", type=click.Path(dir_okay=True, exists=True, file_okay=False), required=True,
                  help="root directory to find files")
    @click.option("--older", help="find older file")
    @click.option("--newer", help="find newer file")
    @click.option("--date", help="find date range(YYYY-mm-dd[..YYYY-mm-dd])")
    @click.option("--bigger", help="find bigger file")
    @click.option("--smaller", help="find smaller file")
    @click.option("--glob", help="glob pattern")
    @click.option("--dry/--wet", help="dry run or wet run", default=False, show_default=True)
    @functools.wraps(func)
    def _(top, older, newer, date, bigger, smaller, glob, dry, **kwargs):
        config = {
            "top": top,
            "older": older,
            "newer": newer,
            "date": date,
            "bigger": bigger,
            "smaller": smaller,
            "glob": glob,
            "dry": dry,
        }
        return func(top=pathlib.Path(top), config=config, **kwargs)
    return _


def s3tree_option(func):
    @click.option("--prefix", default='', help="AWS S3 Object Key Prefix")
    @click.option("--suffix", default='', help="AWS S3 Object Key Suffix")
    @click.option("--older", help="find older file")
    @click.option("--newer", help="find newer file")
    @click.option("--date", help="find date range(YYYY-mm-dd[..YYYY-mm-dd])")
    @click.option("--bigger", help="find bigger file")
    @click.option("--smaller", help="find smaller file")
    @click.option("--glob", help="glob pattern")
    @functools.wraps(func)
    def _(prefix, older, newer, date, bigger, smaller, suffix, glob, **kwargs):
        config = {
            "top": prefix,
            "older": older,
            "newer": newer,
            "date": date,
            "bigger": bigger,
            "smaller": smaller,
            "suffix": suffix,
            "glob": glob,
        }
        if prefix:
            return func(top=pathlib.Path(prefix), config=config, **kwargs)
        return func(top="", config=config, **kwargs)
    return _


def verbose_option(func):
    @click.option("--verbose/--quiet", default=None)
    @functools.wraps(func)
    def _(verbose, **kwargs):
        from logging import basicConfig
        fmt = "%(asctime)s %(levelname)s %(name)s %(message)s"
        if verbose is None:
            basicConfig(level="INFO", format=fmt)
        elif verbose is False:
            basicConfig(level="WARNING", format=fmt)
        else:
            basicConfig(level="DEBUG", format=fmt)
        return func(**kwargs)
    return _


@cli.command()
@s3_option
@verbose_option
def s3_make_bucket(s3: S3ClientType, bucket_name: str):
    """make S3 buckets"""
    res = s3.create_bucket(Bucket=bucket_name)
    click.echo(f"response {res}")


@cli.command()
@s3_option
@verbose_option
def s3_bucket(s3: S3ClientType, bucket_name: str):
    """list S3 buckets"""
    res = s3.list_buckets()
    _log.debug("response %s", res)
    for bkt in res.get("Buckets", []):
        click.echo("%s %s" % (bkt["CreationDate"], bkt["Name"]))


def allobjs(s3: S3ClientType, bucket_name: str, prefix: str, marker: str = ''):
    res = s3.list_objects(Bucket=bucket_name, Prefix=prefix, Marker=marker)
    ct = res.get('Contents', [])
    yield from ct
    if res.get("IsTruncated") and len(ct) != 0:
        mk = ct[-1].get('Key')
        if mk:
            yield from allobjs(s3, bucket_name=bucket_name, prefix=prefix, marker=mk)


def s3obj2stat(obj: dict) -> os.stat_result:
    ts = obj.get("LastModified", datetime.datetime.now()).timestamp()
    sz = obj.get("Size", 0)
    return os.stat_result((0o644, 0, 0, 0, 0, 0, sz, ts, ts, ts))


def allobjs_conf(s3: S3ClientType, bucket_name: str, prefix: str, config: dict):
    _log.debug("allobjs: bucket=%s, prefix=%s, config=%s", bucket_name, prefix, config)
    from .processor import DebugProcessor
    dummy = DebugProcessor(config)
    suffix = config.get("suffix", "")
    objs = allobjs(s3, bucket_name, prefix)
    return filter(lambda x: x["Key"].endswith(suffix) and
                  dummy.check(pathlib.Path(x["Key"]), s3obj2stat(x)), objs)


@cli.command()
@s3_option
@s3tree_option
@verbose_option
def s3_list(s3: S3ClientType, bucket_name: str, config: dict, top: pathlib.Path):
    """list S3 objects"""
    topstr = str(top)
    if topstr == ".":
        topstr = ""
    for i in allobjs_conf(s3, bucket_name, topstr.lstrip("/"), config):
        click.echo("%s %6d %s" % (i["LastModified"], i["Size"], i["Key"]))


@cli.command()
@click.option("--summary/--no-summary", "-S", default=False, type=bool)
@click.option("--pathsep", default="/", show_default=True)
@s3_option
@s3tree_option
@verbose_option
def s3_du(s3: S3ClientType, bucket_name: str, config: dict, top: pathlib.Path, summary: bool, pathsep: str):
    """show S3 directory usage"""
    out = {}
    for i in allobjs_conf(s3, bucket_name, str(top).lstrip("/"), config):
        key = i["Key"]
        ks = key.rsplit(pathsep, 1)
        dirname = ks[0]
        sz = i["Size"]
        if dirname not in out:
            out[dirname] = [0, 0]
        out[dirname][0] += 1
        out[dirname][1] += sz
    if len(out) == 0:
        click.echo("(empty result)")
        return
    if summary:
        for korig in list(out.keys()):
            k = korig
            while len(k) != 0:
                k0 = k.rsplit(pathsep, 1)
                if len(k0) == 1:
                    break
                k = k0[0]
                if k not in out:
                    out[k] = [0, 0]
                out[k][0] += out[korig][0]
                out[k][1] += out[korig][1]
    click.echo("%10s %5s %s" % ("size", "cnt", "name"))
    click.echo("----------+-----+-----------------------")
    for k, v in sorted(out.items(), key=lambda f: f[1][1], reverse=True):
        click.echo("%10d %5d %s" % (v[1], v[0], k))


@cli.command()
@s3_option
@s3tree_option
@verbose_option
@click.option("--dry/--wet", help="dry run or wet run", default=False, show_default=True)
def s3_delete_by(s3: S3ClientType, bucket_name: str, top: pathlib.Path, config: dict, dry: bool):
    """delete S3 objects"""
    del_keys = [x["Key"] for x in allobjs_conf(s3, bucket_name, str(top).lstrip("/"), config)]
    if len(del_keys) == 0:
        _log.info("no object found")
    elif dry:
        _log.info("(dry)remove objects: %s", del_keys)
        click.echo(f"(dry)remove {len(del_keys)} objects")
    else:
        _log.info("(wet)remove %s objects", len(del_keys))
        s3.delete_objects(Bucket=bucket_name, Delete={"Objects": [{"Key": x} for x in del_keys]})


@cli.command()
@s3_option
@filetree_option
@verbose_option
@click.option("--prefix", default='', help="AWS S3 Object Prefix")
@click.option("--content/--stat", help="diff content or stat", default=False, show_default=True)
def s3_diff(s3: S3ClientType, bucket_name: str, prefix: str, top: pathlib.Path, config: dict, content: bool):
    """diff S3 and filetree"""
    all_keys = {pathlib.Path(x["Key"][len(prefix):]): x for x in allobjs_conf(s3, bucket_name, prefix, config)}
    from .processor import ListProcessor, process_walk
    lp = ListProcessor(config)
    process_walk(top, [lp])
    files = {k.relative_to(top): v for k, v in lp.output if v is not None}
    for k in set(all_keys.keys())-set(files.keys()):
        click.echo("only-s3: %s: %s" % (k, all_keys[k]))
    for k in set(files.keys())-set(all_keys.keys()):
        click.echo("only-file: %s: %s" % (k, files[k]))
    for k in set(files.keys()) & set(all_keys.keys()):
        if files[k].st_size != all_keys[k].get("Size"):
            click.echo("size mismatch %s file=%s, obj=%s" % (
                k, files[k].st_size, all_keys[k]["Size"]))


@cli.command()
@s3_option
@s3tree_option
@verbose_option
@compress_option
@click.option("--dry/--wet", help="dry run or wet run", default=False, show_default=True)
@click.option("--keep/--remove", help="keep old file or delete", default=True, show_default=True)
def s3_compress_tree(s3: S3ClientType, bucket_name: str, config: dict, top: pathlib.Path,
                     compress: str, dry: bool, keep: bool):
    """compress S3 objects"""
    topstr = str(top)
    if topstr == ".":
        topstr = ""
    for i in allobjs_conf(s3, bucket_name, topstr.lstrip("/"), config):
        rd = S3GetStream(s3, bucket=bucket_name, key=i["Key"])
        newname, data = auto_compress_stream(pathlib.Path(i["Key"]), compress, rd)
        if newname == i["Key"]:
            _log.debug("do nothing: %s", i["Key"])
            continue
        if dry:
            new_length = sum([len(x) for x in data.gen()])
            _log.info("(dry) recompress %s -> %s (%s->%s)", i["Key"], newname, i["Size"], new_length)
        else:
            ps = S3PutStream(data, s3, bucket=bucket_name, key=str(newname))
            for _ in ps.gen():
                pass
            res = s3.head_object(Bucket=bucket_name, Key=str(newname))
            _log.info("(wet) recompress %s -> %s (%s->%s)", i["Key"], newname, i["Size"], res["ContentLength"])
            if not keep:
                _log.info("remove old %s (->%s)", i["Key"], newname)
                s3.delete_object(Bucket=bucket_name, Key=i["Key"])


@cli.command()
@filetree_option
@verbose_option
def filetree_debug(top: pathlib.Path, config: dict):
    """(debug command)"""
    from .processor import DebugProcessor, process_walk
    proc = [DebugProcessor(config)]
    process_walk(top, proc)


@cli.command()
@filetree_option
@verbose_option
def filetree_list(top: pathlib.Path, config: dict):
    """list files"""
    from .processor import ListProcessor, process_walk
    lp = ListProcessor(config)
    process_walk(top, [lp])
    click.echo("%10s %-19s %s    %d(+%d) total" % ("size", "mtime", "name", lp.processed, lp.skipped))
    click.echo("----------+-------------------+-----------------------")
    for p, st in lp.output:
        if st is None:
            tmstr = "unknown"
            sz = -1
        else:
            tmstr = datetime.datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
            sz = st.st_size
        click.echo("%10s %19s %s" % (sz, tmstr, p))


@cli.command()
@filetree_option
@compress_option
@verbose_option
def filetree_compress(top: pathlib.Path, config: dict, compress):
    """compress files"""
    if compress:
        config["compress"] = compress
    from .processor import CompressProcessor, process_walk
    cproc = CompressProcessor(config)
    proc = [cproc]
    process_walk(top, proc)
    _log.info("compressed=%d, skipped=%d, size=%d->%d (%d bytes)",
              cproc.processed, cproc.skipped, cproc.before_total, cproc.after_total,
              cproc.before_total-cproc.after_total)


@cli.command()
@filetree_option
@verbose_option
def filetree_delete(top: pathlib.Path, config: dict):
    """remove files"""
    from .processor import DelProcessor, process_walk
    proc = [DelProcessor(config)]
    process_walk(top, proc)
    _log.info("removed=%d, skipped=%d", proc[0].processed, proc[0].skipped)


@cli.command()
@click.argument("files", type=click.Path(file_okay=True, dir_okay=True, exists=True, readable=True), nargs=-1)
@verbose_option
def merge(files: list[click.Path]):
    """merge sorted log files"""
    input_stream: list[Stream] = []
    for fn in files:
        p = pathlib.Path(str(fn))
        if p.is_file():
            _, ch = auto_compress_stream(p, "decompress")
            input_stream.append(ch)
        elif p.is_dir():
            for proot, _, pfiles in p.walk():
                for pfn in pfiles:
                    _, ch = auto_compress_stream(proot / pfn, "decompress")
                    input_stream.append(ch)
        else:
            _log.warning("%s is not a directory or file", p)

    for i in MergeStream(input_stream).text_gen():
        click.echo(i, nl=False)


@cli.command()
@s3_option
@click.option("--prefix", default='', help="AWS S3 Object Prefix")
@filetree_option
@compress_option
@verbose_option
def s3_put_tree(s3: S3ClientType, bucket_name: str, prefix: str, top: pathlib.Path, config: dict, compress):
    """compress and put log files to S3"""
    config["s3"] = s3
    config["s3_bucket"] = bucket_name
    config["s3_prefix"] = prefix
    config["skip_names"] = {x["Key"] for x in allobjs(s3, bucket_name, prefix)}
    config["compress"] = compress
    from .processor import S3Processor, process_walk
    proc = [S3Processor(config)]
    process_walk(top, proc)
    _log.info("processed=%d, skipped=%d, upload %d bytes", proc[0].processed, proc[0].skipped, proc[0].uploaded)


@cli.command()
@s3_option
@click.option("--key", required=True, help="AWS S3 Object Key")
@click.argument("filename", type=click.Path(file_okay=True, dir_okay=False, exists=True))
@compress_option
@verbose_option
def s3_put1(s3: S3ClientType, bucket_name: str, key: str, filename: str, compress: str):
    """put 1 file to S3"""
    from .compr_stream import S3PutStream, auto_compress_stream
    input_path = pathlib.Path(filename)
    _, st = auto_compress_stream(input_path, compress)
    ost = S3PutStream(st, s3, bucket_name, key)
    for _ in ost.gen():
        pass


def _s3_read_stream(s3: S3ClientType, bucket_name: str, key: str) -> Stream:
    res = S3GetStream(s3, bucket=bucket_name, key=key)
    _, res = auto_compress_stream(pathlib.Path(key), "decompress", res)
    return res


@cli.command()
@s3_option
@click.argument("keys", nargs=-1)
@verbose_option
def s3_cat(s3: S3ClientType, bucket_name: str, keys: list[str]):
    """concatinate compressed objects"""
    for key in keys:
        for d in _s3_read_stream(s3, bucket_name, key).gen():
            sys.stdout.buffer.write(d)


def _data_via_pager(input: Stream):
    pager_bin = os.getenv("LOG2S3_PAGER", os.getenv("PAGER", "less"))
    proc = subprocess.Popen(shlex.split(pager_bin), stdin=subprocess.PIPE)
    assert proc.stdin is not None  # for type check
    for d in input.gen():
        proc.stdin.write(d)
    proc.communicate()


@cli.command()
@s3_option
@click.argument("key")
@verbose_option
def s3_less(s3: S3ClientType, bucket_name: str, key: str):
    """view compressed object"""
    _data_via_pager(_s3_read_stream(s3, bucket_name, key))


@cli.command()
@s3_option
@click.argument("key")
@click.option("--dry/--wet", help="dry run or wet run", default=False, show_default=True)
@verbose_option
def s3_vi(s3: S3ClientType, bucket_name: str, key: str, dry):
    """edit compressed object and overwrite"""
    bindata = _s3_read_stream(s3, bucket_name, key).read_all().decode("utf-8")
    from .compr_stream import stream_ext, RawReadStream
    _, ext = os.path.splitext(key)
    if ext in stream_ext:
        def compress_fn(f: bytes) -> bytes:
            compress_st = stream_ext[ext][1]
            return compress_st(RawReadStream(f)).read_all()
    else:
        def compress_fn(f: bytes) -> bytes: return f
    newdata = click.edit(text=bindata)
    if newdata is not None and newdata != bindata:
        wr = compress_fn(newdata.encode("utf-8"))
        if dry:
            _log.info("(dry) changed. write back to %s (%d->%d(%d))", key, len(bindata), len(newdata), len(wr))
        else:
            _log.info("(wet) changed. write back to %s (%d->%d(%d))", key, len(bindata), len(newdata), len(wr))
            s3.put_object(Body=wr, Bucket=bucket_name, Key=key)
    else:
        _log.info("not changed")


@cli.command()
@s3_option
@click.argument("keys", nargs=-1)
@verbose_option
def s3_merge(s3: S3ClientType, bucket_name: str, keys: list[str]):
    """merge sorted log objects"""
    input_stream: list[Stream] = []
    for key in keys:
        input_stream.append(_s3_read_stream(s3, bucket_name, key))

    for i in MergeStream(input_stream).text_gen():
        click.echo(i, nl=False)


@cli.command()
@s3_option
@click.argument("keys", nargs=-1)
@click.option("--dry/--wet", help="dry run or wet run", default=False, show_default=True)
@verbose_option
def s3_del(s3: S3ClientType, bucket_name: str, keys: list[str], dry):
    """delete objects"""
    for k in keys:
        res = s3.head_object(Bucket=bucket_name, Key=k)
        click.echo("%s %s %s" % (res["LastModified"], res["ContentLength"], k))
    if dry:
        _log.info("(dry) delete %s keys", len(keys))
    else:
        _log.info("(wet) delete %s keys", len(keys))
        s3.delete_objects(Bucket=bucket_name, Delete={"Objects": [{"Key": x} for x in keys]})


@cli.command()
@s3_option
@click.argument("keys", nargs=-1)
@verbose_option
def s3_head(s3: S3ClientType, bucket_name: str, keys: list[str]):
    """delete objects"""
    for k in keys:
        res = s3.head_object(Bucket=bucket_name, Key=k)
        click.echo(f"{k} = {res}")


@cli.command()
@s3_option
@verbose_option
@click.option("--cleanup/--no-cleanup", default=False)
def s3_list_parts(s3: S3ClientType, bucket_name: str, cleanup):
    """list in-progress multipart upload"""
    res = s3.list_multipart_uploads(Bucket=bucket_name)
    if len(res.get("Uploads", [])) == 0:
        click.echo("(no in-progress multipart uploads found)")
    for upl in res.get("Uploads", []):
        click.echo("%s %s %s" % (upl["Initiated"], upl["UploadId"], upl["Key"]))
        if cleanup:
            _log.info("cleanup %s/%s", upl["Key"], upl["UploadId"])
            s3.abort_multipart_upload(Bucket=bucket_name, Key=upl["Key"], UploadId=upl["UploadId"])


@cli.command("cat")
@click.argument("files", type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True), nargs=-1)
@verbose_option
def cat_file(files: list[click.Path]):
    """concatinate compressed files"""
    for fn in files:
        _, data = auto_compress_stream(pathlib.Path(str(fn)), "decompress")
        for d in data.gen():
            sys.stdout.buffer.write(d)


@cli.command("less")
@click.argument("filename", type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True))
@verbose_option
def view_file(filename: str):
    """view compressed file"""
    _, data = auto_compress_stream(pathlib.Path(filename), "decompress")
    _data_via_pager(data)


@cli.command("vi")
@click.argument("filename", type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True))
@click.option("--dry/--wet", help="dry run or wet run", default=False, show_default=True)
@verbose_option
def edit_file(filename: str, dry):
    """edit compressed file and overwrite"""
    from .compr_stream import stream_ext, RawReadStream
    fname = pathlib.Path(filename)
    _, data = auto_compress_stream(fname, "decompress")
    _, ext = os.path.splitext(fname)
    if ext in stream_ext:
        def compress_fn(f: bytes) -> bytes:
            compress_st = stream_ext[ext][1]
            return compress_st(RawReadStream(f)).read_all()
    else:
        def compress_fn(f: bytes) -> bytes: return f
    bindata = data.read_all().decode('utf-8')
    newdata = click.edit(text=bindata)
    if newdata is not None and newdata != bindata:
        if dry:
            _log.info("(dry) changed. write back to %s", fname)
        else:
            _log.info("(wet) changed. write back to %s", fname)
            # mode, timestamp will be changed
            fname.write_bytes(compress_fn(newdata.encode("utf-8")))
    else:
        _log.info("not changed")


@cli.command()
@click.argument("file")
@click.option("--compress", default=None, type=click.Choice(list(set(stream_compress_modes)-{"raw", "decompress"})),
              help="compress type (default: all)", multiple=True)
def compress_benchmark(compress, file):
    """benchmark compress algorithm

    \b
    outputs:
        mode: mode string
        rate: compression rate (compressed size/original size)
        compress: compress throuput (original bytes/sec)
        decompress: decompress throuput (original bytes/sec)
    """
    import csv
    import timeit
    from .compr_stream import stream_map, stream_compress_modes, RawReadStream, Stream
    if not compress:
        compress = stream_compress_modes
    raw_data = pathlib.Path(file).read_bytes()

    def bench_comp(input_data: bytes):
        s1 = RawReadStream(input_data)
        s2 = comp_st(s1)
        for _ in s2.gen():
            pass

    def bench_decomp(input_data: bytes):
        s1 = RawReadStream(input_data)
        s2 = decomp_st(s1)
        for _ in s2.gen():
            pass

    wr = csv.writer(sys.stdout)
    wr.writerow(["mode", "rate", "compress", "decompress"])
    for c in compress:
        mode = c
        m = stream_map.get(mode)
        if not m:
            click.Abort(f"no such compress mode: {mode}")
            continue
        comp_st: type[Stream] = m[1]
        decomp_st: type[Stream] = m[2]
        compressed_data = comp_st(RawReadStream(raw_data)).read_all()
        assert raw_data == decomp_st(RawReadStream(compressed_data)).read_all()
        isz = len(raw_data)
        csz = len(compressed_data)
        rate = csz/isz
        cnum, csec = timeit.Timer(stmt='bench(input_data)', globals={
            "input_data": raw_data, "comp_st": comp_st, "bench": bench_comp
        }).autorange()
        dnum, dsec = timeit.Timer(stmt='bench(input_data)', globals={
            "input_data": compressed_data, "decomp_st": decomp_st, "bench": bench_decomp
        }).autorange()
        wr.writerow([str(x) for x in [mode, rate, isz*cnum/csec, isz*dnum/dsec]])


@cli.command()
@verbose_option
@click.option("--format", type=click.Choice(["combined", "common", "debug"]), default="combined", show_default=True)
@click.option("--nth", type=int, default=1, show_default=True, help="parse from n-th '{'")
@click.argument("file", type=click.Path(exists=True, file_okay=True, dir_okay=False))
def traefik_json_convert(file, nth, format):
    """
    convert traefik access-log(json) to other format

    \b
    traefik --accesslog=true --accesslog.format=json \\
        --accesslog.fields.defaultmode=keep \\
        --accesslog.fields.headers.defaultmode=keep
    """
    from .compr_stream import FileReadStream, auto_compress_stream
    from collections import defaultdict
    common = "%(ClientHost)s - %(ClientUsername)s [%(httptime)s]" \
        " \"%(RequestMethod)s %(RequestPath)s %(RequestProtocol)s\"" \
        " %(DownstreamStatus)d %(DownstreamContentSize)s"
    combined = "%(ClientHost)s - %(ClientUsername)s [%(httptime)s]" \
        " \"%(RequestMethod)s %(RequestPath)s %(RequestProtocol)s\"" \
        " %(DownstreamStatus)d %(DownstreamContentSize)s" \
        " \"%(request_Referer)s\" \"%(request_User-Agent)s\""
    dateformat = "%d/%b/%Y:%H:%M:%S %z"
    format_map = {
        "combined": combined,
        "common": common,
        "debug": "%s",
    }
    if file in (None, "-"):
        fp = sys.stdin.buffer
        path = pathlib.Path("-")
        ist = FileReadStream(fp)
    else:
        path = pathlib.Path(file)
        ist = None
    _, ofp = auto_compress_stream(path, "decompress", ist)
    fmt = format_map.get(format, combined)
    for line in ofp.text_gen():
        n = line.split('{', nth)
        jsonstr = '{' + n[-1]
        try:
            jsdata: dict = json.loads(jsonstr)
            if "time" in jsdata:
                ts = datetime.datetime.fromisoformat(jsdata.get("time", "")).astimezone()
                jsdata["httptime"] = ts.strftime(dateformat)
            click.echo(fmt % defaultdict(lambda: "-", **jsdata))
        except json.JSONDecodeError:
            _log.exception("parse error: %s", jsonstr)


def do_ible1(name: str, fn: click.Command, args: dict, dry: bool):
    _log.debug("name=%s, fn=%s, args=%s, dry=%s", name, fn, arg_mask(args),  dry)
    _log.info("start %s", name)
    if dry:
        _log.info("run(dry): %s %s", fn, arg_mask(args))
    else:
        _log.info("run(wet): %s %s", fn, arg_mask(args))
        assert fn.callback is not None
        fn.callback(**args)
    _log.info("end %s", name)


def convert_ible(data: Union[list[dict], dict]) -> list[dict]:
    if isinstance(data, dict):
        d = []
        _log.debug("convert %s", arg_mask(data))
        for k, v in data.items():
            name = v.pop("name", k)
            allow_fail = v.pop("allow-fail", None)
            ent = {"name": name, k: v}
            if allow_fail is not None:
                ent["allow-fail"] = allow_fail
            d.append(ent)
        _log.debug("converted: %s", arg_mask(d))
        data = d
    return data


def arg2arg(fn: click.Command, args: dict, baseparam: dict) -> dict:
    params = {}
    for opt in fn.params:
        if isinstance(opt.envvar, list) and len(opt.envvar) != 0:
            for ev in opt.envvar:
                if os.getenv(ev):
                    params[opt.name] = os.getenv(ev)
                    break
        elif isinstance(opt.envvar, str) and opt.envvar and os.getenv(opt.envvar):
            params[opt.name] = os.getenv(opt.envvar)
        elif opt.default or not opt.required:
            params[opt.name] = opt.default
    pnames = [x.name for x in fn.params]
    for name in pnames:
        if name in args:
            params[name] = args[name]
        elif name in baseparam:
            params[name] = baseparam[name]
    return params


def ible_gen(data: list[dict]) -> Generator[tuple[str, click.Command, dict, dict], None, None]:
    _log.debug("input: %s", arg_mask(data))
    if not isinstance(data, list):
        raise Exception(f"invalid list style: {type(data)}")
    baseparam = {}
    for v in data:
        _log.debug("exec %s", arg_mask(v))
        if not isinstance(v, dict):
            raise Exception(f"invalid dict style: {type(v)}")
        kw: set[str] = v.keys()-{"name", "allow-fail"}
        if len(kw) != 1:
            raise Exception(f"invalid command style: {kw}")
        cmd: str = kw.pop()
        args: dict = v[cmd]
        name: str = v.get("name", cmd)
        if not isinstance(args, dict):
            raise Exception(f"invalid args: {args}")
        if cmd == "params":
            baseparam.update(args)
            continue
        if cmd not in cli.commands:
            raise Exception(f"invalid command: {cmd} / {cli.commands.keys()}")
        if cmd.startswith("ible"):
            raise Exception(f"unsupported command: {cmd}")
        fn = cli.commands[cmd]
        yield name, fn, arg2arg(fn, args, baseparam), v


def do_ible(data: list[dict], dry: bool):
    for name, fn, args, v in ible_gen(data):
        try:
            do_ible1(name, fn, args, dry)
        except Exception as e:
            if not v.get("allow-fail"):
                _log.exception("error occured. stop")
                raise
            _log.info("failed. continue: %s", e)


def gen_sh(file: str) -> Generator[tuple[Optional[str], list[str]], None, None]:
    with open(file, "r") as fp:
        name = None
        for line in fp:
            line = line.lstrip().rstrip("\n")
            if line.startswith("#"):
                name = line.lstrip()[1:].strip()
                _log.debug("name: %s", name)
                continue
            tokens = list(shlex.shlex(line, punctuation_chars=True))
            while len(tokens) != 0 and tokens[-1] == "\\":
                try:
                    line += next(fp)
                except StopIteration:
                    break
                tokens = list(shlex.shlex(line, punctuation_chars=True))
            if len(tokens) == 0:
                continue
            yield name, tokens
            name = None


def sh_line2arg(cmdop: click.Command, tokens: list[str]) -> tuple[dict, bool]:
    args: dict[str, Union[str, bool]] = {}
    allow_fail = False
    if tokens[-2:] == ["||", "true"]:
        allow_fail = True
        tokens = tokens[:-2]
        _log.debug("allow fail: %s", tokens)
    tks = iter(tokens)
    for token in tks:
        if token.startswith("#"):
            # comment
            break
        for opt in cmdop.params:
            assert isinstance(opt.name, str)
            if token in opt.opts:
                _log.debug("option %s", opt)
                if getattr(opt, "is_flag", False):
                    _log.debug("true: %s", opt)
                    args[opt.name] = True
                else:
                    val = next(tks)
                    _log.debug("args: %s", arg_mask({opt.name: val}))
                    args[opt.name] = val
                break
            elif token in opt.secondary_opts:
                _log.debug("false: %s", opt)
                args[opt.name] = False
                break
        else:
            raise Exception(f"no such option: {token}")
    return args, allow_fail


def read_sh(file: str) -> list[dict]:
    res: list[dict] = []
    for name, tokens in gen_sh(file):
        if len(tokens) <= 1:
            continue
        if tokens[0] != "log2s3":
            continue
        _log.debug("tokens: %s", tokens[1:])
        cmd = tokens[1]
        if cmd not in cli.commands:
            raise Exception(f"no such subcommand: {cmd}")
        cmdop = cli.commands[cmd]
        args, allow_fail = sh_line2arg(cmdop, tokens[2:])
        _log.debug("result args: %s", arg_mask(args))
        ent = {
            "name": name or cmd,
            cmd: args,
        }
        if allow_fail:
            ent["allow-fail"] = True
        res.append(ent)
        name = None
    return res


def try_read(file: str) -> Union[list[dict], dict]:
    try:
        import tomllib
        with open(file, "rb") as fp:
            return tomllib.load(fp)
    except ValueError as e:
        _log.debug("toml error", exc_info=e)
    try:
        import yaml
        with open(file, "rb") as fp:
            return yaml.safe_load(fp)
    except (yaml.error.YAMLError, ImportError) as e:
        _log.debug("yaml error", exc_info=e)
    try:
        import json
        with open(file, "rb") as fp:
            return json.load(fp)
    except ValueError as e:
        _log.debug("json error", exc_info=e)
    try:
        return read_sh(file)
    except Exception as e:
        _log.debug("sh error", exc_info=e)
    raise Exception(f"cannot load {file}: unknown filetype")


@cli.command()
@verbose_option
@click.option("--dry/--wet", help="dry run or wet run", default=False, show_default=True)
@click.argument("file", type=click.Path(file_okay=True, dir_okay=False, readable=True, exists=True))
def ible_playbook(file, dry):
    """do log2s3-ible playbook"""
    do_ible(convert_ible(try_read(file)), dry)


def sh_dump(data, output):
    click.echo("#! /bin/sh", file=output)
    click.echo("set -eu", file=output)
    click.echo("", file=output)
    for name, fn, args, v in ible_gen(data):
        allow_fail = v.get("allow-fail")
        for comment in io.StringIO(name):
            click.echo(f"# {comment.rstrip()}", file=output)
        fnname: str = fn.name or name
        options: list[str] = [fnname]
        for k, v in args.items():
            opt = [x for x in fn.params if x.name == k][0]
            if opt.default == v:
                continue
            if isinstance(v, bool):
                if v:
                    options.append(opt.opts[0])
                else:
                    options.append(opt.secondary_opts[0])
            elif v is not None:
                options.append(opt.opts[0])
                options.append(v)
        optstr = shlex.join(options)
        suffixstr = ""
        if allow_fail:
            suffixstr = " || true"
        click.echo(f"log2s3 {optstr}{suffixstr}", file=output)
        click.echo("", file=output)


@cli.command()
@verbose_option
@click.option("--format", type=click.Choice(["yaml", "json", "sh"]))
@click.argument("file", type=click.Path(file_okay=True, dir_okay=False, readable=True, exists=True))
@click.option("--output", type=click.File("w"), default="-")
def ible_convert(file, format, output):
    """convert log2s3-ible playbook"""
    data: list[dict] = convert_ible(try_read(file))
    if format == "yaml":
        import yaml
        yaml.dump(data, stream=output, allow_unicode=True)
    elif format == "json":
        import json
        json.dump(data, fp=output, ensure_ascii=False, indent=2)
    elif format == "sh":
        sh_dump(data, output)
    else:
        raise Exception(f"unknown format: {format}")


@cli.command()
@click.argument("args", nargs=-1)
def sh(args):
    """execute /bin/sh"""
    subprocess.run(["sh", *args])


@cli.command()
@click.argument("args", nargs=-1)
def bash(args):
    """execute bash"""
    subprocess.run(["bash", *args])


@cli.command()
@verbose_option
@click.option("--host", default="0.0.0.0", show_default=True)
@click.option("--port", type=int, default=8000, show_default=True)
@click.option("--root", default=".", show_default=True)
@click.option("--prefix", default="")
@click.option("--debug/--no-debug", default=False, show_default=True)
def serve(prefix, root, host, port, debug):
    """start viewer"""
    from .app import update_config, router
    import uvicorn
    from fastapi import FastAPI
    update_config({"working_dir": root, })
    if prefix:
        update_config({"prefix": prefix, })
    app = FastAPI(debug=debug)
    app.include_router(router, prefix=prefix)
    uvicorn.run(app, host=host, port=port, log_config=None)


if __name__ == "__main__":
    cli()
