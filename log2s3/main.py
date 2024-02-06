from logging import getLogger
import os
import sys
import subprocess
import functools
import click
import pathlib
import boto3
from .version import VERSION
from .compr import compress_modes

_log = getLogger(__name__)


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


def filetree_option(func):
    @click.option("--top", type=click.Path(dir_okay=True, exists=True, file_okay=False), required=True,
                  help="root directory to find files")
    @click.option("--older", help="find older file")
    @click.option("--newer", help="find newer file")
    @click.option("--bigger", help="find bigger file")
    @click.option("--smaller", help="find smaller file")
    @click.option("--dry/--wet", help="dry run or wet run", default=False, show_default=True)
    @click.option("--compress", default="gzip", type=click.Choice(compress_modes),
                  help="compress type", show_default=True)
    @functools.wraps(func)
    def _(top, older, newer, bigger, smaller, dry, compress, **kwargs):
        config = {
            "top": top,
            "older": older,
            "newer": newer,
            "bigger": bigger,
            "smaller": smaller,
            "dry": dry,
            "compress": compress,
        }
        return func(top=pathlib.Path(top), config=config, **kwargs)
    return _


def verbose_option(func):
    @click.option("--verbose/--quiet", default=None)
    @functools.wraps(func)
    def _(verbose, **kwargs):
        from logging import basicConfig
        fmt = "%(asctime)s %(levelname)s %(message)s"
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
def s3_make_bucket(s3: boto3.client, bucket_name: str):
    res = s3.create_bucket(Bucket=bucket_name)
    click.echo(f"response {res}")


@cli.command()
@s3_option
@verbose_option
def s3_bucket(s3: boto3.client, bucket_name: str):
    res = s3.list_buckets()
    click.echo(f"response {res}")


def allobjs(s3: boto3.client, bucket_name: str, prefix: str, marker: str = ''):
    res = s3.list_objects(Bucket=bucket_name, Prefix=prefix, Marker=marker)
    ct = res.get('Contents', [])
    yield from ct
    if res.get("IsTruncated") and len(ct) != 0:
        mk = ct[-1].get('Key')
        if mk:
            yield from allobjs(s3, bucket_name=bucket_name, prefix=prefix, marker=mk)


@cli.command()
@click.option("--prefix", default='', help="AWS S3 Object Prefix")
@s3_option
@verbose_option
def s3_list(s3: boto3.client, bucket_name: str, prefix: str):
    res = allobjs(s3, bucket_name, prefix)
    for i in res:
        click.echo("%s %6d %s" % (i["LastModified"], i["Size"], i["Key"]))


@cli.command()
@click.option("--prefix", default='', help="AWS S3 Object Prefix")
@click.option("--summary/--no-summary", "-S", default=False, type=bool)
@click.option("--pathsep", default="/", show_default=True)
@s3_option
@verbose_option
def s3_du(s3: boto3.client, bucket_name: str, prefix: str, summary: bool, pathsep: str):
    res = allobjs(s3, bucket_name, prefix)
    out = {}
    for i in res:
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
@verbose_option
@click.option("--prefix", default='', help="AWS S3 Object Prefix")
@click.option("--suffix", help="object key suffix")
@click.option("--older", help="find older file")
@click.option("--newer", help="find newer file")
@click.option("--bigger", help="find bigger file")
@click.option("--smaller", help="find smaller file")
@click.option("--dry/--wet", help="dry run or wet run", default=False, show_default=True)
def s3_delete_by(s3: boto3.client, bucket_name: str, prefix: str, suffix: str, dry: bool,
                 older: str, newer: str, bigger: str, smaller: str):
    import time
    import pytimeparse
    import humanfriendly
    to_del = allobjs(s3, bucket_name, prefix)
    now = time.time()
    # suffix
    if suffix:
        to_del = [x for x in to_del if x["Key"].endswith(suffix)]
    # older
    if older:
        check = now - pytimeparse.parse(older)
        to_del = [x for x in to_del if x["LastModified"].timestamp() < check]
    # newer
    if newer:
        check = now - pytimeparse.parse(newer)
        to_del = [x for x in to_del if x["LastModified"].timestamp() > check]
    # bigger
    if bigger:
        check = humanfriendly.parse_size(bigger)
        to_del = [x for x in to_del if x["Size"] > check]
    # smaller
    if smaller:
        check = humanfriendly.parse_size(smaller)
        to_del = [x for x in to_del if x["Size"] < check]
    del_keys = [x["Key"] for x in to_del]
    if len(del_keys) == 0:
        _log.info("no object found")
    elif dry:
        _log.info("(dry)remove objects: %s", del_keys)
        click.echo(f"(dry)remove {len(del_keys)} objects")
    else:
        _log.info("(wet)remove %s objects", len(del_keys))
        s3.delete_objects(Bucket=bucket_name, Delete={"Objects": [{"Key": x} for x in del_keys]})


@cli.command()
@filetree_option
@verbose_option
def filetree_debug(top: pathlib.Path, config: dict):
    from .processor import DebugProcessor, process_walk
    proc = [DebugProcessor(config)]
    process_walk(top, proc)


@cli.command()
@filetree_option
@verbose_option
def filetree_compress(top: pathlib.Path, config: dict):
    from .processor import CompressProcessor, process_walk
    proc = [CompressProcessor(config)]
    process_walk(top, proc)


@cli.command()
@filetree_option
@verbose_option
def filetree_delete(top: pathlib.Path, config: dict):
    from .processor import DelProcessor, process_walk
    proc = [DelProcessor(config)]
    process_walk(top, proc)


@cli.command()
@s3_option
@click.option("--prefix", default='', help="AWS S3 Object Prefix")
@filetree_option
@verbose_option
def s3_put_tree(s3: boto3.client, bucket_name: str, prefix: str, top: pathlib.Path, config: dict):
    config["s3"] = s3
    config["s3_bucket"] = bucket_name
    config["s3_prefix"] = prefix
    config["skip_names"] = {x["Key"] for x in allobjs(s3, bucket_name, prefix)}
    from .processor import S3Processor, process_walk
    proc = [S3Processor(config)]
    process_walk(top, proc)


def _s3_read(s3: boto3.client, bucket_name: str, key: str) -> bytes:
    from .compr import auto_compress, do_chain
    _, data = auto_compress(pathlib.Path(key), "decompress")
    res = s3.get_object(Bucket=bucket_name, Key=key)
    data[0] = res["Body"].read
    return do_chain(data)


@cli.command()
@s3_option
@click.argument("key")
@verbose_option
def s3_cat(s3: boto3.client, bucket_name: str, key):
    sys.stdout.buffer.write(_s3_read(s3, bucket_name, key))


@cli.command()
@s3_option
@click.argument("key")
@verbose_option
def s3_less(s3: boto3.client, bucket_name: str, key: str):
    click.echo_via_pager(_s3_read(s3, bucket_name, key).decode("utf-8"))


@cli.command()
@s3_option
@click.argument("key")
@click.option("--dry/--wet", help="dry run or wet run", default=False, show_default=True)
@verbose_option
def s3_vi(s3: boto3.client, bucket_name: str, key: str, dry):
    bindata = _s3_read(s3, bucket_name, key).decode("utf-8")
    from .compr import extcmp_map
    _, ext = os.path.splitext(key)
    if ext in extcmp_map:
        compress_fn = extcmp_map[ext][2]
    else:
        def compress_fn(f): return f
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


@cli.command("cat")
@click.argument("filename", type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True))
@verbose_option
def cat_file(filename: str):
    from .compr import auto_compress, do_chain
    _, data = auto_compress(pathlib.Path(filename), "decompress")
    sys.stdout.buffer.write(do_chain(data))


@cli.command("less")
@click.argument("filename", type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True))
@verbose_option
def view_file(filename: str):
    from .compr import auto_compress, do_chain
    _, data = auto_compress(pathlib.Path(filename), "decompress")
    bindata = do_chain(data)
    click.echo_via_pager(bindata.decode("utf-8"))


@cli.command("vi")
@click.argument("filename", type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True))
@click.option("--dry/--wet", help="dry run or wet run", default=False, show_default=True)
@verbose_option
def edit_file(filename: str, dry):
    from .compr import auto_compress, do_chain, extcmp_map
    fname = pathlib.Path(filename)
    _, data = auto_compress(fname, "decompress")
    _, ext = os.path.splitext(fname)
    if ext in extcmp_map:
        compress_fn = extcmp_map[ext][2]
    else:
        def compress_fn(f): return f
    bindata = do_chain(data).decode('utf-8')
    newdata = click.edit(text=bindata)
    if newdata is not None and newdata != bindata:
        if dry:
            _log.info("(dry) changed. write back to %s", fname)
        else:
            _log.info("(wet) changed. write back to %s", fname)
            # mode, timestamp will be changed
            fname.write_bytes(compress_fn(newdata))
    else:
        _log.info("not changed")


@cli.command()
@click.argument("file")
@click.option("--compress", default=None, type=click.Choice(compress_modes),
              help="compress type", multiple=True)
def compress_benchmark(compress, file):
    import csv
    import timeit
    from .compr import modecmp_map
    if not compress:
        compress = compress_modes
    input_data = pathlib.Path(file).read_bytes()

    def bench_comp():
        comp_fn(input_data)

    def bench_decomp():
        decomp_fn(compressed_data)

    wr = csv.writer(sys.stdout)
    wr.writerow(["mode", "rate", "compress", "decompress"])
    for c in compress:
        mode = c
        m = modecmp_map.get(mode)
        if not m:
            click.Abort(f"no such compress mode: {mode}")
            continue
        decomp_fn = m[1]
        comp_fn = m[2]
        compressed_data = comp_fn(input_data)
        isz = len(input_data)
        csz = len(compressed_data)
        rate = csz/isz
        cnum, csec = timeit.Timer(stmt='bench()', globals={
            "input_data": input_data, "comp_fn": comp_fn, "bench": bench_comp
        }).autorange()
        dnum, dsec = timeit.Timer(stmt='bench()', globals={
            "input_data": compressed_data, "decomp_fn": decomp_fn, "bench": bench_decomp
        }).autorange()
        wr.writerow([str(x) for x in [mode, rate, isz*cnum/csec, csz*dnum/dsec]])


@cli.command()
@click.argument("args", nargs=-1)
def sh(args):
    subprocess.run(["sh", *args])


@cli.command()
@click.argument("args", nargs=-1)
def bash(args):
    subprocess.run(["bash", *args])


if __name__ == "__main__":
    cli()
