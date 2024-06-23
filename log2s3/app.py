import datetime
import html
import io
from typing import Any
from pathlib import Path
from fastapi import APIRouter, HTTPException, Response, Header, Query
from fastapi.responses import StreamingResponse
from .common_stream import Stream, MergeStream, CatStream
from .compr_stream import auto_compress_stream, stream_ext
from logging import getLogger

router = APIRouter()
_log = getLogger(__name__)
api_config: dict[str, Any] = {
    "weekday_colors": {
        5: "lightyellow",  # sat
        6: "lightcyan",    # sun
    },
    "today_color": "yellow",
}
exts = set(stream_ext.keys())


def update_config(conf: dict):
    global api_config
    api_config.update(conf)


def uri2file(file_path: str) -> Path:
    global api_config
    working_dir = Path(api_config.get("working_dir", "."))
    target = (working_dir / file_path).resolve()
    if working_dir.resolve().absolute() not in target.resolve().absolute().parents:
        if not (target.exists() and target.samefile(working_dir)):
            _log.warning("out of path: wdir=%s, target=%s", working_dir, target.resolve())
            raise HTTPException(status_code=403, detail=f"cannot access to {file_path}")
    return target


def file2uri(path: Path) -> str:
    global api_config
    working_dir = Path(api_config.get("working_dir", "."))
    return str(path.relative_to(working_dir.resolve()))


def uriescape(uri: str, quote: bool = True) -> str:
    global api_config
    return html.escape(str(Path(api_config.get("prefix", "/")) / uri), quote)


@router.get("/config")
def read_config() -> dict:
    global api_config
    return api_config


@router.get("/read/{file_path:path}")
def read_file(response: Response, file_path: str, accept_encoding: str = Header("")):
    global api_config
    target = uri2file(file_path)
    accepts = [x.strip() for x in accept_encoding.split(",")]
    media_type = api_config.get("content-type", "text/plain")
    # gzip or brotli passthrough case
    special = {
        "br": (".br",),
        "gzip": (".gz",),
    }
    for acc, exts in special.items():
        if acc in accepts:
            for ext in exts:
                if target.with_suffix(target.suffix + ext).is_file():
                    response.headers["content-encoding"] = acc
                    _log.info("compressed %s: %s", acc, target.with_suffix(target.suffix + ext))
                    return StreamingResponse(
                        content=target.with_suffix(target.suffix + ext).open("rb"),
                        media_type=media_type,)
    # uncompressed case
    if target.is_file():
        _log.info("raw %s: %s", acc, target)
        return StreamingResponse(content=target.open("rb"), media_type=media_type)
    # other type case (directory, etc...)
    if target.exists():
        raise HTTPException(status_code=403, detail=f"cannot access to {file_path}")
    # compressed case
    target_compressed = [x for x in target.parent.iterdir() if x.is_file() and x.name.startswith(target.name+".")]
    for p in target_compressed:
        _, stream = auto_compress_stream(p, "decompress")
        _log.info("auto decompress %s: %s", acc, p)
        return StreamingResponse(content=stream.gen(), media_type=media_type)
    raise HTTPException(status_code=404, detail=f"not found: {file_path}")


def reg_file(res: dict, p: Path):
    if p.suffix in exts:
        val = p.with_suffix("")
    else:
        val = p
    name = p.name
    try:
        dt = datetime.datetime.strptime(name.split(".")[0], "%Y-%m-%d")
    except ValueError:
        return
    k2 = dt.strftime("%Y-%m-%d")
    k1 = file2uri(p.parent)
    v1 = file2uri(val)
    try:
        # check k1 and v1 are in working_dir tree
        uri2file(k1)
        uri2file(v1)
    except HTTPException:
        return
    if k1 not in res:
        res[k1] = {}
    if k2 not in res[k1]:
        res[k1][k2] = v1


def list_dir(file_path: str, file_prefix: str = "") -> dict[str, dict[str, str]]:
    res = {}

    target = uri2file(file_path)
    if target.is_dir():
        targets = [target]
    else:
        targets = [x for x in target.parent.iterdir() if x.name.startswith(target.name)]

    for target in targets:
        if target.is_file():
            reg_file(res, target)
        elif target.is_dir():
            for root, _, filenames in target.walk():
                root = Path(root)
                files = [root / x for x in filenames if Path(x).suffix in (exts | {".log", ".txt"})]
                files = [x for x in files if x.name.startswith(file_prefix)]
                for x in files:
                    reg_file(res, x)
    _log.debug("list_dir: keys=%s", res.keys())
    return res


@router.get("/list/{file_path:path}")
def list_raw(file_path: str,
             month=Query(pattern='^[0-9]{4}', default="")):
    return list_dir(file_path, month)


@router.get("/html1/{file_path:path}")
def html1(file_path: str, month=Query(pattern='^[0-9]{4}', default="")):
    def gen(ldir: dict[str, dict[str, str]]):
        yield f"<html><title>{file_path}</title><body>"
        for title, files in ldir.items():
            buf = io.StringIO()
            uri = uriescape(f"html1/{title}")
            buf.write('<div style="border: 1px solid black; float: left; margin: 10px; padding: 1em;">')
            buf.write(f'<h2><a href="{uri}">{title}</a></h2><ul>')
            premonth = None
            for dtstr in sorted(files.keys()):
                dt = datetime.datetime.strptime(dtstr, "%Y-%m-%d")
                month = dt.strftime("%Y-%m")
                if premonth != month:
                    if premonth is not None:
                        buf.write("</li>")
                    buf.write(f"<li>{month}: ")
                    premonth = month
                link = files[dtstr]
                uri = uriescape(f"read/{link}")
                linkhtml = f'<a href="{uri}">{dt.strftime("%d")}</a>'
                color = api_config.get("weekday_colors", {}).get(dt.weekday())
                if color is not None:
                    buf.write(f' <span style="background-color: {color};">{linkhtml}</span>')
                else:
                    buf.write(f' {linkhtml}')
            buf.write("</li></ul>")
            buf.write('</div>')
            yield buf.getvalue()
        yield "</body></html>"
    ldir = list_dir(file_path, month)
    if len(ldir) == 0:
        raise HTTPException(status_code=404, detail=f"not found: {file_path}")
    return StreamingResponse(content=gen(ldir), media_type="text/html")


def html2_gen1(uri: str, month: str, files: dict[str, str]) -> str:
    dt = datetime.datetime.strptime(month, "%Y-%m").date()
    buf = io.StringIO()
    buf.write(f'<tr><th colspan="7"><a href="{uri}?month={month}">{month}</a></th></tr>')
    wday = (dt.weekday()+1) % 7
    buf.write('<tr align="right">')
    if wday != 0:
        buf.write(f'<td colspan="{wday}"></td>')
    for i in range(32):
        cdt = dt + datetime.timedelta(days=i)
        wday = (cdt.weekday()+1) % 7
        if cdt.month != dt.month:
            if wday != 0:
                buf.write(f'<td colspan="{7-wday}"></td>')
            buf.write('</tr>')
            break
        if wday == 0:
            buf.write('</tr><tr align="right">')
        dtstr = cdt.strftime("%Y-%m-%d")
        if cdt == datetime.date.today():
            color = api_config.get("today_color")
        else:
            color = api_config.get("weekday_colors", {}).get(cdt.weekday())
        if color is None:
            buf.write('<td>')
        else:
            buf.write(f'<td style="background-color: {color};">')
        if dtstr in files:
            link = files[dtstr]
            uri = uriescape(f"read/{link}")
            buf.write(f'<a href="{uri}">{cdt.day}</a>')
        else:
            buf.write(f"{cdt.day}")
        buf.write('</td>')
    buf.write('</tr>')
    return buf.getvalue()


def html2_gen(ldir: dict[str, dict[str, str]], file_path: str):
    buf = io.StringIO()
    buf.write(f"<html><title>{file_path}</title><body>")
    for title, files in ldir.items():
        uri = uriescape(f"html2/{title}")
        buf.write('<div style="float: left; margin: 1em;">')
        buf.write(f'<h2><a href="{uri}">{title}</a></h2>')
        buf.write('<table border="1" style="border-collapse: collapse"><tr>')
        b = datetime.date(2000, 1, 2)
        for i in range(7):
            wd = (b+datetime.timedelta(days=i))
            wdstr = wd.strftime("%a")
            color = api_config.get("weekday_colors", {}).get(wd.weekday())
            if color:
                buf.write(f'<th style="background-color: {color};"><code>{wdstr}</code></th>')
            else:
                buf.write(f'<th><code>{wdstr}</code></th>')
        buf.write('</tr>')
        months = {x.rsplit("-", 1)[0] for x in files.keys()}
        for month in sorted(months):
            buf.write(html2_gen1(uri, month, files))
        buf.write("</table></div>")
        yield buf.getvalue()
        buf.truncate(0)
        buf.seek(0)
    yield "</body></html>"


@router.get("/html2/{file_path:path}")
def html2(file_path: str, month=Query(pattern='^[0-9]{4}', default="")):
    ldir = list_dir(file_path, month)
    if len(ldir) == 0:
        raise HTTPException(status_code=404, detail=f"not found: {file_path}")
    return StreamingResponse(content=html2_gen(ldir, file_path), media_type="text/html")


def find_target(p: Path, accepts: list[str]) -> Path:
    # gzip pass through
    if "gzip" in accepts:
        if p.with_suffix(p.suffix + ".gz").is_file():
            return p.with_suffix(p.suffix + ".gz")
    # raw pass through
    if p.is_file():
        return p
    # others
    if "br" in accepts:
        if p.with_suffix(p.suffix + ".br").exists():
            return p.with_suffix(p.suffix + ".br")
    # compressed case
    target_compressed = [x for x in p.parent.iterdir() if x.is_file() and x.name.startswith(p.name+".")]
    if len(target_compressed):
        return target_compressed[0]
    raise HTTPException(status_code=404, detail=f"not found: {p}")


def get_streams(files: dict[str, dict[str, str]], accepts: list[str]) -> tuple[list[Stream], dict]:
    outputs: dict[str, list[str]] = {}
    for _, v in files.items():
        for k, fn in v.items():
            if k not in outputs:
                outputs[k] = []
            outputs[k].append(fn)
    output_list: list[Path] = []
    for k in sorted(outputs.keys()):
        for fname in sorted(outputs[k]):
            target = uri2file(fname)
            target_file = find_target(target, accepts)
            output_list.append(target_file)
    mode = "decompress"
    hdrs = {}
    if "gzip" in accepts:
        mode = "gzip"
        hdrs["content-encoding"] = "gzip"
    elif "br" in accepts and ".br" in stream_ext:
        mode = "brotli"
        hdrs["content-encoding"] = "br"
    _log.debug("streams: %s files, mode=%s, hdrs=%s",
               len(output_list), mode, hdrs)
    return [y[1] for y in [auto_compress_stream(x, mode) for x in output_list]], hdrs


@router.get("/cat/{file_path:path}")
def cat_file(file_path: str,
             month=Query(pattern='^[0-9]{4}', default="")):
    media_type = api_config.get("content-type", "text/plain")
    ldir = list_dir(file_path, month)
    if len(ldir) == 0:
        raise HTTPException(status_code=404, detail=f"not found: {file_path}")
    streams, hdrs = get_streams(ldir, [])
    # daily sort
    return StreamingResponse(
        content=CatStream(streams).gen(), media_type=media_type, headers=hdrs)


@router.get("/merge/{file_path:path}")
def merge_file(file_path: str,
               month=Query(pattern='^[0-9]{4}', default="")):
    media_type = api_config.get("content-type", "text/plain")
    ldir = list_dir(file_path, month)
    if len(ldir) == 0:
        raise HTTPException(status_code=404, detail=f"not found: {file_path}")
    streams, hdrs = get_streams(ldir, [])  # cannot do passthrough compression
    # daily sort
    return StreamingResponse(
        content=MergeStream(streams).gen(), media_type=media_type, headers=hdrs)
