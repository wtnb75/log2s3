import datetime
import html
import io
import json
from typing import Generator
from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse, HTMLResponse
from .app import api_config, uri2file, list_dir
from .compr_stream import auto_compress_stream, stream_ext
from logging import getLogger

router = APIRouter()
_log = getLogger(__name__)

_htmx_base = "/htmx"
_exts = set(stream_ext.keys())
_PAGE_SIZE = 200
_FOLD_THRESHOLD = 200


def update_config(conf: dict):
    global _htmx_base
    if "prefix" in conf:
        _htmx_base = conf["prefix"].rstrip("/")
    from .app import update_config as _app_update

    shared = {k: v for k, v in conf.items() if k != "prefix"}
    if shared:
        _app_update(shared)


def _u(*parts: str) -> str:
    """Build an absolute URL under the htmx base."""
    return _htmx_base + "/" + "/".join(str(p).strip("/") for p in parts if p)


# ---------------------------------------------------------------------------
# Static assets
# ---------------------------------------------------------------------------

_CSS = """
body { font-family: monospace; margin: 0; padding: 0; }
#tabs {
    display: flex; gap: 4px; padding: 8px;
    background: #f0f0f0; border-bottom: 1px solid #ccc; flex-wrap: wrap;
}
.tab-btn {
    padding: 4px 12px; cursor: pointer;
    border: 1px solid #ccc; background: white;
    text-decoration: none; color: #333;
}
.tab-btn.active { background: #333; color: white; border-color: #333; }
#main-area { padding: 8px; }
.month-nav { margin-bottom: 8px; }
.month-nav a { margin: 0 6px; text-decoration: none; color: #0066cc; }
table { border-collapse: collapse; margin-bottom: 8px; }
td, th {
    border: 1px solid #ccc; padding: 2px 8px;
    text-align: right; min-width: 2em;
}
th { background: #eee; }
td a { text-decoration: none; color: #0066cc; }
#search {
    width: 100%; box-sizing: border-box;
    margin-bottom: 8px; padding: 4px;
    font-family: monospace; font-size: 13px;
}
#log-content { font-family: monospace; font-size: 12px; }
.log-line { border-bottom: 1px solid #eee; padding: 2px 4px; }
.log-line:hover .line-link { visibility: visible; }
.line-link {
    visibility: hidden; text-decoration: none;
    color: #aaa; padding-right: 4px; font-size: 10px;
}
details.json-block > summary {
    color: #666; cursor: pointer; font-style: italic;
}
details.json-block > pre {
    margin: 0; padding: 4px 16px;
    background: #f8f8f8; overflow-x: auto;
}
details.log-line > summary { cursor: pointer; }
.line-highlight { background: #ffffaa; }
.sentinel { height: 1px; }
"""

_JS = r"""
(function () {
    // Scroll to #line-N from URL hash after htmx settles
    var targetId = location.hash ? location.hash.slice(1) : null;
    if (!targetId || !targetId.startsWith("line-")) return;

    function tryScroll() {
        var el = document.getElementById(targetId);
        if (!el) return;
        el.scrollIntoView({ behavior: "smooth", block: "center" });
        el.classList.add("line-highlight");
        document.removeEventListener("htmx:afterSettle", tryScroll);
    }

    document.addEventListener("DOMContentLoaded", tryScroll);
    document.addEventListener("htmx:afterSettle", tryScroll);
})();
"""


# ---------------------------------------------------------------------------
# Log line rendering
# ---------------------------------------------------------------------------


def _detect_json(line: str) -> tuple[str, str]:
    """Return (text_before_json, json_html). json_html is '' if no JSON found.

    Tries every { and [ position left-to-right so that earlier non-JSON
    brackets (e.g. [ERROR]) don't prevent detection of a later JSON object.
    """
    positions = sorted({pos for ch in ("{", "[") for pos in _findall(line, ch)})
    for json_start in positions:
        try:
            obj = json.loads(line[json_start:])
        except (json.JSONDecodeError, ValueError):
            continue
        pretty = json.dumps(obj, indent=2, ensure_ascii=False)
        if isinstance(obj, dict) and obj:
            keys = list(obj.keys())[:3]
            trailer = " ..." if len(obj) > 3 else ""
            summary = "{ " + ", ".join(keys) + trailer + " }"
        else:
            raw = line[json_start:]
            summary = (raw[:40] + "...") if len(raw) > 40 else raw
        json_html = (
            '<details class="json-block">'
            f"<summary>{html.escape(summary)}</summary>"
            f"<pre>{html.escape(pretty)}</pre>"
            "</details>"
        )
        return line[:json_start], json_html
    return line, ""


def _findall(s: str, ch: str) -> list[int]:
    """Return all positions of ch in s."""
    positions = []
    idx = 0
    while True:
        pos = s.find(ch, idx)
        if pos < 0:
            break
        positions.append(pos)
        idx = pos + 1
    return positions


def _render_line(line: str, line_id: str) -> str:
    line = line.rstrip("\n")
    link = f'<a class="line-link" href="#{line_id}">#</a>'
    text_part, json_html = _detect_json(line)
    escaped = html.escape(text_part)

    if len(line) > _FOLD_THRESHOLD:
        trunc = html.escape(line[:_FOLD_THRESHOLD]) + "..."
        return (
            f'<details class="log-line" id="{line_id}"><summary>{link}{trunc}</summary>{escaped}{json_html}</details>\n'
        )
    return f'<div class="log-line" id="{line_id}">{link}{escaped}{json_html}</div>\n'


# ---------------------------------------------------------------------------
# File reading
# ---------------------------------------------------------------------------


def _find_file(file_path: str):
    from fastapi import HTTPException

    target = uri2file(file_path)
    if target.is_file():
        return target
    candidates = sorted(p for p in target.parent.iterdir() if p.is_file() and p.name.startswith(target.name + "."))
    if candidates:
        return candidates[0]
    raise HTTPException(status_code=404, detail=f"not found: {file_path}")


def _iter_lines(file_path: str) -> Generator[str, None, None]:
    actual = _find_file(file_path)
    _, stream = auto_compress_stream(actual, "decompress")
    yield from stream.text_gen()


# ---------------------------------------------------------------------------
# Calendar rendering
# ---------------------------------------------------------------------------


def _month_link(cal: str, base: str, m: str, label: str) -> str:
    return f'<a hx-get="{cal}?month={m}" hx-target="#calendar-area" hx-push-url="{base}?month={m}" href="#">{label}</a>'


def _available_months(dir_path: str) -> list[str]:
    """Return sorted list of YYYY-MM strings that have files."""
    all_ldir = list_dir(dir_path, "")
    months: set[str] = set()
    for files in all_ldir.values():
        for date_str in files:
            months.add(date_str[:7])
    return sorted(months)


def _find_latest_file(dir_path: str, month: str) -> tuple[str, str, str] | tuple[str, None, None]:
    """Return (effective_month, latest_date, filepath) for the latest log in dir_path.

    Tries the given month first. If no files found, falls back to the latest
    available month across all time. Returns (month, None, None) if no files exist.
    """
    for m in (month, None):
        ldir = list_dir(dir_path, m or "")
        all_files: dict[str, str] = {}
        for files in ldir.values():
            all_files.update(files)
        if all_files:
            latest_date = max(all_files.keys())
            return latest_date[:7], latest_date, all_files[latest_date]
    return month, None, None


def _calendar_inner(dir_path: str, month: str) -> str:
    """Inner HTML for #calendar-area."""
    ldir = list_dir(dir_path, month)

    dt = datetime.date.fromisoformat(month + "-01")
    prev_m = (dt - datetime.timedelta(days=1)).strftime("%Y-%m")
    next_m = (dt.replace(day=28) + datetime.timedelta(days=4)).replace(day=1).strftime("%Y-%m")

    base = _u(dir_path)
    cal = _u("_calendar", dir_path)
    weekday_colors = api_config.get("weekday_colors", {})
    today = datetime.date.today()
    today_color = api_config.get("today_color", "yellow")

    buf = io.StringIO()
    buf.write(
        '<div class="month-nav">'
        + _month_link(cal, base, prev_m, f"&lt; {prev_m}")
        + f" | <b>{month}</b> | "
        + _month_link(cal, base, next_m, f"{next_m} &gt;")
        + "</div>"
    )

    if not ldir:
        available = _available_months(dir_path)
        if not available:
            buf.write("<p>(no files)</p>")
        else:
            before = [m for m in available if m < month]
            after = [m for m in available if m > month]
            parts = []
            if before:
                parts.append("前: " + _month_link(cal, base, before[-1], before[-1]))
            if after:
                parts.append("次: " + _month_link(cal, base, after[0], after[0]))
            buf.write("<p>この月のファイルなし &nbsp; " + " &nbsp; ".join(parts) + "</p>")
        return buf.getvalue()

    for _, files in ldir.items():
        buf.write("<table><tr>")
        sun = datetime.date(2000, 1, 2)  # a Sunday
        for i in range(7):
            wd = sun + datetime.timedelta(days=i)
            color = weekday_colors.get(wd.weekday())
            style = f' style="background:{color}"' if color else ""
            buf.write(f"<th{style}>{wd.strftime('%a')}</th>")
        buf.write("</tr>")

        months = sorted({d[:7] for d in files})
        for m in months:
            m_dt = datetime.date.fromisoformat(m + "-01")
            buf.write(f'<tr><th colspan="7">{m}</th></tr><tr>')
            wday = (m_dt.weekday() + 1) % 7  # Sun=0
            if wday:
                buf.write(f'<td colspan="{wday}"></td>')
            d = m_dt
            while d.month == m_dt.month:
                wday = (d.weekday() + 1) % 7
                if wday == 0 and d != m_dt:
                    buf.write("</tr><tr>")
                dtstr = d.strftime("%Y-%m-%d")
                if d == today:
                    color = today_color
                else:
                    color = weekday_colors.get(d.weekday())
                style = f' style="background:{color}"' if color else ""
                if dtstr in files:
                    fp = files[dtstr]
                    cnt_url = _u("_content", fp)
                    push_url = f"{base}?month={month}&date={dtstr}"
                    buf.write(
                        f"<td{style}>"
                        f'<a hx-get="{cnt_url}" hx-target="#log-content"'
                        f' hx-push-url="{push_url}" href="#">{d.day}</a>'
                        f"</td>"
                    )
                else:
                    buf.write(f"<td{style}>{d.day}</td>")
                d += datetime.timedelta(days=1)
            wday = (d.weekday() + 1) % 7
            if wday:
                buf.write(f'<td colspan="{7 - wday}"></td>')
            buf.write("</tr>")
        buf.write("</table>")
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Main area rendering
# ---------------------------------------------------------------------------


def _main_inner(dir_path: str, month: str, log_content: str = "") -> str:
    """Inner HTML for #main-area."""
    cal_html = _calendar_inner(dir_path, month)
    search_url = _u("_search", dir_path)
    return (
        f'<div id="calendar-area">{cal_html}</div>'
        f'<input id="search" type="text" placeholder="Search..."'
        f' hx-get="{search_url}" hx-include="[name=\'month\']"'
        f' hx-target="#log-content" hx-trigger="keyup changed delay:500ms" name="q">'
        f'<input type="hidden" name="month" value="{html.escape(month)}">'
        f'<div id="log-content">{log_content}</div>'
    )


# ---------------------------------------------------------------------------
# Log content streaming
# ---------------------------------------------------------------------------


def _log_content_gen(file_path: str, offset: int, limit: int) -> Generator[str, None, None]:
    count = 0
    has_more = False
    for n, line in enumerate(_iter_lines(file_path)):
        if n < offset:
            continue
        if count >= limit:
            has_more = True
            break
        yield _render_line(line, f"line-{n}")
        count += 1
    if has_more:
        next_offset = offset + count
        cnt_url = _u("_content", file_path)
        yield (
            f'<div class="sentinel"'
            f' hx-get="{cnt_url}?offset={next_offset}&limit={limit}"'
            f' hx-trigger="revealed" hx-target="#log-content" hx-swap="beforeend"></div>\n'
        )


def _log_content_str(file_path: str, offset: int, limit: int) -> str:
    return "".join(_log_content_gen(file_path, offset, limit))


# ---------------------------------------------------------------------------
# Full page
# ---------------------------------------------------------------------------


def _full_page_gen(dirs: list[str], selected: str, month: str, date: str) -> Generator[str, None, None]:
    yield (
        "<!DOCTYPE html><html><head>"
        '<meta charset="utf-8">'
        "<title>log viewer</title>"
        f"<style>{_CSS}</style>"
        '<script src="https://unpkg.com/htmx.org@2" defer></script>'
        f"<script>{_JS}</script>"
        "</head><body>"
    )

    # Tab bar (inline, no extra request)
    yield '<div id="tabs">'
    for d in dirs:
        cls = "tab-btn active" if d == selected else "tab-btn"
        main_url = _u("_main", d)
        push_url = _u(d) + f"?month={month}"
        yield (
            f'<a class="{cls}" href="{push_url}"'
            f' hx-get="{main_url}?month={month}"'
            f' hx-target="#main-area" hx-push-url="{push_url}">'
            f"{html.escape(d)}</a>"
        )
    yield "</div>"

    yield '<div id="main-area">'
    if selected:
        if date:
            ldir = list_dir(selected, month)
            fp = (ldir.get(selected) or {}).get(date)
            initial_log = _log_content_str(fp, 0, _PAGE_SIZE) if fp else ""
            yield _main_inner(selected, month, initial_log)
        else:
            effective_month, _, latest_fp = _find_latest_file(selected, month)
            initial_log = _log_content_str(latest_fp, 0, _PAGE_SIZE) if latest_fp else ""
            yield _main_inner(selected, effective_month, initial_log)
    yield "</div></body></html>"


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/")
def index():
    dirs = sorted(list_dir(".", "").keys())
    return StreamingResponse(
        _full_page_gen(dirs, "", datetime.date.today().strftime("%Y-%m"), ""),
        media_type="text/html",
    )


@router.get("/_main/{dir_path:path}")
def main_area(dir_path: str, month: str = Query(default="")):
    if not month:
        month = datetime.date.today().strftime("%Y-%m")
    effective_month, latest_date, latest_fp = _find_latest_file(dir_path, month)
    initial_log = _log_content_str(latest_fp, 0, _PAGE_SIZE) if latest_fp else ""
    content = _main_inner(dir_path, effective_month, initial_log)
    push_url = _u(dir_path) + f"?month={effective_month}"
    if latest_date:
        push_url += f"&date={latest_date}"
    return HTMLResponse(content, headers={"HX-Push-Url": push_url})


@router.get("/_calendar/{dir_path:path}")
def calendar_area(dir_path: str, month: str = Query(default="")):
    if not month:
        month = datetime.date.today().strftime("%Y-%m")
    return HTMLResponse(_calendar_inner(dir_path, month))


@router.get("/_content/{file_path:path}")
def content(
    file_path: str,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=_PAGE_SIZE, ge=1, le=1000),
):
    return StreamingResponse(
        _log_content_gen(file_path, offset, limit),
        media_type="text/html",
    )


@router.get("/_search/{dir_path:path}")
def search(
    dir_path: str,
    q: str = Query(default=""),
    month: str = Query(default=""),
):
    def _gen():
        if not q:
            return
        ldir = list_dir(dir_path, month)
        files = ldir.get(dir_path, {})
        for date_str in sorted(files):
            fp = files[date_str]
            for n, line in enumerate(_iter_lines(fp)):
                if q.lower() in line.lower():
                    yield _render_line(line, f"line-{date_str}-{n}")

    return StreamingResponse(_gen(), media_type="text/html")


@router.get("/{dir_path:path}")
def index_dir(
    dir_path: str,
    month: str = Query(default=""),
    date: str = Query(default=""),
):
    if not month:
        month = datetime.date.today().strftime("%Y-%m")
    dirs = sorted(list_dir(".", "").keys())
    return StreamingResponse(
        _full_page_gen(dirs, dir_path, month, date),
        media_type="text/html",
    )
