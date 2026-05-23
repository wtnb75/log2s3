import datetime
import html
import io
import json
from pathlib import Path
from typing import Generator
from urllib.parse import urlencode
from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse, HTMLResponse
from .app import uri2file, list_dir, api_config
from .compr_stream import auto_compress_stream, stream_ext
from logging import getLogger

router = APIRouter()
_log = getLogger(__name__)
month_query = Query(pattern="^([0-9]{4}-[0-9]{2}|)$", default="")

_htmx_base = "/htmx"
_exts = set(stream_ext.keys())
_PAGE_SIZE = 200
_WD_CLASS = {5: "wd-5", 6: "wd-6"}  # weekday → CSS class (sat, sun)


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
:root {
    --bg:#fff; --fg:#333; --border:#ccc;
    --tabs-bg:#f0f0f0; --tab-bg:#fff; --tab-fg:#333;
    --tab-active-bg:#333; --tab-active-fg:#fff;
    --link:#0066cc; --th-bg:#eee;
    --log-border:#eee; --line-link:#aaa;
    --json-color:#666; --json-pre-bg:#f8f8f8;
    --btn-bg:#f0f0f0; --btn-hover:#ddd;
    --highlight:#ffffaa; --mark-bg:#ff0;
    --cal-sat:lightyellow; --cal-sun:lightcyan; --cal-today:yellow;
}
[data-theme="dark"] {
    --bg:#1a1a1a; --fg:#d4d4d4; --border:#444;
    --tabs-bg:#252525; --tab-bg:#2d2d2d; --tab-fg:#ccc;
    --tab-active-bg:#ccc; --tab-active-fg:#1a1a1a;
    --link:#5baeff; --th-bg:#2d2d2d;
    --log-border:#2a2a2a; --line-link:#555;
    --json-color:#888; --json-pre-bg:#252525;
    --btn-bg:#2d2d2d; --btn-hover:#3d3d3d;
    --highlight:#554400; --mark-bg:#806600;
    --cal-sat:#3a3a10; --cal-sun:#103a3a; --cal-today:#4a3a00;
}
@media (prefers-color-scheme: dark) {
    :root:not([data-theme="light"]) {
        --bg:#1a1a1a; --fg:#d4d4d4; --border:#444;
        --tabs-bg:#252525; --tab-bg:#2d2d2d; --tab-fg:#ccc;
        --tab-active-bg:#ccc; --tab-active-fg:#1a1a1a;
        --link:#5baeff; --th-bg:#2d2d2d;
        --log-border:#2a2a2a; --line-link:#555;
        --json-color:#888; --json-pre-bg:#252525;
        --btn-bg:#2d2d2d; --btn-hover:#3d3d3d;
        --highlight:#554400; --mark-bg:#806600;
        --cal-sat:#3a3a10; --cal-sun:#103a3a; --cal-today:#4a3a00;
    }
}
body { font-family: monospace; margin: 0; padding: 0; background: var(--bg); color: var(--fg); }
#tabs {
    display: flex; gap: 4px; padding: 8px; align-items: center;
    background: var(--tabs-bg); border-bottom: 1px solid var(--border); flex-wrap: wrap;
}
.tab-btn {
    padding: 4px 12px; cursor: pointer;
    border: 1px solid var(--border); background: var(--tab-bg);
    text-decoration: none; color: var(--tab-fg);
}
.tab-btn.active { background: var(--tab-active-bg); color: var(--tab-active-fg); border-color: var(--tab-active-bg); }
#theme-toggle {
    margin-left: auto; cursor: pointer; padding: 2px 8px; font-size: 14px;
    border: 1px solid var(--border); background: var(--btn-bg); color: var(--fg);
}
#main-area { padding: 8px; }
.month-nav { margin-bottom: 8px; }
.month-nav a { margin: 0 6px; text-decoration: none; color: var(--link); }
table { border-collapse: collapse; margin-bottom: 8px; }
td, th {
    border: 1px solid var(--border); padding: 2px 8px;
    text-align: right; min-width: 2em;
}
th { background: var(--th-bg); }
td a { text-decoration: none; color: var(--link); }
#search {
    width: 100%; box-sizing: border-box;
    margin-bottom: 8px; padding: 4px;
    font-family: monospace; font-size: 13px;
    background: var(--tab-bg); color: var(--fg); border: 1px solid var(--border);
}
#log-content { font-family: monospace; font-size: 12px; }
.log-line { border-bottom: 1px solid var(--log-border); padding: 2px 4px; }
.log-line:hover .line-link { visibility: visible; }
.line-link {
    visibility: hidden; text-decoration: none;
    color: var(--line-link); padding-right: 4px; font-size: 10px;
}
details.json-block > summary {
    color: var(--json-color); cursor: pointer; font-style: italic;
}
details.json-block > pre {
    margin: 0; padding: 4px 16px;
    background: var(--json-pre-bg); overflow-x: auto;
}
.copy-btn {
    float: right; font-size: 10px; padding: 1px 6px;
    cursor: pointer; border: 1px solid var(--border);
    background: var(--btn-bg); color: var(--fg); margin: 2px 4px;
}
.copy-btn:hover { background: var(--btn-hover); }
details.log-line > summary {
    cursor: pointer;
    display: block;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}
details.log-line[open] > summary {
    white-space: normal;
    overflow: visible;
    text-overflow: unset;
}
.line-highlight { background: var(--highlight); }
.sentinel { height: 1px; }
mark { background: var(--mark-bg); padding: 0 1px; border-radius: 2px; }
.wd-5 { background: var(--cal-sat); }
.wd-6 { background: var(--cal-sun); }
.wd-today { background: var(--cal-today); }
"""

# Applied synchronously in <head> to avoid flash of unstyled content
_THEME_INIT = (
    "(function(){var t=localStorage.getItem('theme');if(t)document.documentElement.setAttribute('data-theme',t);})();"
)

_JS = r"""
(function () {
    function currentTheme() {
        return document.documentElement.getAttribute("data-theme") ||
            (matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
    }

    document.addEventListener("DOMContentLoaded", function () {
        // Theme toggle — getElementById requires DOM to be ready
        var themeBtn = document.getElementById("theme-toggle");
        function applyTheme(t) {
            document.documentElement.setAttribute("data-theme", t);
            themeBtn.textContent = t === "dark" ? "☀️" : "🌙";
        }
        applyTheme(currentTheme());
        themeBtn.addEventListener("click", function () {
            var next = currentTheme() === "dark" ? "light" : "dark";
            localStorage.setItem("theme", next);
            applyTheme(next);
        });

        // Scroll to #line-N from URL hash
        var targetId = location.hash ? location.hash.slice(1) : null;
        if (targetId && targetId.startsWith("line-")) {
            function tryScroll() {
                var el = document.getElementById(targetId);
                if (!el) return;
                el.scrollIntoView({ behavior: "smooth", block: "center" });
                el.classList.add("line-highlight");
                document.removeEventListener("htmx:afterSettle", tryScroll);
            }
            tryScroll();
            document.addEventListener("htmx:afterSettle", tryScroll);
        }
    });

    // Update active tab highlight on click
    document.addEventListener("click", function (e) {
        var tab = e.target.closest(".tab-btn");
        if (!tab) return;
        document.querySelectorAll(".tab-btn").forEach(function (t) { t.classList.remove("active"); });
        tab.classList.add("active");
    });

    // Copy JSON to clipboard (event delegation — works before DOMContentLoaded)
    document.addEventListener("click", function (e) {
        if (!e.target.classList.contains("copy-btn")) return;
        var pre = e.target.closest("details").querySelector("pre");
        if (!pre) return;
        navigator.clipboard.writeText(pre.textContent).then(function () {
            var btn = e.target;
            btn.textContent = "Copied!";
            setTimeout(function () { btn.textContent = "Copy"; }, 1500);
        });
    });
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
            '<button class="copy-btn">Copy</button>'
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


def _highlight(text: str, q: str) -> str:
    """Return HTML-escaped text with case-insensitive matches of q wrapped in <mark>."""
    if not q:
        return html.escape(text)
    parts = []
    lower_text = text.lower()
    lower_q = q.lower()
    start = 0
    while True:
        pos = lower_text.find(lower_q, start)
        if pos < 0:
            parts.append(html.escape(text[start:]))
            break
        parts.append(html.escape(text[start:pos]))
        parts.append(f"<mark>{html.escape(text[pos : pos + len(q)])}</mark>")
        start = pos + len(q)
    return "".join(parts)


def _render_line(line: str, line_id: str, q: str = "") -> str:
    line = line.rstrip("\n")
    link = f'<a class="line-link" href="#{line_id}">#</a>'
    _, json_html = _detect_json(line)
    body = _highlight(line, q)
    return f'<details class="log-line" id="{line_id}"><summary>{link}{body}</summary>{json_html}</details>\n'


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
        working_dir = Path(api_config.get("working_dir", ".")).resolve()
        rel_candidate = str(candidates[0].resolve().relative_to(working_dir))
        return uri2file(rel_candidate)
    raise HTTPException(status_code=404, detail=f"not found: {file_path}")


def _iter_lines(file_path: str) -> Generator[str, None, None]:
    actual = _find_file(file_path)
    _, stream = auto_compress_stream(actual, "decompress")
    yield from stream.text_gen()


# ---------------------------------------------------------------------------
# Calendar rendering
# ---------------------------------------------------------------------------


def _month_link(cal: str, base: str, m: str, label: str) -> str:
    ecal = html.escape(cal, quote=True)
    ebase = html.escape(base, quote=True)
    return (
        f'<a hx-get="{ecal}?month={m}" hx-target="#calendar-area" hx-push-url="{ebase}?month={m}" href="#">{label}</a>'
    )


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

    try:
        dt = datetime.date.fromisoformat(month + "-01")
    except ValueError:
        return "<p>(invalid month)</p>"
    prev_m = (dt - datetime.timedelta(days=1)).strftime("%Y-%m")
    next_m = (dt.replace(day=28) + datetime.timedelta(days=4)).replace(day=1).strftime("%Y-%m")

    base = _u(dir_path)
    cal = _u("_calendar", dir_path)
    today = datetime.date.today()

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
            cls = _WD_CLASS.get(wd.weekday(), "")
            attr = f' class="{cls}"' if cls else ""
            buf.write(f"<th{attr}>{wd.strftime('%a')}</th>")
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
                    cls = "wd-today"
                else:
                    cls = _WD_CLASS.get(d.weekday(), "")
                attr = f' class="{cls}"' if cls else ""
                if dtstr in files:
                    fp = files[dtstr]
                    cnt_url = html.escape(_u("_content", fp), quote=True)
                    push_url = f"{html.escape(base, quote=True)}?month={month}&date={dtstr}"
                    buf.write(
                        f"<td{attr}>"
                        f'<a hx-get="{cnt_url}" hx-target="#log-content"'
                        f' hx-push-url="{push_url}" href="#">{d.day}</a>'
                        f"</td>"
                    )
                else:
                    buf.write(f"<td{attr}>{d.day}</td>")
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
    search_url = html.escape(_u("_search", dir_path), quote=True)
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
        query = urlencode({"offset": next_offset, "limit": limit})
        cnt_url = html.escape(f'{_u("_content", file_path)}?{query}', quote=True)
        yield (
            f'<div class="sentinel"'
            f' hx-get="{cnt_url}"'
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
        f"<script>{_THEME_INIT}</script>"
        f"<style>{_CSS}</style>"
        '<script src="https://unpkg.com/htmx.org@2" defer></script>'
        f"<script>{_JS}</script>"
        "</head><body>"
    )

    # Tab bar (inline, no extra request)
    yield '<div id="tabs">'
    for d in dirs:
        cls = "tab-btn active" if d == selected else "tab-btn"
        main_url = html.escape(_u("_main", d), quote=True)
        push_url = html.escape(_u(d), quote=True) + f"?month={month}"
        yield (
            f'<a class="{cls}" href="{push_url}"'
            f' hx-get="{main_url}?month={month}"'
            f' hx-target="#main-area" hx-push-url="{push_url}">'
            f"{html.escape(d)}</a>"
        )
    yield '<button id="theme-toggle"></button>'
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
def main_area(dir_path: str, month: str = month_query):
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
def calendar_area(dir_path: str, month: str = month_query):
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
    month: str = month_query,
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
                    yield _render_line(line, f"line-{date_str}-{n}", q)

    return StreamingResponse(_gen(), media_type="text/html")


@router.get("/{dir_path:path}")
def index_dir(
    dir_path: str,
    month: str = month_query,
    date: str = Query(default=""),
):
    if not month:
        month = datetime.date.today().strftime("%Y-%m")
    dirs = sorted(list_dir(".", "").keys())
    return StreamingResponse(
        _full_page_gen(dirs, dir_path, month, date),
        media_type="text/html",
    )
