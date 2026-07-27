import datetime
import gzip
import tempfile
import unittest
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from log2s3.app_htmx import (
    _detect_json,
    _findall,
    _highlight,
    _render_line,
    router,
    update_config,
)

# ---------------------------------------------------------------------------
# Pure function tests
# ---------------------------------------------------------------------------


class TestFindall(unittest.TestCase):
    def test_empty_string(self):
        self.assertEqual([], _findall("", "{"))

    def test_no_match(self):
        self.assertEqual([], _findall("hello", "{"))

    def test_single_match(self):
        self.assertEqual([5], _findall("hello{world", "{"))

    def test_multiple_matches(self):
        self.assertEqual([0, 3, 6], _findall("{a}{b}{c}", "{"))

    def test_consecutive(self):
        self.assertEqual([0, 1, 2], _findall("{{{", "{"))


class TestDetectJson(unittest.TestCase):
    def test_no_json(self):
        text, html = _detect_json("hello world")
        self.assertEqual("hello world", text)
        self.assertEqual("", html)

    def test_invalid_json(self):
        text, html = _detect_json("INFO {bad json}")
        self.assertEqual("INFO {bad json}", text)
        self.assertEqual("", html)

    def test_json_object(self):
        text, html = _detect_json('INFO {"key": "value"}')
        self.assertEqual("INFO ", text)
        self.assertIn("json-block", html)
        self.assertIn("key", html)

    def test_json_at_start(self):
        text, html = _detect_json('{"key": 1}')
        self.assertEqual("", text)
        self.assertIn("json-block", html)

    def test_json_array(self):
        text, html = _detect_json("INFO [1, 2, 3]")
        self.assertEqual("INFO ", text)
        self.assertIn("json-block", html)

    def test_bracket_before_json(self):
        # [ERROR] is not valid JSON; the { after it is
        text, html = _detect_json('[ERROR] {"key": "val"}')
        self.assertEqual("[ERROR] ", text)
        self.assertIn("json-block", html)
        self.assertIn("key", html)

    def test_json_summary_many_keys(self):
        # >3 keys: summary shows first 3 + " ..."
        _, html = _detect_json('LOG {"a":1,"b":2,"c":3,"d":4}')
        self.assertIn("a, b, c", html)
        self.assertIn("...", html)

    def test_json_summary_three_keys(self):
        # exactly 3 keys: no trailing "..."
        _, html = _detect_json('LOG {"a":1,"b":2,"c":3}')
        self.assertIn("a, b, c", html)
        self.assertNotIn("...", html)

    def test_json_pretty_in_pre(self):
        _, html = _detect_json('{"k":"v"}')
        self.assertIn("<pre>", html)
        # html.escape turns " into &quot; inside <pre>
        self.assertIn("&quot;k&quot;", html)
        self.assertIn("&quot;v&quot;", html)

    def test_json_copy_button(self):
        _, html = _detect_json('{"k": 1}')
        self.assertIn("copy-btn", html)


class TestHighlight(unittest.TestCase):
    def test_empty_query_escapes(self):
        self.assertEqual("a &lt; b", _highlight("a < b", ""))

    def test_no_match(self):
        self.assertEqual("hello world", _highlight("hello world", "xyz"))

    def test_simple_match(self):
        result = _highlight("hello world", "world")
        self.assertIn("<mark>world</mark>", result)
        self.assertTrue(result.startswith("hello "))

    def test_case_insensitive(self):
        result = _highlight("Hello World", "hello")
        self.assertIn("<mark>Hello</mark>", result)

    def test_multiple_matches(self):
        result = _highlight("abc abc abc", "abc")
        self.assertEqual(3, result.count("<mark>"))

    def test_html_escaping_around_match(self):
        result = _highlight("<b>hello</b>", "hello")
        self.assertIn("&lt;b&gt;", result)
        self.assertIn("<mark>hello</mark>", result)
        self.assertNotIn("<b>", result)

    def test_html_escaping_in_query(self):
        # query containing HTML special chars
        result = _highlight("a<b>c", "<b>")
        self.assertIn("<mark>&lt;b&gt;</mark>", result)


class TestRenderLine(unittest.TestCase):
    def test_basic_structure(self):
        result = _render_line("hello world", "line-0")
        self.assertIn('<details class="log-line"', result)
        self.assertIn('id="line-0"', result)
        self.assertIn("hello world", result)

    def test_line_link(self):
        result = _render_line("test", "line-5")
        self.assertIn('href="#line-5"', result)
        self.assertIn("line-link", result)

    def test_strips_trailing_newline(self):
        result = _render_line("hello\n", "line-0")
        # newline in log data should be stripped (only trailing \n from template remains)
        self.assertNotIn("hello\n", result)

    def test_always_uses_details(self):
        # short line
        self.assertIn("<details", _render_line("hi", "line-0"))
        # long line
        self.assertIn("<details", _render_line("x" * 300, "line-0"))

    def test_html_escaping(self):
        result = _render_line("<script>alert(1)</script>", "line-0")
        self.assertIn("&lt;script&gt;", result)
        self.assertNotIn("<script>alert", result)

    def test_with_query_highlight(self):
        result = _render_line("hello world", "line-0", q="world")
        self.assertIn("<mark>world</mark>", result)

    def test_with_json(self):
        result = _render_line('INFO {"key": "val"}', "line-0")
        self.assertIn("json-block", result)
        self.assertIn("copy-btn", result)

    def test_no_json(self):
        result = _render_line("plain log line", "line-0")
        self.assertNotIn("json-block", result)
        self.assertNotIn("copy-btn", result)


# ---------------------------------------------------------------------------
# Endpoint integration tests
# ---------------------------------------------------------------------------

_RAW_CONTENT = '2024-01-01 INFO hello world\n2024-01-01 INFO {"key": "value", "n": 1}\n'
_GZ_CONTENT = gzip.compress(_RAW_CONTENT.encode())


class TestHtmxEndpoints(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.include_router(router, prefix="/htmx")
        self.client = TestClient(self.app)
        self.td = tempfile.TemporaryDirectory()
        update_config({"working_dir": self.td.name, "prefix": "/htmx"})
        for name in ["applog", "syslog"]:
            dirp = Path(self.td.name) / name
            dirp.mkdir()
            for i in range(5):
                dt = datetime.date(2024, 1, 1) + datetime.timedelta(days=i)
                (dirp / dt.strftime("%Y-%m-%d.log")).write_text(_RAW_CONTENT)
            for i in range(5):
                dt = datetime.date(2024, 2, 1) + datetime.timedelta(days=i)
                (dirp / (dt.strftime("%Y-%m-%d") + ".log.gz")).write_bytes(_GZ_CONTENT)

    def tearDown(self):
        del self.client
        del self.app
        del self.td

    # --- GET / ---

    def test_index_status(self):
        res = self.client.get("/htmx/")
        self.assertEqual(200, res.status_code)
        self.assertIn("text/html", res.headers["content-type"])

    def test_index_lists_dirs(self):
        res = self.client.get("/htmx/")
        self.assertIn("applog", res.text)
        self.assertIn("syslog", res.text)

    def test_index_includes_htmx_script(self):
        res = self.client.get("/htmx/")
        self.assertIn("htmx.org", res.text)

    def test_index_theme_toggle_button(self):
        res = self.client.get("/htmx/")
        self.assertIn("theme-toggle", res.text)

    def test_index_css_variables(self):
        res = self.client.get("/htmx/")
        self.assertIn("--bg", res.text)
        self.assertIn("data-theme", res.text)

    # --- GET /{dir_path} ---

    def test_dir_page(self):
        res = self.client.get("/htmx/applog", params={"month": "2024-01"})
        self.assertEqual(200, res.status_code)
        self.assertIn("applog", res.text)
        self.assertIn("2024-01", res.text)

    def test_dir_page_with_date_shows_log(self):
        res = self.client.get("/htmx/applog", params={"month": "2024-01", "date": "2024-01-01"})
        self.assertEqual(200, res.status_code)
        self.assertIn("hello world", res.text)

    def test_dir_page_active_tab(self):
        res = self.client.get("/htmx/applog", params={"month": "2024-01"})
        # active class should be on applog tab
        idx = res.text.find("tab-btn active")
        self.assertGreater(idx, -1)
        self.assertIn("applog", res.text[idx : idx + 100])

    def test_dir_page_no_month_defaults_to_today(self):
        res = self.client.get("/htmx/applog")
        self.assertEqual(200, res.status_code)
        self.assertIn("applog", res.text)

    # --- GET /_main/{dir_path} ---

    def test_main_area_status(self):
        res = self.client.get("/htmx/_main/applog", params={"month": "2024-01"})
        self.assertEqual(200, res.status_code)

    def test_main_area_contains_sections(self):
        res = self.client.get("/htmx/_main/applog", params={"month": "2024-01"})
        self.assertIn("calendar-area", res.text)
        self.assertIn("log-content", res.text)
        self.assertIn("search", res.text)

    def test_main_area_pushurl_header(self):
        res = self.client.get("/htmx/_main/applog", params={"month": "2024-01"})
        self.assertIn("HX-Push-Url", res.headers)

    def test_main_area_auto_loads_latest(self):
        # tab click with no date → loads latest available log
        res = self.client.get("/htmx/_main/applog", params={"month": "2024-01"})
        self.assertIn("hello world", res.text)

    def test_main_area_no_month_defaults_to_today(self):
        # omitting month should not crash and should return a valid page
        res = self.client.get("/htmx/_main/applog")
        self.assertEqual(200, res.status_code)
        self.assertIn("calendar-area", res.text)

    # --- GET /_calendar/{dir_path} ---

    def test_calendar_status(self):
        res = self.client.get("/htmx/_calendar/applog", params={"month": "2024-01"})
        self.assertEqual(200, res.status_code)

    def test_calendar_shows_month(self):
        res = self.client.get("/htmx/_calendar/applog", params={"month": "2024-01"})
        self.assertIn("2024-01", res.text)

    def test_calendar_month_nav(self):
        res = self.client.get("/htmx/_calendar/applog", params={"month": "2024-01"})
        self.assertIn("2023-12", res.text)  # prev
        self.assertIn("2024-02", res.text)  # next

    def test_calendar_empty_month_message(self):
        res = self.client.get("/htmx/_calendar/applog", params={"month": "2023-01"})
        self.assertEqual(200, res.status_code)
        self.assertIn("この月のファイルなし", res.text)
        self.assertIn("2024-01", res.text)  # link to nearest month with files

    def test_calendar_no_month_defaults_to_today(self):
        res = self.client.get("/htmx/_calendar/applog")
        self.assertEqual(200, res.status_code)

    def test_calendar_weekday_css_classes(self):
        # Inline color styles must be replaced with CSS classes
        res = self.client.get("/htmx/_calendar/applog", params={"month": "2024-01"})
        self.assertIn("wd-5", res.text)  # Saturday header
        self.assertIn("wd-6", res.text)  # Sunday header
        self.assertNotIn("lightyellow", res.text)
        self.assertNotIn("lightcyan", res.text)

    # --- GET /_content/{file_path} ---

    def test_content_raw_file(self):
        res = self.client.get("/htmx/_content/applog/2024-01-01.log")
        self.assertEqual(200, res.status_code)
        self.assertIn("hello world", res.text)
        self.assertIn("log-line", res.text)

    def test_content_compressed_file(self):
        res = self.client.get("/htmx/_content/applog/2024-02-01.log")
        self.assertEqual(200, res.status_code)
        self.assertIn("hello world", res.text)

    def test_content_json_detected(self):
        res = self.client.get("/htmx/_content/applog/2024-01-01.log")
        self.assertIn("json-block", res.text)
        self.assertIn("copy-btn", res.text)

    def test_content_pagination_sentinel(self):
        res = self.client.get("/htmx/_content/applog/2024-01-01.log", params={"limit": 1})
        self.assertIn("sentinel", res.text)
        self.assertIn("offset=1", res.text)

    def test_content_no_sentinel_at_end(self):
        res = self.client.get("/htmx/_content/applog/2024-01-01.log", params={"offset": 9999})
        self.assertNotIn("sentinel", res.text)

    def test_content_line_ids(self):
        res = self.client.get("/htmx/_content/applog/2024-01-01.log")
        self.assertIn('id="line-0"', res.text)
        self.assertIn('id="line-1"', res.text)

    def test_content_not_found(self):
        # _find_file raises HTTPException inside the StreamingResponse generator.
        # HTTP 200 headers are already sent by the time the generator runs, so the
        # status stays 200 but the body is empty (stream aborted with no output).
        client = TestClient(self.app, raise_server_exceptions=False)
        res = client.get("/htmx/_content/applog/9999-01-01.log")
        self.assertEqual("", res.text)

    # --- GET /_search/{dir_path} ---

    def test_search_match(self):
        res = self.client.get("/htmx/_search/applog", params={"q": "hello", "month": "2024-01"})
        self.assertEqual(200, res.status_code)
        # "hello" is inside <mark>, so check surrounding context separately
        self.assertIn("<mark>hello</mark>", res.text)
        self.assertIn(" world", res.text)

    def test_search_highlights_match(self):
        res = self.client.get("/htmx/_search/applog", params={"q": "hello", "month": "2024-01"})
        self.assertIn("<mark>hello</mark>", res.text)

    def test_search_case_insensitive(self):
        res = self.client.get("/htmx/_search/applog", params={"q": "HELLO", "month": "2024-01"})
        self.assertIn("<mark>", res.text)

    def test_search_no_match(self):
        res = self.client.get("/htmx/_search/applog", params={"q": "zzznomatch", "month": "2024-01"})
        self.assertEqual(200, res.status_code)
        self.assertEqual("", res.text)

    def test_search_empty_query_returns_empty(self):
        res = self.client.get("/htmx/_search/applog", params={"q": "", "month": "2024-01"})
        self.assertEqual(200, res.status_code)
        self.assertEqual("", res.text)

    def test_search_line_id_format(self):
        # search results use line-{date}-{n} IDs
        res = self.client.get("/htmx/_search/applog", params={"q": "hello", "month": "2024-01"})
        self.assertIn("line-2024-01-", res.text)

    def test_search_json_detected(self):
        res = self.client.get("/htmx/_search/applog", params={"q": "key", "month": "2024-01"})
        self.assertIn("json-block", res.text)
