"""Streaming ingest of CMS hospital price transparency MRFs.

Reads JSON and CSV machine-readable files over HTTP without ever loading a
whole file into memory. Two hard caps bound every read so a 13 GB file
cannot stall a run:

* ``max_rows`` — maximum data rows (CSV) / items (JSON) scanned
* ``max_bytes`` — maximum bytes pulled off the wire

When a cap is hit the parser stops cleanly and reports partial coverage
(``scan_complete=False``) instead of pretending the file was fully read.

Supported layouts (CMS template v2.0 / v3.0, detected from the file itself):
* JSON — root object with ``standard_charge_information`` array.
* CSV "tall" — row 1 general headers, row 2 general values, row 3 data
  headers, then one row per (item, payer).
* CSV "wide" — same three header rows, then one row per item with one
  ``standard_charge | <payer> | <plan> | negotiated_dollar`` column set
  per payer/plan.
* ZIP containing a single CSV (e.g. Parkland, Cleveland Clinic).
"""

from __future__ import annotations

import codecs
import csv
import io
import itertools
import json
import re
import tempfile
import zipfile
from dataclasses import dataclass, field

import requests

from .discovery import USER_AGENT

CHUNK = 1 << 20  # 1 MB read chunks

# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------


@dataclass
class IngestStats:
    hospital_id: str
    source_url: str
    format_detected: str = ""  # json | csv-tall | csv-wide | zip-csv
    template_version: str = ""
    file_last_updated: str = ""
    hospital_name: str = ""
    npi: str = ""
    state: str = ""
    bytes_total: int | None = None  # Content-Length, when the server sends one
    bytes_scanned: int = 0
    rows_scanned: int = 0  # data rows (CSV) / items (JSON)
    matched_items: int = 0
    matched_rows: int = 0
    cpt_counts: dict[str, int] = field(default_factory=dict)
    scan_complete: bool = False
    cap_hit: str = ""  # "" | "max_rows" | "max_bytes"
    errors: list[str] = field(default_factory=list)


@dataclass
class MatchedItem:
    """One shoppable item, in the file's own terms, before normalization."""

    description: str
    codes: list[tuple[str, str]]  # (code, type), e.g. ("70553", "CPT")
    setting: str = ""
    gross_charge: float | None = None
    cash_price: float | None = None
    negotiated_min: float | None = None
    negotiated_max: float | None = None
    # one entry per payer/plan with a dollar negotiated charge
    payers: list[dict] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Byte-counted text streaming
# ---------------------------------------------------------------------------


def iter_text_chunks(resp: requests.Response, max_bytes: int | None):
    """Yield decoded text chunks from a streamed response, honoring max_bytes.

    Returns (chunks, bytes_read_fn). ``utf-8-sig`` strips a leading BOM,
    which several hospital JSON files carry. ``errors="replace"`` keeps one
    stray non-UTF-8 byte (Parkland's CSV has a 0xAD byte) from killing a
    whole file — the replacement lands in a description string, never in a
    price, which is ASCII.
    """
    decoder = codecs.getincrementaldecoder("utf-8-sig")(errors="replace")
    total = [0]

    def gen():
        for chunk in resp.iter_content(chunk_size=CHUNK):
            if not chunk:
                continue
            if max_bytes is not None:
                remaining = max_bytes - total[0]
                if remaining <= 0:
                    break
                chunk = chunk[:remaining]
            total[0] += len(chunk)
            yield decoder.decode(chunk)
            if max_bytes is not None and total[0] >= max_bytes:
                break
        tail = decoder.decode(b"", final=True)
        if tail:
            yield tail

    return gen(), total


def iter_lines(text_chunks):
    """Split text chunks into lines (csv.reader reassembles quoted newlines)."""
    buf = ""
    for chunk in text_chunks:
        buf += chunk
        while "\n" in buf:
            line, buf = buf.split("\n", 1)
            yield line.rstrip("\r")
    if buf:
        yield buf


# ---------------------------------------------------------------------------
# JSON streaming
# ---------------------------------------------------------------------------


class JsonArrayStreamer:
    """Yield objects from the ``standard_charge_information`` array of a
    streamed JSON MRF, one object at a time, using only the stdlib decoder."""

    def __init__(self, text_chunks, array_key: str = "standard_charge_information"):
        self._chunks = iter(text_chunks)
        self._buf = ""
        self._pos = 0
        self._eof = False
        self._decoder = json.JSONDecoder()
        self._key = array_key
        self.prefix = ""  # file text before the array (holds root metadata)

    def _fill(self, want: int = 65536) -> bool:
        """Read more text until at least ``want`` unconsumed chars or EOF."""
        while len(self._buf) - self._pos < want and not self._eof:
            try:
                chunk = next(self._chunks)
            except StopIteration:
                self._eof = True
                break
            self._buf += chunk
        if self._pos > 1_000_000:  # compact: drop consumed prefix
            self._buf = self._buf[self._pos:]
            self._pos = 0
        return len(self._buf) - self._pos >= want or self._eof

    def _find_array(self) -> None:
        needle = f'"{self._key}"'
        while True:
            idx = self._buf.find(needle, self._pos)
            if idx != -1:
                bracket = self._buf.find("[", idx + len(needle))
                if bracket != -1:
                    self.prefix = self._buf[:idx]
                    self._pos = bracket + 1
                    return
            if self._eof:
                raise ValueError(f"{self._key!r} array not found in JSON stream")
            self._fill(1 << 20)

    def __iter__(self):
        self._find_array()
        while True:
            self._fill(1)
            while self._pos < len(self._buf) and self._buf[self._pos] in " \t\r\n,":
                self._pos += 1
            if self._pos >= len(self._buf):
                if self._eof:
                    return
                self._fill(1)
                continue
            ch = self._buf[self._pos]
            if ch == "]":
                return
            if ch != "{":
                raise ValueError(f"unexpected character {ch!r} in JSON array")
            while True:
                try:
                    obj, end = self._decoder.raw_decode(self._buf, self._pos)
                except json.JSONDecodeError:
                    if self._eof:
                        raise ValueError("truncated JSON object at end of stream")
                    if len(self._buf) - self._pos > 64 * 1024 * 1024:
                        raise ValueError("single JSON object over 64 MB; aborting")
                    self._fill(1 << 20)
                    continue
                self._pos = end
                yield obj
                break


_META_STR_FMT = r'"%s"\s*:\s*"((?:[^"\\]|\\.)*)"'
_META_STATE = re.compile(r'"license_information"\s*:\s*\{[^}]*?"state"\s*:\s*"([A-Za-z]{2})"')
_META_NPI = re.compile(r'"type_2_npi"\s*:\s*\[\s*"(\d+)"')


def extract_json_meta(prefix: str) -> dict:
    """Pull root metadata out of the JSON text that precedes the charge array."""
    meta: dict[str, str] = {}
    for key in ("hospital_name", "last_updated_on", "version"):
        hit = re.search(_META_STR_FMT % key, prefix)
        if hit:
            meta[key] = hit.group(1)
    m = _META_STATE.search(prefix)
    if m:
        meta["state"] = m.group(1).upper()
    m = _META_NPI.search(prefix)
    if m:
        meta["npi"] = m.group(1)
    return meta


# ---------------------------------------------------------------------------
# Value helpers
# ---------------------------------------------------------------------------


def to_float(value) -> float | None:
    """Parse a charge value; blanks, 'N/A' and non-numeric junk become None."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip().replace("$", "").replace(",", "")
    if s in ("", "N/A", "NA", "null", "None", "-"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# CSV ingest
# ---------------------------------------------------------------------------


def _csv_meta(rows: list[list[str]]) -> dict:
    """Row 0 = general headers, row 1 = general values.

    Header casing varies by hospital (Parkland uses ``Hospital_Name`` /
    ``Last_updated_on`` / ``Version``; most others use lowercase), so keys
    are matched case-insensitively.
    """
    headers, values = rows[0], rows[1]
    meta = {h.strip().lower(): v for h, v in zip(headers, values)}
    out: dict[str, str] = {}
    out["hospital_name"] = meta.get("hospital_name", "").strip()
    out["last_updated_on"] = meta.get("last_updated_on", "").strip()
    out["version"] = meta.get("version", "").strip()
    npi_raw = meta.get("type_2_npi", "")
    out["npi"] = npi_raw.split("|")[0].strip() if npi_raw else ""
    lic = next((h for h in headers if h.strip().lower().startswith("license_number|")), "")
    out["state"] = lic.split("|", 1)[1].strip().upper() if "|" in lic else ""
    return out


def is_cpt_code(code: str, ctype: str) -> bool:
    """Does this (code, type) pair identify a CPT code?

    Real-world quirk found 2026-11-17: Memorial Hermann labels its CPT
    codes with type "HCPCS", not "CPT" (e.g. code 70553, type HCPCS).
    That is correct in a sense — CPT codes *are* HCPCS Level I codes —
    but it breaks a naive ``type == "CPT"`` filter. HCPCS Level II codes
    start with a letter, so a 5-digit all-numeric code typed HCPCS is a
    CPT code. An empty type is accepted too; the caller only ever matches
    codes against the curated 5-digit set, so a stray local code cannot
    slip through.
    """
    if not code:
        return False
    ctype = (ctype or "").strip().upper()
    if ctype in ("", "CPT"):
        return True
    return ctype == "HCPCS" and len(code) == 5 and code.isdigit()


def _is_cpt(code: str, ctype: str) -> bool:
    return is_cpt_code(code, ctype)


def _parse_csv_rows(
    reader: csv.reader,
    cpt_set: set[str],
    stats: IngestStats,
    max_rows: int | None,
) -> list[MatchedItem]:
    """Consume a CSV MRF. Returns matched items (tall rows grouped by item).

    A byte cap is enforced by the text-chunk source upstream: when the cap
    is hit the line iterator simply ends, and the caller records the cap
    from the byte counter (see ingest_url).
    """
    try:
        header_row = next(reader)
        value_row = next(reader)
        data_headers = next(reader)
    except StopIteration:
        stats.errors.append("CSV has fewer than 3 header rows")
        return []
    meta = _csv_meta([header_row, value_row])
    stats.template_version = meta["version"]
    stats.hospital_name = meta["hospital_name"]
    stats.file_last_updated = meta["last_updated_on"]
    stats.npi = meta["npi"]
    stats.state = meta["state"]

    idx = {h.strip(): i for i, h in enumerate(data_headers)}

    def col(name: str) -> int | None:
        return idx.get(name)

    # Tall layout: one row per (item, payer); payer_name column exists.
    # Wide layout: one row per item; per-payer columns named
    #   standard_charge | <payer> | <plan> | negotiated_dollar
    is_wide = "payer_name" not in idx
    stats.format_detected = "csv-wide" if is_wide else "csv-tall"

    code_cols = []  # (code_col, type_col)
    for h in data_headers:
        m = re.fullmatch(r"code\|(\d+)", h.strip())
        if m:
            code_cols.append((idx[h], idx.get(f"code|{m.group(1)}|type")))

    wide_payer_cols = []  # (payer, plan, dollar_col, median_col, p10_col, p90_col)
    if is_wide:
        for h in data_headers:
            m = re.fullmatch(
                r"standard_charge \| (.+) \| (.+) \| negotiated_dollar", h.strip()
            )
            if m:
                payer, plan = m.group(1), m.group(2)
                wide_payer_cols.append(
                    (
                        payer,
                        plan,
                        idx[h],
                        idx.get(f"median_amount | {payer} | {plan}"),
                        idx.get(f"10th_percentile | {payer} | {plan}"),
                        idx.get(f"90th_percentile | {payer} | {plan}"),
                    )
                )

    def cell(row: list[str], j: int | None) -> str:
        return row[j].strip() if j is not None and j < len(row) else ""

    items: list[MatchedItem] = []
    grouped: dict[tuple, MatchedItem] = {}

    def item_key(row: list[str]) -> tuple:
        return (
            cell(row, col("description")),
            cell(row, code_cols[0][0]) if code_cols else "",
            cell(row, col("setting")),
        )

    for row in reader:
        stats.rows_scanned += 1
        if max_rows is not None and stats.rows_scanned > max_rows:
            stats.cap_hit = "max_rows"
            break
        if is_wide:
            _handle_wide_row(row, items, stats, cpt_set, code_cols, wide_payer_cols,
                             col, cell)
        else:
            _handle_tall_row(row, grouped, stats, cpt_set, code_cols, col, cell)
    items.extend(grouped.values())
    _finalize_stats(items, stats)
    return items


def _row_cpts(row, code_cols, cell) -> list[str]:
    found = []
    for code_j, type_j in code_cols:
        code = cell(row, code_j)
        ctype = cell(row, type_j) if type_j is not None else ""
        if code and _is_cpt(code, ctype):
            found.append(code)
    return found


def _handle_tall_row(row, grouped, stats, cpt_set, code_cols, col, cell) -> None:
    """Merge one tall row into the per-CPT group.

    Tall files list the same CPT under several descriptions (chargemaster
    line vs. payer-facing description) and settings ("both" on the item row,
    "outpatient" on payer rows), so rows are grouped by CPT code alone.
    The Gold layer aggregates per (hospital, CPT) anyway, so this loses
    nothing the dashboard uses; it only keeps gross/cash attached to the
    payer rows of the same procedure.
    """
    cpts = [c for c in _row_cpts(row, code_cols, cell) if c in cpt_set]
    if not cpts:
        return
    stats.matched_rows += 1
    for cpt in cpts:
        item = grouped.get(cpt)
        if item is None:
            item = MatchedItem(
                description=cell(row, col("description")),
                codes=[(cpt, "CPT")],
                setting=cell(row, col("setting")),
            )
            grouped[cpt] = item
        # item-level amounts live on the payer-less row; payer rows carry the rest
        gross = to_float(cell(row, col("standard_charge|gross")))
        cash = to_float(cell(row, col("standard_charge|discounted_cash")))
        if gross is not None and item.gross_charge is None:
            item.gross_charge = gross
        if cash is not None and item.cash_price is None:
            item.cash_price = cash
        nmin = to_float(cell(row, col("standard_charge|min")))
        nmax = to_float(cell(row, col("standard_charge|max")))
        if nmin is not None:
            item.negotiated_min = nmin if item.negotiated_min is None else min(item.negotiated_min, nmin)
        if nmax is not None:
            item.negotiated_max = nmax if item.negotiated_max is None else max(item.negotiated_max, nmax)
        payer = cell(row, col("payer_name"))
        dollar = to_float(cell(row, col("standard_charge|negotiated_dollar")))
        if payer and dollar is not None:
            item.payers.append(
                {
                    "payer_name": payer,
                    "plan_name": cell(row, col("plan_name")),
                    "dollar": dollar,
                    "median": to_float(cell(row, col("median_amount"))),
                    "p10": to_float(cell(row, col("10th_percentile"))),
                    "p90": to_float(cell(row, col("90th_percentile"))),
                }
            )


def _handle_wide_row(row, items, stats, cpt_set, code_cols, wide_payer_cols, col, cell) -> None:
    cpts = [c for c in _row_cpts(row, code_cols, cell) if c in cpt_set]
    if not cpts:
        return
    stats.matched_rows += 1
    item = MatchedItem(
        description=cell(row, col("description")),
        codes=[(c, "CPT") for c in cpts],
        setting=cell(row, col("setting")),
        gross_charge=to_float(cell(row, col("standard_charge|gross"))),
        cash_price=to_float(cell(row, col("standard_charge|discounted_cash"))),
        negotiated_min=to_float(cell(row, col("standard_charge|min"))),
        negotiated_max=to_float(cell(row, col("standard_charge|max"))),
    )
    for payer, plan, dj, mj, p10j, p90j in wide_payer_cols:
        dollar = to_float(cell(row, dj))
        if dollar is not None:
            item.payers.append(
                {
                    "payer_name": payer,
                    "plan_name": plan,
                    "dollar": dollar,
                    "median": to_float(cell(row, mj)),
                    "p10": to_float(cell(row, p10j)),
                    "p90": to_float(cell(row, p90j)),
                }
            )
    items.append(item)


# ---------------------------------------------------------------------------
# JSON ingest
# ---------------------------------------------------------------------------


def _parse_json_stream(text_chunks, cpt_set: set[str], stats: IngestStats,
                      max_rows: int | None) -> list[MatchedItem]:
    streamer = JsonArrayStreamer(text_chunks)
    items: list[MatchedItem] = []
    for obj in streamer:
        stats.rows_scanned += 1
        if max_rows is not None and stats.rows_scanned > max_rows:
            stats.cap_hit = "max_rows"
            break
        codes = [
            (str(ci.get("code", "")).strip(), str(ci.get("type", "")).strip())
            for ci in obj.get("code_information", []) or []
            if isinstance(ci, dict)
        ]
        cpts = [c for c, t in codes if c in cpt_set and is_cpt_code(c, t)]
        if not cpts:
            continue
        stats.matched_rows += 1
        charges = obj.get("standard_charges", []) or []
        for charge in charges:
            if not isinstance(charge, dict):
                continue
            item = MatchedItem(
                description=str(obj.get("description", "")).strip(),
                codes=[(c, "CPT") for c in cpts],
                setting=str(charge.get("setting", "")).strip(),
                gross_charge=to_float(charge.get("gross_charge")),
                cash_price=to_float(charge.get("discounted_cash")),
                negotiated_min=to_float(charge.get("minimum")),
                negotiated_max=to_float(charge.get("maximum")),
            )
            for p in charge.get("payers_information", []) or []:
                if not isinstance(p, dict):
                    continue
                dollar = to_float(p.get("standard_charge_dollar"))
                if dollar is None:
                    continue
                item.payers.append(
                    {
                        "payer_name": str(p.get("payer_name", "")).strip(),
                        "plan_name": str(p.get("plan_name", "")).strip(),
                        "dollar": dollar,
                        "median": to_float(p.get("median_amount")),
                        "p10": to_float(p.get("10th_percentile")),
                        "p90": to_float(p.get("90th_percentile")),
                    }
                )
            items.append(item)
    meta = extract_json_meta(streamer.prefix)
    stats.hospital_name = meta.get("hospital_name", "")
    stats.file_last_updated = meta.get("last_updated_on", "")
    stats.template_version = meta.get("version", "")
    stats.npi = meta.get("npi", "")
    stats.state = meta.get("state", "")
    stats.format_detected = "json"
    _finalize_stats(items, stats)
    return items


# ---------------------------------------------------------------------------
# Top-level ingest
# ---------------------------------------------------------------------------


def _caps(scan) -> tuple[int | None, int | None]:
    if scan == "full" or scan is None:
        return None, None
    if isinstance(scan, dict):
        return scan.get("max_rows"), scan.get("max_bytes")
    return None, None


def ingest_url(
    url: str,
    hospital_id: str,
    cpt_set: set[str],
    scan="full",
    timeout: tuple[int, int] = (30, 300),
) -> tuple[list[MatchedItem], IngestStats]:
    """Stream one MRF URL and return matched items + stats.

    ``scan`` is "full" or {"max_rows": N, "max_bytes": B}. The caller decides
    the format from the URL/content-type; ZIPs are downloaded whole (bounded)
    and their single CSV member is streamed.
    """
    max_rows, max_bytes = _caps(scan)
    stats = IngestStats(hospital_id=hospital_id, source_url=url)
    resp = requests.get(
        url, headers={"User-Agent": USER_AGENT}, timeout=timeout, stream=True
    )
    resp.raise_for_status()
    stats.bytes_total = (
        int(resp.headers["content-length"])
        if resp.headers.get("content-length", "").isdigit()
        else None
    )
    ctype = (resp.headers.get("content-type") or "").lower()
    is_zip = "zip" in ctype or url.lower().endswith(".zip")
    if is_zip:
        return _ingest_zip(resp, hospital_id, cpt_set, stats, max_rows, max_bytes)
    chunks, total = iter_text_chunks(resp, max_bytes)
    # Content sniffing decides JSON vs CSV: content-type and file extension
    # lie (Houston Methodist serves JSON as application/octet-stream from
    # a .ashx URL). A JSON MRF always opens with "{" after BOM/whitespace.
    first_chunk = next(chunks, "")
    sniff = first_chunk.lstrip("﻿ \t\r\n")
    is_json = sniff.startswith("{")
    chunks = itertools.chain([first_chunk], chunks)
    stats.format_detected = "json" if is_json else "csv"
    try:
        if is_json:
            items = _parse_json_stream(chunks, cpt_set, stats, max_rows)
        else:
            items = _parse_csv_rows(
                csv.reader(iter_lines(chunks)), cpt_set, stats, max_rows
            )
    finally:
        resp.close()
    stats.bytes_scanned = total[0]
    if not stats.cap_hit and max_bytes is not None and total[0] >= max_bytes:
        # the byte source stopped early; only a cap if the file is bigger
        if stats.bytes_total is None or stats.bytes_total > max_bytes:
            stats.cap_hit = "max_bytes"
    stats.scan_complete = not stats.cap_hit
    _finalize_stats(items, stats)
    return items, stats


def _ingest_zip(resp, hospital_id, cpt_set, stats, max_rows, max_bytes):
    """Download a ZIP MRF (bounded), stream its single CSV member."""
    stats.format_detected = "zip-csv"
    content_length = resp.headers.get("content-length")
    if content_length and int(content_length) > 200 * 1024 * 1024:
        stats.errors.append("zip over 200 MB download guard; skipped")
        return [], stats
    data = resp.content  # bounded by the guard above; zip members stream after
    stats.bytes_scanned = len(data)
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=True) as tmp:
        tmp.write(data)
        tmp.flush()
        with zipfile.ZipFile(tmp.name) as zf:
            members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not members:
                stats.errors.append("zip contains no .csv member")
                return [], stats
            member = max(members, key=lambda n: zf.getinfo(n).file_size)
            with zf.open(member) as raw:
                chunks, decomp_total = _zip_text_chunks(raw, max_bytes)
                items = _parse_csv_rows(
                    csv.reader(iter_lines(chunks)), cpt_set, stats, max_rows
                )
                stats.bytes_scanned = len(data)  # zip bytes on the wire
                if (
                    not stats.cap_hit
                    and max_bytes is not None
                    and decomp_total[0] >= max_bytes
                ):
                    stats.cap_hit = "max_bytes"  # decompressed stream capped
    if not stats.cap_hit:
        stats.scan_complete = True
    _finalize_stats(items, stats)
    return items, stats


def _zip_text_chunks(raw, max_bytes):
    """Yield decoded text chunks from an open zip member, honoring max_bytes.

    Returns (chunks, bytes_read) where bytes_read[0] is decompressed bytes.
    """
    decoder = codecs.getincrementaldecoder("utf-8-sig")(errors="replace")
    total = [0]

    def gen():
        while True:
            chunk = raw.read(CHUNK)
            if not chunk:
                break
            if max_bytes is not None:
                remaining = max_bytes - total[0]
                if remaining <= 0:
                    break
                chunk = chunk[:remaining]
            total[0] += len(chunk)
            yield decoder.decode(chunk)
            if max_bytes is not None and total[0] >= max_bytes:
                break
        tail = decoder.decode(b"", final=True)
        if tail:
            yield tail

    return gen(), total


def _finalize_stats(items, stats: IngestStats) -> None:
    stats.matched_items = len(items)
    counts: dict[str, int] = {}
    for item in items:
        for code, _ in item.codes:
            counts[code] = counts.get(code, 0) + 1
    stats.cpt_counts = counts
