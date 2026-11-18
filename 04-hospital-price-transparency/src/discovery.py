"""Discovery: resolve a hospital's MRF URL from its cms-hpt.txt pointer file.

Per 45 CFR 180.50, every hospital hosts a ``cms-hpt.txt`` file at its website
root. The file is a sequence of blocks separated by blank lines; each block
has ``location-name:``, ``source-page-url:``, ``mrf-url:``, ``contact-name:``
and ``contact-email:`` lines. Some hospitals instead publish an
instructions-only file with no ``mrf-url`` lines — that is a discovery
failure, not a parseable pointer file.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import requests

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "hospital-price-transparency-pipeline/1.0 (+https://adhocpra.github.io/)"
)


@dataclass
class PointerEntry:
    location_name: str
    mrf_url: str
    source_page_url: str = ""
    contact_name: str = ""
    contact_email: str = ""


@dataclass
class DiscoveryResult:
    hospital_id: str
    pointer_url: str
    entries: list[PointerEntry] = field(default_factory=list)
    resolved_url: str | None = None
    method: str = ""  # "discovery" | "fallback" | "failed"
    detail: str = ""


def fetch_pointer_file(website_root: str, timeout: int = 30) -> str:
    """GET {website_root}/cms-hpt.txt and return its text.

    Raises requests.HTTPError / requests.RequestException on failure so the
    caller can fall back to the recorded MRF URL.
    """
    url = website_root.rstrip("/") + "/cms-hpt.txt"
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def parse_pointer_file(text: str) -> list[PointerEntry]:
    """Parse a cms-hpt.txt body into pointer entries.

    Blocks are separated by blank lines. Keys are ``key: value``; values may
    be empty. Lines that are not ``key: value`` (e.g. the decorative
    instructions banners some hospitals ship) are ignored.
    """
    entries: list[PointerEntry] = []
    current: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            if current.get("location-name") and current.get("mrf-url"):
                entries.append(_block_to_entry(current))
            current = {}
            continue
        m = re.match(r"^([a-z][a-z\-]*):\s*(.*)$", line)
        if m:
            current[m.group(1)] = m.group(2).strip()
    if current.get("location-name") and current.get("mrf-url"):
        entries.append(_block_to_entry(current))
    return entries


def _block_to_entry(block: dict[str, str]) -> PointerEntry:
    return PointerEntry(
        location_name=block.get("location-name", ""),
        mrf_url=block.get("mrf-url", ""),
        source_page_url=block.get("source-page-url", ""),
        contact_name=block.get("contact-name", ""),
        contact_email=block.get("contact-email", ""),
    )


def resolve_mrf_url(
    hospital: dict,
    fallback_url: str | None = None,
    timeout: int = 30,
) -> DiscoveryResult:
    """Resolve the MRF URL for one hospital from its config entry.

    Tries discovery from cms-hpt.txt first (matching ``location_match``
    case-insensitively against the block's location-name). On any discovery
    failure — HTTP error, no entries, or no location match — falls back to
    the recorded ``mrf_url`` and says so in ``method``/``detail``.
    """
    hid = hospital["id"]
    pointer_url = hospital["website_root"].rstrip("/") + "/cms-hpt.txt"
    try:
        text = fetch_pointer_file(hospital["website_root"], timeout=timeout)
        entries = parse_pointer_file(text)
    except Exception as exc:  # noqa: BLE001 — discovery failure is a fallback
        return DiscoveryResult(
            hospital_id=hid,
            pointer_url=pointer_url,
            method="fallback" if fallback_url else "failed",
            resolved_url=fallback_url,
            detail=f"discovery failed ({type(exc).__name__}: {exc}); "
            + ("using recorded mrf_url" if fallback_url else "no fallback recorded"),
        )
    if not entries:
        return DiscoveryResult(
            hospital_id=hid,
            pointer_url=pointer_url,
            method="fallback" if fallback_url else "failed",
            resolved_url=fallback_url,
            detail="cms-hpt.txt fetched but contains no mrf-url entries "
            "(instructions-only file); "
            + ("using recorded mrf_url" if fallback_url else "no fallback recorded"),
        )
    match = hospital.get("location_match", "").lower()
    chosen = next(
        (e for e in entries if match and match in e.location_name.lower()), None
    )
    if chosen is None:
        return DiscoveryResult(
            hospital_id=hid,
            pointer_url=pointer_url,
            entries=entries,
            method="fallback" if fallback_url else "failed",
            resolved_url=fallback_url,
            detail=f"no location matching {hospital.get('location_match')!r} "
            f"among {len(entries)} entries; "
            + ("using recorded mrf_url" if fallback_url else "no fallback recorded"),
        )
    return DiscoveryResult(
        hospital_id=hid,
        pointer_url=pointer_url,
        entries=entries,
        method="discovery",
        resolved_url=chosen.mrf_url,
        detail=f"resolved via cms-hpt.txt ({chosen.location_name})",
    )
