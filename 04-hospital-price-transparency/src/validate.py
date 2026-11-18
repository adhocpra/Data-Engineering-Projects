"""Data-quality checks over the Silver layer.

Each check returns a CheckResult; ``severity`` is "error" (a row that must
not ship) or "warning" (suspicious, reported but not blocking). The same
functions back the pytest suite in tests/ and the run-time validation
report, so the numbers in the run log and the test suite always agree.
"""

from __future__ import annotations

from dataclasses import dataclass, field

US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI",
    "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN",
    "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH",
    "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA",
    "WV", "WI", "WY",
}


@dataclass
class CheckResult:
    name: str
    severity: str  # "error" | "warning"
    passed: int
    failed: int
    examples: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.failed == 0


def _is_pos_num(v) -> bool:
    return isinstance(v, (int, float)) and v > 0


def check_required_fields(rows: list[dict]) -> CheckResult:
    # NPI is deliberately NOT in this list: some source files do not publish
    # one (Norman Regional's v2.2.0 CSV has no type_2_npi column at all).
    # A missing NPI is a warning (check_npi_present); a malformed one is an
    # error (check_npi_format).
    failed = 0
    examples: list[str] = []
    for r in rows:
        missing = [c for c in ("hospital_name", "state", "cpt_code", "description")
                   if not r.get(c)]
        if missing:
            failed += 1
            if len(examples) < 3:
                examples.append(f"{r.get('hospital_id')}/{r.get('cpt_code')}: missing {missing}")
    return CheckResult("required_fields_non_null", "error", len(rows) - failed, failed, examples)


def check_cpt_valid(rows: list[dict], allowed: set[str]) -> CheckResult:
    failed = 0
    examples: list[str] = []
    for r in rows:
        cpt = str(r.get("cpt_code") or "")
        if len(cpt) != 5 or not cpt.isdigit() or cpt not in allowed:
            failed += 1
            if len(examples) < 3:
                examples.append(f"{r.get('hospital_id')}: bad CPT {cpt!r}")
    return CheckResult("cpt_valid_5_digit_in_set", "error", len(rows) - failed, failed, examples)


def check_state_valid(rows: list[dict]) -> CheckResult:
    failed = 0
    examples: list[str] = []
    for r in rows:
        if (r.get("state") or "") not in US_STATES:
            failed += 1
            if len(examples) < 3:
                examples.append(f"{r.get('hospital_id')}: bad state {r.get('state')!r}")
    return CheckResult("state_valid_us_code", "error", len(rows) - failed, failed, examples)


def check_npi_format(rows: list[dict]) -> CheckResult:
    """A present NPI must be 10 digits. A missing NPI is not a failure here —
    it is reported separately as a warning (check_npi_present)."""
    failed = 0
    examples: list[str] = []
    for r in rows:
        npi = str(r.get("npi") or "")
        if npi and (len(npi) != 10 or not npi.isdigit()):
            failed += 1
            if len(examples) < 3:
                examples.append(f"{r.get('hospital_id')}: bad NPI {npi!r}")
    return CheckResult("npi_10_digits_when_present", "error", len(rows) - failed, failed, examples)


def check_npi_present(rows: list[dict]) -> CheckResult:
    """Warn when a hospital's rows carry no NPI (its MRF does not publish one).

    The pipeline never guesses an NPI: for Norman Regional, the public NPPES
    registry lists two active Type 2 NPIs for the same address
    (1427244409 and 1922273937), and picking one would risk publishing the
    wrong identifier.
    """
    failed = 0
    examples: list[str] = []
    for r in rows:
        if not (r.get("npi") or ""):
            failed += 1
            if len(examples) < 3:
                examples.append(f"{r.get('hospital_id')}: no NPI published in MRF")
    return CheckResult("npi_present", "warning", len(rows) - failed, failed, examples)


def check_prices_positive(rows: list[dict]) -> CheckResult:
    failed = 0
    examples: list[str] = []
    for r in rows:
        for col in ("gross_charge", "cash_price", "negotiated_dollar",
                    "negotiated_min", "negotiated_max"):
            v = r.get(col)
            if v is not None and not _is_pos_num(v):
                failed += 1
                if len(examples) < 3:
                    examples.append(
                        f"{r.get('hospital_id')}/{r.get('cpt_code')}: {col}={v!r}")
                break
    return CheckResult("prices_positive_where_present", "error", len(rows) - failed, failed, examples)


def check_cash_le_gross(rows: list[dict]) -> CheckResult:
    """Discounted cash price should not exceed the gross charge.

    A small number of hospitals publish cash > gross (data errors on their
    side); those rows are flagged, not silently dropped.
    """
    failed = 0
    examples: list[str] = []
    for r in rows:
        gross, cash = r.get("gross_charge"), r.get("cash_price")
        if gross is not None and cash is not None and cash > gross:
            failed += 1
            if len(examples) < 3:
                examples.append(
                    f"{r.get('hospital_id')}/{r.get('cpt_code')}: "
                    f"cash ${cash:,.2f} > gross ${gross:,.2f}")
    return CheckResult("cash_le_gross", "warning", len(rows) - failed, failed, examples)


def check_min_le_max(rows: list[dict]) -> CheckResult:
    failed = 0
    examples: list[str] = []
    for r in rows:
        lo, hi = r.get("negotiated_min"), r.get("negotiated_max")
        if lo is not None and hi is not None and lo > hi:
            failed += 1
            if len(examples) < 3:
                examples.append(
                    f"{r.get('hospital_id')}/{r.get('cpt_code')}: min ${lo:,.2f} > max ${hi:,.2f}")
    return CheckResult("negotiated_min_le_max", "warning", len(rows) - failed, failed, examples)


def run_all_checks(rows: list[dict], allowed_cpts: set[str]) -> list[CheckResult]:
    return [
        check_required_fields(rows),
        check_cpt_valid(rows, allowed_cpts),
        check_state_valid(rows),
        check_npi_format(rows),
        check_npi_present(rows),
        check_prices_positive(rows),
        check_cash_le_gross(rows),
        check_min_le_max(rows),
    ]
