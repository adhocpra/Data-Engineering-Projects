"""Tests for the streaming MRF parsers — CSV tall/wide and JSON.

Parsers are fed in-memory fixtures so the suite runs offline in seconds.
"""

import csv
import io

from src.ingest import (
    IngestStats,
    JsonArrayStreamer,
    _parse_csv_rows,
    _parse_json_stream,
    extract_json_meta,
)

CPT = {"70553", "99213", "80053"}


def _csv_reader(text: str):
    return csv.reader(io.StringIO(text))


def test_csv_tall_groups_item_and_payer_rows():
    text = (
        "hospital_name,last_updated_on,version,location_name,hospital_address,type_2_npi,license_number|TX,To the best...,attester_name\n"
        "Test Hospital,2026-04-01,3.0.0,Test Hospital,1 Main St,1234567890,000001,true,Jane Doe\n"
        "description,code|1,code|1|type,setting,standard_charge|gross,standard_charge|discounted_cash,payer_name,plan_name,standard_charge|negotiated_dollar,standard_charge|min,standard_charge|max\n"
        "MRI BRAIN W WO CONTRAST,70553,CPT,outpatient,6541.25,2093.20,,,,,\n"
        "Mri brain w/o & w/dye,70553,CPT,outpatient,,,Acme Health,All Plans,665.88,350.46,665.88\n"
        "Office visit level 3,99213,CPT,outpatient,344.00,110.08,,,,,\n"
        "Comprehensive metabolic panel,80053,CPT,outpatient,785.50,251.36,,,,,\n"
        "Something else,12345,CPT,outpatient,10.00,5.00,,,,,\n"
    )
    stats = IngestStats(hospital_id="test", source_url="http://example.test/x.csv")
    items = _parse_csv_rows(_csv_reader(text), CPT, stats, None)
    assert stats.format_detected == "csv-tall"
    assert stats.rows_scanned == 5
    assert stats.matched_items == 3
    assert stats.cpt_counts == {"70553": 1, "99213": 1, "80053": 1}
    assert stats.hospital_name == "Test Hospital"
    assert stats.npi == "1234567890"
    assert stats.state == "TX"
    mri = next(i for i in items if i.codes[0][0] == "70553")
    assert mri.gross_charge == 6541.25
    assert mri.cash_price == 2093.20
    assert mri.negotiated_min == 350.46
    assert mri.negotiated_max == 665.88
    assert mri.payers[0]["payer_name"] == "Acme Health"
    assert mri.payers[0]["dollar"] == 665.88
    visit = next(i for i in items if i.codes[0][0] == "99213")
    assert visit.payers == []  # item-only row, no payer rows


def test_csv_wide_expands_payer_columns():
    text = (
        "hospital_name,last_updated_on,version,location_name,hospital_address,type_2_npi,license_number|OK,To the best...,attester_name\n"
        "Wide Hospital,2026-01-15,3.0.0,Wide Hospital,2 Oak Ave,0987654321,000002,true,John Roe\n"
        "description,code|1,code|1|type,setting,standard_charge|gross,standard_charge|discounted_cash,standard_charge | Acme | Gold | negotiated_dollar,standard_charge | Beta | Silver | negotiated_dollar,standard_charge|min,standard_charge|max\n"
        "MRI BRAIN W WO CONTRAST,70553,CPT,outpatient,5000.00,1600.00,900.00,750.00,700.00,900.00\n"
    )
    stats = IngestStats(hospital_id="test", source_url="http://example.test/y.csv")
    items = _parse_csv_rows(_csv_reader(text), CPT, stats, None)
    assert stats.format_detected == "csv-wide"
    assert len(items) == 1
    assert items[0].gross_charge == 5000.00
    assert {p["payer_name"] for p in items[0].payers} == {"Acme", "Beta"}
    assert {p["dollar"] for p in items[0].payers} == {900.00, 750.00}


def test_csv_capitalized_meta_headers():
    # Parkland (2026-11-17): row-1 headers are capitalized
    # (Hospital_Name, Last_updated_on, Version) and must still parse.
    text = (
        "Hospital_Name,Last_updated_on,Version,location_name,hospital_address,license_number|TX,type_2_npi,attester_name\n"
        "Parkland Health,2026-10-16,3.0.0,Parkland Health,5200 Harry Hines Blvd,00474 TX,1932123247,Kelli Rossi\n"
        "description,code|1,code|1|type,setting,standard_charge|gross,standard_charge|discounted_cash,payer_name,plan_name,standard_charge|negotiated_dollar,standard_charge|min,standard_charge|max\n"
        "MRI BRAIN,70553,CPT,outpatient,5000.00,1600.00,,,,,\n"
    )
    stats = IngestStats(hospital_id="test", source_url="http://example.test/p.csv")
    items = _parse_csv_rows(_csv_reader(text), CPT, stats, None)
    assert stats.hospital_name == "Parkland Health"
    assert stats.file_last_updated == "2026-10-16"
    assert stats.template_version == "3.0.0"
    assert stats.npi == "1932123247"
    assert stats.state == "TX"
    assert len(items) == 1


def test_csv_hcpcs_type_counts_as_cpt_but_not_level_ii():
    # Real-world quirk (Memorial Hermann, 2026-11-17): CPT codes are typed
    # "HCPCS", not "CPT". HCPCS Level II codes start with a letter and must
    # NOT match.
    text = (
        "hospital_name,last_updated_on,version,location_name,hospital_address,type_2_npi,license_number|TX,To the best...,attester_name\n"
        "Test Hospital,2026-04-01,3.0.0,Test Hospital,1 Main St,1234567890,000001,true,Jane Doe\n"
        "description,code|1,code|1|type,code|2,code|2|type,setting,standard_charge|gross,standard_charge|discounted_cash,payer_name,plan_name,standard_charge|negotiated_dollar,standard_charge|min,standard_charge|max\n"
        "MRI BRAIN,6117055302,CDM,70553,HCPCS,outpatient,6541.25,2093.20,,,,,\n"
        "Some drug,J1234,HCPCS,, ,outpatient,100.00,50.00,,,,,\n"
    )
    stats = IngestStats(hospital_id="test", source_url="http://example.test/x.csv")
    items = _parse_csv_rows(_csv_reader(text), CPT, stats, None)
    assert stats.matched_items == 1
    assert items[0].codes[0][0] == "70553"
    assert items[0].gross_charge == 6541.25


def test_csv_row_cap_stops_scan():
    rows = "\n".join(
        ["description,code|1,code|1|type,setting,standard_charge|gross,standard_charge|discounted_cash,payer_name,plan_name,standard_charge|negotiated_dollar,standard_charge|min,standard_charge|max"]
        + [f"Row {i},70553,CPT,outpatient,100.00,50.00,,,," for i in range(50)]
    )
    text = (
        "hospital_name,last_updated_on,version,location_name,hospital_address,type_2_npi,license_number|TX,att,attester_name\n"
        "H,2026-04-01,3.0.0,H,addr,1234567890,1,true,X\n" + rows + "\n"
    )
    stats = IngestStats(hospital_id="test", source_url="http://example.test/z.csv")
    items = _parse_csv_rows(_csv_reader(text), CPT, stats, max_rows=10)
    assert stats.cap_hit == "max_rows"
    assert stats.rows_scanned == 11  # 10 scanned + the row that trips the cap


def test_json_streamer_yields_objects_and_meta():
    doc = (
        '{"hospital_name":"Json Hospital","last_updated_on":"2026-03-01",'
        '"version":"3.0.0","license_information":{"license_number":"1","state":"MN"},'
        '"type_2_npi":["1234567890"],'
        '"standard_charge_information":['
        '{"description":"MRI BRAIN","code_information":[{"code":"70553","type":"CPT"}],'
        '"standard_charges":[{"setting":"outpatient","gross_charge":6000.0,"discounted_cash":2000.0,'
        '"minimum":500.0,"maximum":1200.0,'
        '"payers_information":[{"payer_name":"Acme","plan_name":"Gold","standard_charge_dollar":800.0,"median_amount":780.0}]}]},'
        '{"description":"Unrelated","code_information":[{"code":"99999","type":"CPT"}],'
        '"standard_charges":[{"setting":"outpatient","gross_charge":1.0}]}'
        ']}'
    )
    # feed the document in awkward chunks to prove incremental parsing
    chunks = [doc[i : i + 37] for i in range(0, len(doc), 37)]
    streamer = JsonArrayStreamer(iter(chunks))
    objs = list(streamer)
    assert len(objs) == 2
    meta = extract_json_meta(streamer.prefix)
    assert meta["hospital_name"] == "Json Hospital"
    assert meta["state"] == "MN"
    assert meta["npi"] == "1234567890"
    assert meta["version"] == "3.0.0"

    stats = IngestStats(hospital_id="test", source_url="http://example.test/j.json")
    chunks = [doc[i : i + 37] for i in range(0, len(doc), 37)]
    items = _parse_json_stream(iter(chunks), CPT, stats, None)
    assert stats.rows_scanned == 2
    assert stats.matched_items == 1
    mri = items[0]
    assert mri.gross_charge == 6000.0
    assert mri.cash_price == 2000.0
    assert mri.negotiated_min == 500.0
    assert mri.payers[0]["payer_name"] == "Acme"
    assert mri.payers[0]["median"] == 780.0
