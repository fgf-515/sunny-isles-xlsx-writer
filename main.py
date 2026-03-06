import os
import uuid
from io import BytesIO
from datetime import datetime, date
from typing import Optional, List, Dict, Any, Tuple

import requests
import openpyxl
from fastapi import FastAPI, UploadFile, File, Form, Header, HTTPException
from fastapi.responses import StreamingResponse

app = FastAPI()

ARCGIS_QUERY_URL = (
    "https://arcgis.gdsc.miami.edu/arcgis/rest/services/"
    "mdc_property_point_view/FeatureServer/0/query"
)

DEFAULT_OUTFIELDS = (
    "objectid,folio,true_site_addr,true_site_unit,true_site_city,true_site_zip_code,"
    "true_owner1,true_owner2,true_owner3,"
    "true_mailing_addr1,true_mailing_addr2,true_mailing_addr3,"
    "true_mailing_city,true_mailing_state,true_mailing_zip_code,true_mailing_country,"
    "condo_flag,parent_folio"
)

def _require_api_key(x_api_key: Optional[str]) -> None:
    expected = os.environ.get("API_KEY")
    if not expected:
        raise HTTPException(status_code=500, detail="Server misconfigured: API_KEY not set")
    if not x_api_key or x_api_key != expected:
        raise HTTPException(status_code=401, detail="Invalid API key")

def _classify_owner_type(owner_raw: str) -> str:
    s = (owner_raw or "").upper()
    if any(k in s for k in ["LLC", "INC", "CORP", "CO ", " LTD", "LP", "LLP", "HOLDINGS", "PARTNERS"]):
        return "Entity"
    if any(k in s for k in ["TRUST", "TRUSTEE"]):
        return "Trust"
    if s.strip():
        return "Individual"
    return "Unknown"

def _join_owner(*parts: Optional[str]) -> str:
    vals = [p.strip() for p in parts if isinstance(p, str) and p.strip()]
    return "|".join(vals)

def _arcgis_page(where: str, out_fields: str, offset: int, count: int) -> Dict[str, Any]:
    params = {
        "f": "json",
        "where": where,
        "outFields": out_fields,
        "returnGeometry": "false",
        "orderByFields": "objectid",
        "resultOffset": str(offset),
        "resultRecordCount": str(count),
    }
    r = requests.get(ARCGIS_QUERY_URL, params=params, timeout=30)
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"ArcGIS HTTP {r.status_code}: {r.text[:500]}")
    data = r.json()
    if "error" in data:
        raise HTTPException(status_code=502, detail=f"ArcGIS error: {data['error']}")
    return data

def _arcgis_fetch_all(where: str, out_fields: str) -> Tuple[List[Dict[str, Any]], str]:
    all_features: List[Dict[str, Any]] = []
    offset = 0
    page_size = 2000

    while True:
        data = _arcgis_page(where, out_fields, offset, page_size)
        feats = data.get("features", []) or []
        all_features.extend(feats)

        exceeded = bool(data.get("exceededTransferLimit"))
        if not exceeded:
            break
        if len(feats) == 0:
            break
        offset += page_size

    # provenance URL (for logging)
    source_url = f"{ARCGIS_QUERY_URL}?where={where}"
    return all_features, source_url

def _get_header_map(ws) -> Dict[str, int]:
    headers = [c.value for c in ws[1]]
    return {str(h).strip(): i for i, h in enumerate(headers) if h is not None}

def _cell(ws, row: int, col_name: str, header_map: Dict[str, int]):
    return ws.cell(row=row, column=header_map[col_name] + 1)

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/run/building")
async def run_building(
    workbook: UploadFile = File(...),
    target_name: str = Form(...),
    street_number: str = Form(...),
    street_keyword: str = Form(...),
    city: str = Form("Sunny Isles Beach"),
    state: str = Form("FL"),
    county: str = Form("Miami-Dade"),
    market: str = Form("South Florida"),
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
):
    _require_api_key(x_api_key)

    where = (
        f"true_site_city='{city}' "
        f"AND UPPER(true_site_addr) LIKE '%{street_number}%' "
        f"AND UPPER(true_site_addr) LIKE '%{street_keyword.upper()}%'"
    )

    features, source_url = _arcgis_fetch_all(where=where, out_fields=DEFAULT_OUTFIELDS)

    content = await workbook.read()
    wb = openpyxl.load_workbook(BytesIO(content))
    ws_leads = wb["Leads"]
    ws_targets = wb["Targets"]
    ws_log = wb["Run_Log"]

    leads_map = _get_header_map(ws_leads)
    targets_map = _get_header_map(ws_targets)

    # Dedupe set
    existing = set()
    for r in range(2, ws_leads.max_row + 1):
        folio = _cell(ws_leads, r, "folio", leads_map).value
        unit = _cell(ws_leads, r, "unit", leads_map).value
        if folio is None:
            continue
        key = (str(folio).strip(), ("" if unit is None else str(unit).strip()))
        existing.add(key)

    records_added = 0
    duplicates_skipped = 0
    now_dt = datetime.now()
    today = date.today()

    for feat in features:
        attr = (feat.get("attributes") or {})
        folio = str(attr.get("folio") or "").strip()
        unit = attr.get("true_site_unit")
        unit_s = "" if unit is None else str(unit).strip()
        if not folio:
            continue

        key = (folio, unit_s)
        if key in existing:
            duplicates_skipped += 1
            continue

        owner_raw = _join_owner(attr.get("true_owner1"), attr.get("true_owner2"), attr.get("true_owner3"))
        owner_type = _classify_owner_type(owner_raw)

        # Build row by header names (your template must match these column names)
        row = {h: None for h in leads_map.keys()}
        row.update({
            "record_id": str(uuid.uuid4()),
            "created_at": now_dt,
            "last_verified_at": today,
            "status": "New",
            "market": market,
            "county": county,
            "state": state,
            "property_address_1": attr.get("true_site_addr"),
            "unit": None if unit_s == "" else unit_s,
            "city": attr.get("true_site_city"),
            "zip": attr.get("true_site_zip_code"),
            "folio": folio,
            "condo_flag": attr.get("condo_flag"),
            "owner_name_raw": owner_raw,
            "owner_type": owner_type,
            "mailing_address_1": attr.get("true_mailing_addr1"),
            "mailing_address_2": attr.get("true_mailing_addr2"),
            "mailing_address_3": attr.get("true_mailing_addr3"),
            "mailing_city": attr.get("true_mailing_city"),
            "mailing_state": attr.get("true_mailing_state"),
            "mailing_zip": attr.get("true_mailing_zip_code"),
            "mailing_country": attr.get("true_mailing_country"),
            "source_primary": "MDC Open Data",
            "source_urls": source_url,
        })

        out_row = [None] * len(leads_map)
        for col_name, idx in leads_map.items():
            out_row[idx] = row.get(col_name)

        ws_leads.append(out_row)
        existing.add(key)
        records_added += 1

    # Update Targets row (match by value)
    target_updated = False
    for r in range(2, ws_targets.max_row + 1):
        val = _cell(ws_targets, r, "value", targets_map).value
        ttype = _cell(ws_targets, r, "target_type", targets_map).value
        if (ttype == "Building") and isinstance(val, str) and (val.strip() == target_name.strip()):
            _cell(ws_targets, r, "status", targets_map).value = "Completed"
            _cell(ws_targets, r, "completed_at", targets_map).value = today
            notes_cell = _cell(ws_targets, r, "notes", targets_map)
            prev = (notes_cell.value or "")
            extra = f" run={today} added={records_added} skipped={duplicates_skipped} where={where}"
            notes_cell.value = (prev + " | " + extra).strip(" |")
            target_updated = True
            break

    # Append Run_Log row (columns assumed in your template)
    ws_log.append([
        str(uuid.uuid4()),
        now_dt,
        f"Building: {target_name}",
        records_added,
        duplicates_skipped,
        "" if target_updated else "Target row not found / not updated",
        "Sunny Isles Owner Intel Agent",
        f"where={where}; features={len(features)}",
    ])

    out = BytesIO()
    wb.save(out)
    out.seek(0)
    filename = f"Owner_Intel_UPDATED_{target_name.replace(' ', '_')}.xlsx"

    return StreamingResponse(
        out,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
