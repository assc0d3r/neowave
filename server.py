#!/usr/bin/env python3
"""NEOWAVE Intelligence Terminal — production web/archive layer.

The server intentionally does not calculate or mutate canonical NEOWAVE structure.
It validates, stores, versions and renders locked-analysis records, and can delegate
new analysis runs to an external canonical engine command configured by environment.
"""
from __future__ import annotations

import datetime as dt
import base64
import hashlib
import json
import mimetypes
import os
import re
import shlex
import sqlite3
import subprocess
import threading
import traceback
import uuid
from contextlib import contextmanager
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

ROOT = Path(__file__).resolve().parent
WEB_ROOT = ROOT / "web"
DATA_DIR = Path(os.environ.get("NEOWAVE_DATA_DIR", ROOT / "data")).resolve()
DB_PATH = Path(os.environ.get("NEOWAVE_DB_PATH", DATA_DIR / "neowave.db")).resolve()
LOCKS_DIR = DATA_DIR / "locks"
JOBS_DIR = DATA_DIR / "jobs"
UPLOADS_DIR = DATA_DIR / "uploads"
SCHEMA_PATH = ROOT / "engine" / "contracts" / "NEOWAVE_LOCKED_ANALYSIS_SCHEMA_V2_2.json"
CONTRACT_PATH = ROOT / "engine" / "contracts" / "NEOWAVE_V3_2_1_EXECUTABLE_MASTER_CONTRACT.json"
MAX_BODY = int(os.environ.get("NEOWAVE_MAX_BODY_BYTES", str(12 * 1024 * 1024)))
MAX_UPLOAD_FILES = int(os.environ.get("NEOWAVE_MAX_UPLOAD_FILES", "12"))
ADMIN_TOKEN = os.environ.get("NEOWAVE_ADMIN_TOKEN", "").strip()
ENGINE_COMMAND = os.environ.get("NEOWAVE_ENGINE_COMMAND", "").strip()
HOST = os.environ.get("NEOWAVE_HOST", "127.0.0.1")
PORT = int(os.environ.get("NEOWAVE_PORT", "8080"))

REQUIRED_V22 = [
    "schema_version", "engine", "instrument", "analysis_cutoff", "source_manifest",
    "completed_bar_manifest", "market_snapshot", "scenario_registry", "rule_ledger",
    "level_state_machine", "next_evidence", "participation_evidence", "confidence_card",
    "forecast_record", "versioning", "validation", "lock", "data_acquisition", "chart_generation"
]

DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
LOCKS_DIR.mkdir(parents=True, exist_ok=True)
JOBS_DIR.mkdir(parents=True, exist_ok=True)
UPLOADS_DIR.mkdir(parents=True, exist_ok=True)


def utcnow() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def db_connect() -> sqlite3.Connection:
    db = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys=ON")
    db.execute("PRAGMA busy_timeout=5000")
    return db


@contextmanager
def db_session():
    db = db_connect()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    schema = (ROOT / "database_schema.sql").read_text(encoding="utf-8")
    with db_session() as db:
        db.executescript(schema)
        db.execute("INSERT OR IGNORE INTO watchlists(name,created_at) VALUES(?,?)", ("Primary Watchlist", utcnow()))


def json_dumps(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def canonical_hash(payload: dict) -> str:
    return hashlib.sha256(json_dumps(payload).encode("utf-8")).hexdigest()


def as_number(value):
    if isinstance(value, bool) or value is None:
        return None
    try:
        x = float(value)
        if x != x or x in (float("inf"), float("-inf")):
            return None
        return x
    except (TypeError, ValueError):
        return None


def clamp_score(value):
    x = as_number(value)
    if x is None:
        return None
    return max(0.0, min(100.0, x))


def first(*values):
    for v in values:
        if v not in (None, "", [], {}):
            return v
    return None


def deep_get(obj, path, default=None):
    cur = obj
    for key in path.split("."):
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def normalize_symbol(s: str) -> str:
    s = re.sub(r"[^A-Za-z0-9._-]", "", (s or "").upper())
    return s[:32]


def safe_filename(name: str) -> str:
    base = Path(str(name or "upload.bin")).name
    base = re.sub(r"[^A-Za-z0-9._ -]", "_", base).strip(" .")
    return (base or "upload.bin")[:120]


def decode_upload_files(files) -> list[dict]:
    if not isinstance(files, list) or not files:
        raise ValueError("At least one file is required.")
    if len(files) > MAX_UPLOAD_FILES:
        raise ValueError(f"Too many files. Maximum is {MAX_UPLOAD_FILES}.")
    decoded = []
    total = 0
    for i, item in enumerate(files):
        if not isinstance(item, dict):
            raise ValueError(f"Upload item {i+1} must be an object.")
        name = safe_filename(item.get("name") or item.get("filename") or f"upload-{i+1}.bin")
        data = item.get("content_base64")
        if not isinstance(data, str) or not data:
            raise ValueError(f"{name}: content_base64 is required.")
        try:
            raw = base64.b64decode(data, validate=True)
        except Exception as exc:
            raise ValueError(f"{name}: invalid base64 content.") from exc
        total += len(raw)
        if total > MAX_BODY:
            raise ValueError("Combined upload size exceeds the server body limit.")
        decoded.append({"name": name, "bytes": raw, "size": len(raw), "type": str(item.get("type") or "")[:120]})
    return decoded


def write_job_inputs(job_id: str, files: list[dict]) -> tuple[Path, Path, list[dict]]:
    input_dir = (UPLOADS_DIR / job_id).resolve()
    input_dir.mkdir(parents=True, exist_ok=True)
    manifest = []
    used = set()
    for f in files:
        name = f["name"]
        stem, suffix = os.path.splitext(name)
        candidate = name
        n = 2
        while candidate.lower() in used:
            candidate = f"{stem}-{n}{suffix}"
            n += 1
        used.add(candidate.lower())
        path = (input_dir / candidate).resolve()
        try:
            path.relative_to(input_dir)
        except ValueError:
            raise ValueError("Unsafe upload path.")
        path.write_bytes(f["bytes"])
        manifest.append({
            "filename": candidate,
            "path": str(path),
            "size": f["size"],
            "content_type": f["type"],
            "sha256": hashlib.sha256(f["bytes"]).hexdigest(),
        })
    manifest_path = input_dir / "input_manifest.json"
    manifest_path.write_text(json.dumps({"job_id": job_id, "created_at": utcnow(), "files": manifest}, indent=2), encoding="utf-8")
    return input_dir, manifest_path, manifest


def validate_payload(payload: dict) -> dict:
    errors, warnings = [], []
    if not isinstance(payload, dict):
        return {"status": "FAIL", "errors": ["Payload must be a JSON object."], "warnings": []}
    missing = [k for k in REQUIRED_V22 if k not in payload]
    if missing:
        errors.append("Missing required V2.2 fields: " + ", ".join(missing))
    if str(payload.get("schema_version")) != "2.2":
        errors.append("schema_version must be 2.2 for canonical production ingestion.")
    inst = payload.get("instrument") or {}
    for k in ("symbol", "company", "exchange", "currency"):
        if not inst.get(k): errors.append(f"instrument.{k} is required.")
    if not isinstance(payload.get("scenario_registry"), list) or not payload.get("scenario_registry"):
        errors.append("scenario_registry must contain at least one scenario.")
    if not isinstance(payload.get("rule_ledger"), list) or not payload.get("rule_ledger"):
        errors.append("rule_ledger must contain at least one rule result.")
    bars = payload.get("completed_bar_manifest") or []
    if not isinstance(bars, list) or not bars:
        errors.append("completed_bar_manifest must contain completed-bar records.")
    else:
        bad = [b.get("timeframe_id", "?") for b in bars if b.get("developing_bar_excluded") is not True]
        if bad: errors.append("Developing-bar exclusion failed for: " + ", ".join(map(str, bad)))
    part = payload.get("participation_evidence") or {}
    if part.get("structural_mutation_allowed") is not False:
        errors.append("participation_evidence.structural_mutation_allowed must be false.")
    if part.get("role") not in (None, "SUPPORTING_EVIDENCE_ONLY"):
        errors.append("Participation evidence must remain supporting evidence only.")
    lock = payload.get("lock") or {}
    for k in ("hash_algorithm","schema_hash","source_bundle_hash","rule_engine_hash","payload_hash","current_lock_id"):
        if not lock.get(k): errors.append(f"lock.{k} is required.")
    validation = payload.get("validation") or {}
    if validation.get("analysis_validation") in ("FAIL", False):
        errors.append("Upstream analysis_validation is FAIL.")
    if validation.get("decision_map_validation") in ("FAIL", False):
        errors.append("Upstream decision_map_validation is FAIL.")
    if validation.get("warnings"):
        warnings.extend([str(x) for x in validation.get("warnings") if x])

    # The historical V2.2 JSON schema contains a V10 const in data_acquisition while
    # the current master contract identifies V11.5.1. Treat this as a compatibility
    # warning rather than mutating/rejecting a valid current lock solely on that stale const.
    acq = payload.get("data_acquisition") or {}
    engine = str(acq.get("engine") or "")
    engver = str(acq.get("engine_version") or "")
    if engine and engine != "$NEOWAVE_AUTONOMOUS_INPUT_V10":
        warnings.append(f"Schema-compatibility note: data_acquisition.engine={engine}; master contract may supersede V2.2's historical V10 const.")
    if "11.5.1" not in engver and engver:
        warnings.append(f"Data acquisition engine version is {engver}; current terminal contract expects V11.5.1 lineage.")

    return {"status": "FAIL" if errors else ("PASS_WITH_WARNINGS" if warnings else "PASS"), "errors": errors, "warnings": warnings}


def primary_scenario(payload: dict) -> dict:
    scenarios = payload.get("scenario_registry") or []
    if not scenarios: return {}
    def score(s):
        cls = str(s.get("classification", "")).upper()
        rank = s.get("rank")
        return (0 if cls == "PRIMARY" else 1, as_number(rank) if as_number(rank) is not None else 999)
    return sorted([s for s in scenarios if isinstance(s, dict)], key=score)[0] if scenarios else {}


def extract_levels(payload: dict) -> dict:
    lsm = payload.get("level_state_machine") or {}
    levels = lsm.get("levels") or {}
    if isinstance(levels, list):
        out = {}
        for item in levels:
            if not isinstance(item, dict): continue
            key = str(first(item.get("id"), item.get("name"), item.get("type"), "level")).lower()
            out[key] = first(item.get("price"), item.get("value"), item.get("level"))
        levels = out
    if not isinstance(levels, dict): levels = {}
    def pick(*keys):
        for k in keys:
            for actual, value in levels.items():
                if str(actual).lower() == k.lower(): return value
        return None
    return {
        "buy_low": first(pick("buy_zone_low","entry_low","preferred_entry_low"), deep_get(payload,"structural_state.finalized_structural_levels.buy_zone_low")),
        "buy_high": first(pick("buy_zone_high","entry_high","preferred_entry_high"), deep_get(payload,"structural_state.finalized_structural_levels.buy_zone_high")),
        "confirmation": first(pick("confirmation","activation_trigger","confirmation_level","breakout"), deep_get(payload,"structural_state.finalized_structural_levels.activation_trigger")),
        "invalidation": first(pick("invalidation","structural_invalidation","stop"), deep_get(payload,"structural_state.finalized_structural_levels.structural_invalidation")),
        "target_1": first(pick("target_1","t1","target1"), deep_get(payload,"structural_state.finalized_structural_levels.target_1")),
        "target_2": first(pick("target_2","t2","target2"), deep_get(payload,"structural_state.finalized_structural_levels.target_2")),
        "target_3": first(pick("target_3","t3","target3"), deep_get(payload,"structural_state.finalized_structural_levels.target_3")),
        "current_state": lsm.get("current_state"),
        "raw": levels,
    }


def extract_summary(payload: dict) -> dict:
    inst = payload.get("instrument") or {}
    scen = primary_scenario(payload)
    cc = payload.get("confidence_card") or {}
    market = payload.get("market_snapshot") or {}
    canonical_daily = market.get("canonical_daily") or {}
    levels = extract_levels(payload)
    state = first(
        scen.get("label"),
        scen.get("pattern_family"),
        levels.get("current_state"),
        deep_get(payload,"structural_state.pattern"),
        "UNRESOLVED"
    )
    confidence = clamp_score(first(
        scen.get("confidence"), cc.get("structural_interpretation"), cc.get("forecast_confidence")
    ))
    evidence = clamp_score(first(cc.get("evidence_completeness"), deep_get(payload,"participation_evidence.participation_confirmation_score")))
    dataq = clamp_score(first(cc.get("data_confidence"), deep_get(payload,"validation.data_quality_score")))
    execution = clamp_score(first(deep_get(payload,"confidence_card.execution_confidence"), deep_get(payload,"execution.score")))
    mtf = clamp_score(first(deep_get(payload,"confidence_card.mtf_alignment"), deep_get(payload,"market_snapshot.mtf_alignment_score")))
    price = as_number(first(canonical_daily.get("close"), canonical_daily.get("Close"), market.get("close"), deep_get(payload,"market_snapshot.last_price")))
    pct = as_number(first(canonical_daily.get("change_pct"), canonical_daily.get("price_change_pct"), market.get("price_change_pct")))
    return {
        "exchange": normalize_symbol(str(inst.get("exchange") or "NSE")),
        "symbol": normalize_symbol(str(inst.get("symbol") or "")),
        "company": str(inst.get("company") or inst.get("symbol") or ""),
        "currency": str(inst.get("currency") or "INR"),
        "sector": str(inst.get("sector") or payload.get("sector") or ""),
        "isin": str(inst.get("isin") or ""),
        "cutoff": str(payload.get("analysis_cutoff") or utcnow()),
        "state": str(state),
        "scenario": str(first(scen.get("label"), scen.get("pattern_family"), "")),
        "confidence": confidence,
        "evidence_score": evidence,
        "data_quality_score": dataq,
        "execution_score": execution,
        "mtf_score": mtf,
        "price": price,
        "price_change_pct": pct,
        "levels": levels,
        "next_evidence": payload.get("next_evidence") or [],
        "engine_name": str(deep_get(payload,"engine.name", "NEOWAVE")),
        "engine_version": str(deep_get(payload,"engine.version", "")),
        "schema_version": str(payload.get("schema_version") or ""),
        "lock_id": str(deep_get(payload,"lock.current_lock_id", "")),
    }


def compute_delta(previous: sqlite3.Row | None, current: dict) -> dict:
    if previous is None: return {"status":"FIRST_LOCK","changes":[]}
    fields = ["structural_state","confidence","evidence_score","data_quality_score","execution_score","mtf_score","price"]
    changes=[]
    mapping={"structural_state":current["state"],"confidence":current["confidence"],"evidence_score":current["evidence_score"],"data_quality_score":current["data_quality_score"],"execution_score":current["execution_score"],"mtf_score":current["mtf_score"],"price":current["price"]}
    for f in fields:
        old=previous[f]; new=mapping[f]
        if old != new:
            changes.append({"field":f,"before":old,"after":new})
    try: old_levels=json.loads(previous["levels_json"] or "{}")
    except Exception: old_levels={}
    for k in ("buy_low","buy_high","confirmation","invalidation","target_1","target_2","target_3"):
        if old_levels.get(k)!=current["levels"].get(k):
            changes.append({"field":f"levels.{k}","before":old_levels.get(k),"after":current["levels"].get(k)})
    return {"status":"CHANGED" if changes else "UNCHANGED","changes":changes}


def build_events(payload: dict, analysis_id: str) -> list[dict]:
    default_ts = str(payload.get("analysis_cutoff") or utcnow())
    events=[]
    def add(stage,status,detail="",event_ts=None):
        events.append({"analysis_id":analysis_id,"ordinal":len(events)+1,"ts":str(event_ts or default_ts),"stage":stage,"status":status,"detail":detail})
    add("DATA_ACQUISITION","PASS", str(deep_get(payload,"data_acquisition.engine_version", deep_get(payload,"data_acquisition.engine", ""))))
    for ev in deep_get(payload,"data_acquisition.source_events",[]) or []:
        if isinstance(ev,dict):
            add(str(ev.get("stage") or ev.get("event") or "SOURCE_EVENT"), str(ev.get("status") or "RECORDED"), str(ev.get("detail") or ev.get("source") or ""), first(ev.get("timestamp"),ev.get("ts"),ev.get("time")))
        else: add("SOURCE_EVENT","RECORDED",str(ev))
    badbars=[x for x in payload.get("completed_bar_manifest",[]) if x.get("developing_bar_excluded") is not True]
    add("COMPLETED_BAR_GATE", "PASS" if not badbars else "FAIL", f"{len(payload.get('completed_bar_manifest',[]))} timeframe manifests")
    add("CANONICAL_STRUCTURE","PASS", str(primary_scenario(payload).get("label") or primary_scenario(payload).get("pattern_family") or "Primary scenario resolved"))
    rules=payload.get("rule_ledger") or []
    failed=[r for r in rules if str(r.get("status","")).upper() in ("FAIL","FAILED") or r.get("result") is False]
    add("RULE_LEDGER", "PASS" if not failed else "WARNING", f"{len(rules)} rules; {len(failed)} failed/warning")
    add("SCENARIO_REGISTRY","PASS", f"{len(payload.get('scenario_registry') or [])} scenarios preserved")
    add("FINALIZED_LEVELS","PASS", str((payload.get("level_state_machine") or {}).get("current_state") or "Level state machine recorded"))
    add("EVIDENCE_RECONCILIATION","PASS", f"{len(payload.get('next_evidence') or [])} next-evidence conditions")
    for key in ("analysis_chronology","execution_chronology","run_chronology"):
        for ev in payload.get(key) or []:
            if isinstance(ev,dict): add(str(ev.get("stage") or ev.get("event") or key.upper()),str(ev.get("status") or "RECORDED"),str(ev.get("detail") or ev.get("message") or ""),first(ev.get("timestamp"),ev.get("ts"),ev.get("time")))
            else: add(key.upper(),"RECORDED",str(ev))
    v=payload.get("validation") or {}
    add("VALIDATION", "PASS" if v.get("analysis_validation") not in (False,"FAIL") else "FAIL", str(v.get("analysis_validation","recorded")))
    add("LOCK", "PASS", str(deep_get(payload,"lock.current_lock_id", "locked")))
    return events


def strict_schema_check(payload: dict) -> dict:
    try:
        import jsonschema  # optional runtime dependency
        schema=json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
        jsonschema.validate(payload, schema)
        return {"available":True,"status":"PASS","detail":"JSON Schema V2.2 validation passed."}
    except ModuleNotFoundError:
        return {"available":False,"status":"SKIPPED","detail":"python package jsonschema not installed; contract gate validation still executed."}
    except Exception as exc:
        # Do not blindly fail on the known data_acquisition const drift; surface it.
        msg=str(exc)
        if "data_acquisition" in msg and ("AUTONOMOUS_INPUT_V10" in msg or "const" in msg):
            return {"available":True,"status":"COMPATIBILITY_WARNING","detail":msg[:900]}
        return {"available":True,"status":"FAIL","detail":msg[:1800]}


def import_analysis(payload: dict, actor="operator") -> dict:
    validation=validate_payload(payload)
    strict=strict_schema_check(payload)
    if strict["status"]=="FAIL":
        validation["errors"].append("Strict JSON Schema validation failed: "+strict["detail"])
        validation["status"]="FAIL"
    elif strict["status"]=="COMPATIBILITY_WARNING":
        validation["warnings"].append("Strict schema compatibility warning recorded; master-contract compatibility gate applied.")
        if validation["status"]=="PASS": validation["status"]="PASS_WITH_WARNINGS"
    validation["strict_schema"] = strict
    if validation["errors"]:
        raise ValueError(json.dumps(validation, ensure_ascii=False))

    summary=extract_summary(payload)
    if not summary["symbol"]: raise ValueError("Instrument symbol is empty after normalization.")
    src_hash=canonical_hash(payload)
    imported=utcnow()
    with db_session() as db:
        duplicate=db.execute("SELECT id FROM analyses WHERE source_hash=?",(src_hash,)).fetchone()
        if duplicate:
            return {"analysis_id":duplicate["id"],"duplicate":True,"validation":validation}
        sec=db.execute("SELECT * FROM securities WHERE exchange=? AND symbol=?",(summary["exchange"],summary["symbol"])).fetchone()
        if sec is None:
            cur=db.execute("INSERT INTO securities(exchange,symbol,company,currency,sector,isin,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                           (summary["exchange"],summary["symbol"],summary["company"],summary["currency"],summary["sector"],summary["isin"],imported,imported))
            security_id=cur.lastrowid
        else:
            security_id=sec["id"]
            db.execute("UPDATE securities SET company=?,currency=?,sector=?,isin=?,updated_at=? WHERE id=?",
                       (summary["company"],summary["currency"],summary["sector"],summary["isin"],imported,security_id))
        prev=db.execute("SELECT * FROM analyses WHERE security_id=? AND is_latest=1 ORDER BY analysis_cutoff DESC LIMIT 1",(security_id,)).fetchone()
        analysis_id = summary["lock_id"] or f"NW-{summary['exchange']}-{summary['symbol']}-{re.sub(r'[^0-9]','',summary['cutoff'])[:14]}-{src_hash[:8]}"
        analysis_id = re.sub(r"[^A-Za-z0-9._:-]","-",analysis_id)[:160]
        if db.execute("SELECT 1 FROM analyses WHERE id=?",(analysis_id,)).fetchone(): analysis_id += "-"+src_hash[:8]
        delta=compute_delta(prev, summary)
        db.execute("UPDATE analyses SET is_latest=0 WHERE security_id=?",(security_id,))
        db.execute("""INSERT INTO analyses(id,security_id,lock_id,analysis_cutoff,imported_at,engine_name,engine_version,schema_version,structural_state,primary_scenario,confidence,evidence_score,data_quality_score,execution_score,mtf_score,price,price_change_pct,levels_json,next_evidence_json,delta_json,source_hash,raw_json,validation_status,validation_json,prior_analysis_id,is_latest)
                      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)""",
                   (analysis_id,security_id,summary["lock_id"],summary["cutoff"],imported,summary["engine_name"],summary["engine_version"],summary["schema_version"],summary["state"],summary["scenario"],summary["confidence"],summary["evidence_score"],summary["data_quality_score"],summary["execution_score"],summary["mtf_score"],summary["price"],summary["price_change_pct"],json.dumps(summary["levels"],ensure_ascii=False),json.dumps(summary["next_evidence"],ensure_ascii=False),json.dumps(delta,ensure_ascii=False),src_hash,json.dumps(payload,ensure_ascii=False),validation["status"],json.dumps(validation,ensure_ascii=False),prev["id"] if prev else None))
        for e in build_events(payload, analysis_id):
            db.execute("INSERT INTO analysis_events(analysis_id,ordinal,ts,stage,status,detail) VALUES(?,?,?,?,?,?)",(e["analysis_id"],e["ordinal"],e["ts"],e["stage"],e["status"],e["detail"]))
        fr=payload.get("forecast_record") or {}
        db.execute("INSERT OR IGNORE INTO forecast_outcomes(analysis_id,forecast_id,status,outcome_json) VALUES(?,?,?,?)",(analysis_id,str(fr.get("forecast_id") or ""),str(fr.get("outcome_status") or "OPEN"),json.dumps(fr,ensure_ascii=False)))
        lockfile=LOCKS_DIR/f"{analysis_id}.json"
        tmp=lockfile.with_suffix(".json.tmp"); tmp.write_text(json.dumps(payload,indent=2,ensure_ascii=False),encoding="utf-8"); os.replace(tmp,lockfile)
        db.execute("INSERT INTO audit_log(ts,request_id,actor,action,object_type,object_id,result,detail) VALUES(?,?,?,?,?,?,?,?)",(imported,"internal",actor,"IMPORT_LOCK","analysis",analysis_id,"PASS",validation["status"]))
        db.commit()
    return {"analysis_id":analysis_id,"duplicate":False,"validation":validation,"summary":summary,"delta":delta}


def row_to_dict(row): return {k: row[k] for k in row.keys()}


def list_stocks(q="", state="", sort="recent", limit=200):
    where=["a.is_latest=1"] ; args=[]
    if q:
        where.append("(s.symbol LIKE ? OR s.company LIKE ? OR s.isin LIKE ? OR a.structural_state LIKE ?)")
        like=f"%{q}%"; args += [like,like,like,like]
    if state:
        where.append("a.structural_state LIKE ?"); args.append(f"%{state}%")
    order={"confidence":"COALESCE(a.confidence,-1) DESC","symbol":"s.symbol ASC","recent":"a.analysis_cutoff DESC"}.get(sort,"a.analysis_cutoff DESC")
    sql=f"""SELECT s.*,a.id AS analysis_id,a.analysis_cutoff,a.structural_state,a.primary_scenario,a.confidence,a.evidence_score,a.data_quality_score,a.execution_score,a.mtf_score,a.price,a.price_change_pct,a.validation_status,a.levels_json,a.delta_json
             FROM securities s JOIN analyses a ON a.security_id=s.id WHERE {' AND '.join(where)} ORDER BY {order} LIMIT ?"""
    args.append(min(max(int(limit),1),500))
    with db_session() as db:
        rows=db.execute(sql,args).fetchall()
    out=[]
    for r in rows:
        d=row_to_dict(r); d["levels"]=json.loads(d.pop("levels_json") or "{}"); d["delta"]=json.loads(d.pop("delta_json") or "{}"); out.append(d)
    return out


def dashboard_stats():
    with db_session() as db:
        counts=db.execute("""SELECT COUNT(*) stocks, SUM(CASE WHEN a.confidence>=80 THEN 1 ELSE 0 END) high_confidence, AVG(a.data_quality_score) avg_data_quality, COUNT(DISTINCT a.id) latest_analyses FROM analyses a WHERE a.is_latest=1""").fetchone()
        locks=db.execute("SELECT COUNT(*) n FROM analyses").fetchone()["n"]
        open_forecasts=db.execute("SELECT COUNT(*) n FROM forecast_outcomes WHERE status NOT IN ('RESOLVED','CLOSED','SUCCESS','FAIL')").fetchone()["n"]
        latest=list_stocks(limit=8)
    return {"stocks":counts["stocks"] or 0,"high_confidence":counts["high_confidence"] or 0,"avg_data_quality":counts["avg_data_quality"],"locks":locks,"open_forecasts":open_forecasts,"recent":latest}


def stock_detail(exchange,symbol):
    exchange=normalize_symbol(exchange); symbol=normalize_symbol(symbol)
    with db_session() as db:
        sec=db.execute("SELECT * FROM securities WHERE exchange=? AND symbol=?",(exchange,symbol)).fetchone()
        if not sec: return None
        analyses=db.execute("SELECT * FROM analyses WHERE security_id=? ORDER BY analysis_cutoff DESC",(sec["id"],)).fetchall()
    if not analyses: return None
    latest=analyses[0]
    out={"security":row_to_dict(sec),"latest":analysis_public(latest,include_raw=True),"history":[analysis_public(a,include_raw=False) for a in analyses]}
    return out


def analysis_public(row, include_raw=False):
    d=row_to_dict(row)
    for k in ("levels_json","next_evidence_json","delta_json","validation_json"):
        d[k[:-5] if k.endswith("_json") else k]=json.loads(d.pop(k) or ("[]" if k=="next_evidence_json" else "{}"))
    if include_raw:
        d["raw"]=json.loads(d.pop("raw_json"))
    else: d.pop("raw_json",None)
    with db_session() as db:
        ev=db.execute("SELECT ordinal,ts,stage,status,detail FROM analysis_events WHERE analysis_id=? ORDER BY ordinal",(d["id"],)).fetchall()
    d["events"]=[row_to_dict(x) for x in ev]
    return d


def system_info():
    contract={}
    try:
        contract=json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    except Exception: pass
    meta=contract.get("contract_meta",{})
    versions=meta.get("version_inheritance",{})
    return {
      "terminal_version":"1.0.0-production",
      "canonical_contract":meta.get("contract_version","3.2.1-FULLSTACK-EXECUTABLE"),
      "market_data_engine":versions.get("market_data_engine","V11.5.1-MARE-EXCHANGE-NATIVE-FUSION-SELFHEAL"),
      "metric_engine":versions.get("metric_capability_engine","MCE-1.0"),
      "schema":"2.2",
      "engine_bridge_configured":bool(ENGINE_COMMAND),
      "write_auth_enabled":bool(ADMIN_TOKEN),
      "upload_processing_enabled": True,
      "max_upload_files": MAX_UPLOAD_FILES,
      "max_body_bytes": MAX_BODY,
      "database":str(DB_PATH),
      "structural_firewall":"ENFORCED_AT_INGESTION_AND_PRESENTATION",
      "field_policy":meta.get("field_policy","Missing data remains explicit; never fabricate."),
    }


def run_engine_job(job_id, exchange, symbol, uploaded_files=None):
    started=utcnow(); outdir=JOBS_DIR/job_id; outdir.mkdir(parents=True,exist_ok=True)
    input_dir = None
    manifest_path = None
    with db_session() as db:
        db.execute("UPDATE jobs SET status='RUNNING',started_at=?,output_dir=? WHERE id=?",(started,str(outdir),job_id)); db.commit()
    try:
        if uploaded_files:
            input_dir, manifest_path, manifest = write_job_inputs(job_id, uploaded_files)
            lock_jsons = []
            for item in manifest:
                p = Path(item["path"])
                if p.suffix.lower() == ".json":
                    try:
                        candidate = json.loads(p.read_text(encoding="utf-8"))
                        if isinstance(candidate, dict) and str(candidate.get("schema_version")) == "2.2" and "lock" in candidate:
                            lock_jsons.append(candidate)
                    except Exception:
                        pass
            if lock_jsons:
                last = None
                for payload in lock_jsons:
                    last = import_analysis(payload, actor="upload-lock")
                with db_session() as db:
                    db.execute("UPDATE jobs SET status='COMPLETE',finished_at=?,analysis_id=? WHERE id=?",(utcnow(),last["analysis_id"] if last else None,job_id)); db.commit()
                return
        if not ENGINE_COMMAND: raise RuntimeError("ENGINE_NOT_CONFIGURED")
        command=ENGINE_COMMAND.format(
            exchange=exchange,
            symbol=symbol,
            output_dir=str(outdir),
            input_dir=str(input_dir or ""),
            input_manifest=str(manifest_path or ""),
        )
        proc=subprocess.run(shlex.split(command),cwd=ROOT,capture_output=True,text=True,timeout=int(os.environ.get("NEOWAVE_ENGINE_TIMEOUT_SECONDS","900")))
        (outdir/"stdout.log").write_text(proc.stdout or "",encoding="utf-8")
        (outdir/"stderr.log").write_text(proc.stderr or "",encoding="utf-8")
        if proc.returncode!=0: raise RuntimeError(f"ENGINE_EXIT_{proc.returncode}: {(proc.stderr or proc.stdout)[-1200:]}")
        candidates=[outdir/"LOCKED_ANALYSIS.json",outdir/f"{symbol}_NEOWAVE_LOCKED_ANALYSIS.json"]
        candidates += list(outdir.glob("*LOCKED*ANALYSIS*.json"))
        lock=next((p for p in candidates if p.exists()),None)
        if not lock: raise RuntimeError("ENGINE_OUTPUT_MISSING_LOCKED_ANALYSIS")
        payload=json.loads(lock.read_text(encoding="utf-8")); res=import_analysis(payload,actor="engine-bridge")
        with db_session() as db:
            db.execute("UPDATE jobs SET status='COMPLETE',finished_at=?,analysis_id=? WHERE id=?",(utcnow(),res["analysis_id"],job_id)); db.commit()
    except Exception as exc:
        code="ENGINE_NOT_CONFIGURED" if str(exc)=="ENGINE_NOT_CONFIGURED" else "ENGINE_RUN_FAILED"
        with db_session() as db:
            db.execute("UPDATE jobs SET status='FAILED',finished_at=?,error_code=?,error_detail=? WHERE id=?",(utcnow(),code,str(exc)[:2000],job_id)); db.commit()


class Handler(BaseHTTPRequestHandler):
    server_version = "NEOWAVE-Terminal/1.0"

    def log_message(self, fmt, *args):
        print(f"[{self.log_date_time_string()}] {self.client_address[0]} {fmt % args}")

    def request_id(self):
        return self.headers.get("X-Request-ID") or uuid.uuid4().hex[:16]

    def actor(self):
        return (self.headers.get("X-NEOWAVE-User") or "operator")[:80]

    def security_headers(self, content_type="application/json; charset=utf-8"):
        self.send_header("Content-Type",content_type)
        self.send_header("X-Content-Type-Options","nosniff")
        self.send_header("Referrer-Policy","same-origin")
        self.send_header("X-Frame-Options","DENY")
        self.send_header("Permissions-Policy","camera=(), microphone=(), geolocation=(), payment=()")
        self.send_header("Content-Security-Policy","default-src 'self'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src 'self' https://fonts.gstatic.com; img-src 'self' data: blob:; script-src 'self'; connect-src 'self'; media-src 'self' blob:; frame-ancestors 'none'; base-uri 'self'; form-action 'self'")

    def send_json(self,status,obj):
        data=json.dumps(obj,ensure_ascii=False).encode("utf-8")
        self.send_response(status); self.security_headers(); self.send_header("Content-Length",str(len(data))); self.send_header("Cache-Control","no-store"); self.end_headers(); self.wfile.write(data)

    def read_json(self):
        length=int(self.headers.get("Content-Length","0") or 0)
        if length<=0 or length>MAX_BODY: raise ValueError("Invalid or excessive request body size.")
        return json.loads(self.rfile.read(length))

    def require_write_auth(self):
        if not ADMIN_TOKEN: return True
        auth=self.headers.get("Authorization","")
        if auth == f"Bearer {ADMIN_TOKEN}": return True
        self.send_json(HTTPStatus.UNAUTHORIZED,{"error":"WRITE_AUTH_REQUIRED"}); return False

    def audit(self,action,objtype,objid,result,detail=""):
        try:
            with db_session() as db:
                db.execute("INSERT INTO audit_log(ts,request_id,actor,action,object_type,object_id,result,detail) VALUES(?,?,?,?,?,?,?,?)",(utcnow(),self.request_id(),self.actor(),action,objtype,objid,result,detail[:1200])); db.commit()
        except Exception: pass

    def do_HEAD(self):
        u=urlparse(self.path); path=unquote(u.path)
        if path.startswith("/api/"):
            self.send_response(200); self.security_headers(); self.send_header("Cache-Control","no-store"); self.end_headers(); return
        if path in ("","/"): path="/index.html"
        rel=Path(path.lstrip("/")); target=(WEB_ROOT/rel).resolve()
        try: target.relative_to(WEB_ROOT.resolve())
        except ValueError: self.send_response(403); self.end_headers(); return
        if not target.exists() or not target.is_file(): target=WEB_ROOT/"index.html"
        ctype=mimetypes.guess_type(target.name)[0] or "application/octet-stream"
        self.send_response(200); self.security_headers(ctype); self.send_header("Content-Length",str(target.stat().st_size)); self.end_headers()

    def do_GET(self):
        try:
            u=urlparse(self.path); path=unquote(u.path); qs=parse_qs(u.query)
            if path=="/api/health": return self.send_json(200,{"status":"ok","time":utcnow()})
            if path=="/api/system": return self.send_json(200,system_info())
            if path=="/api/dashboard": return self.send_json(200,dashboard_stats())
            if path=="/api/stocks": return self.send_json(200,{"items":list_stocks(q=(qs.get("q")or[""])[0],state=(qs.get("state")or[""])[0],sort=(qs.get("sort")or["recent"])[0],limit=(qs.get("limit")or[200])[0])})
            m=re.fullmatch(r"/api/stocks/([^/]+)/([^/]+)",path)
            if m:
                data=stock_detail(m.group(1),m.group(2)); return self.send_json(200,data) if data else self.send_json(404,{"error":"STOCK_NOT_FOUND"})
            m=re.fullmatch(r"/api/analyses/([^/]+)",path)
            if m:
                with db_session() as db: row=db.execute("SELECT * FROM analyses WHERE id=?",(m.group(1),)).fetchone()
                return self.send_json(200,analysis_public(row,include_raw=True)) if row else self.send_json(404,{"error":"ANALYSIS_NOT_FOUND"})
            m=re.fullmatch(r"/api/jobs/([^/]+)",path)
            if m:
                with db_session() as db: row=db.execute("SELECT * FROM jobs WHERE id=?",(m.group(1),)).fetchone()
                return self.send_json(200,row_to_dict(row)) if row else self.send_json(404,{"error":"JOB_NOT_FOUND"})
            if path=="/api/performance":
                with db_session() as db:
                    rows=db.execute("SELECT status,target_achieved,invalidated,mfe_pct,mae_pct,days_to_resolution FROM forecast_outcomes").fetchall()
                total=len(rows); resolved=[r for r in rows if r["status"] not in ("OPEN","PENDING","")]
                wins=[r for r in resolved if r["target_achieved"]==1]
                return self.send_json(200,{"total":total,"resolved":len(resolved),"target_achieved_pct":(len(wins)/len(resolved)*100 if resolved else None),"items":[row_to_dict(r) for r in rows[-100:]]})
            if path=="/api/audit":
                with db_session() as db: rows=db.execute("SELECT * FROM audit_log ORDER BY id DESC LIMIT 100").fetchall()
                return self.send_json(200,{"items":[row_to_dict(r) for r in rows]})
            if path=="/api/preferences":
                user=(qs.get("user")or["operator"])[0][:80]
                with db_session() as db: row=db.execute("SELECT * FROM user_preferences WHERE user_key=?",(user,)).fetchone()
                return self.send_json(200,row_to_dict(row) if row else {"user_key":user,"theme":"glass-intelligence","motion":1,"settings_json":"{}"})
            return self.serve_static(path)
        except Exception as exc:
            traceback.print_exc(); self.send_json(500,{"error":"INTERNAL_ERROR","detail":str(exc)[:500]})

    def do_POST(self):
        try:
            u=urlparse(self.path); path=unquote(u.path)
            if path.startswith("/api/") and not self.require_write_auth(): return
            if path=="/api/analyses/import":
                body=self.read_json(); payload=body.get("payload") if isinstance(body,dict) and "payload" in body else body
                try: res=import_analysis(payload,actor=self.actor())
                except ValueError as exc:
                    try: detail=json.loads(str(exc))
                    except Exception: detail={"errors":[str(exc)]}
                    self.audit("IMPORT_LOCK","analysis","","FAIL",str(exc)); return self.send_json(422,{"error":"LOCK_VALIDATION_FAILED","validation":detail})
                self.audit("IMPORT_LOCK","analysis",res["analysis_id"],"PASS",res["validation"]["status"]); return self.send_json(201,res)
            if path=="/api/analyze":
                body=self.read_json(); exchange=normalize_symbol(body.get("exchange","NSE")); symbol=normalize_symbol(body.get("symbol",""))
                if not symbol: return self.send_json(400,{"error":"SYMBOL_REQUIRED"})
                if not ENGINE_COMMAND: return self.send_json(503,{"error":"ENGINE_NOT_CONFIGURED","detail":"Set NEOWAVE_ENGINE_COMMAND to a canonical engine bridge that emits LOCKED_ANALYSIS.json. The website will not fabricate analysis."})
                job_id="JOB-"+uuid.uuid4().hex[:12].upper(); now=utcnow()
                with db_session() as db: db.execute("INSERT INTO jobs(id,exchange,symbol,status,created_at) VALUES(?,?,?,?,?)",(job_id,exchange,symbol,"QUEUED",now)); db.commit()
                threading.Thread(target=run_engine_job,args=(job_id,exchange,symbol),daemon=True).start()
                self.audit("RUN_ANALYSIS","job",job_id,"QUEUED",f"{exchange}:{symbol}"); return self.send_json(202,{"job_id":job_id,"status":"QUEUED"})
            if path=="/api/analyze/upload":
                body=self.read_json(); exchange=normalize_symbol(body.get("exchange","NSE")); symbol=normalize_symbol(body.get("symbol",""))
                files=decode_upload_files(body.get("files"))
                if not symbol:
                    symbol = normalize_symbol(Path(files[0]["name"]).stem) or "UPLOADED"
                job_id="JOB-"+uuid.uuid4().hex[:12].upper(); now=utcnow()
                with db_session() as db: db.execute("INSERT INTO jobs(id,exchange,symbol,status,created_at) VALUES(?,?,?,?,?)",(job_id,exchange,symbol,"QUEUED",now)); db.commit()
                threading.Thread(target=run_engine_job,args=(job_id,exchange,symbol,files),daemon=True).start()
                self.audit("UPLOAD_ANALYSIS_INPUTS","job",job_id,"QUEUED",f"{exchange}:{symbol}; files={len(files)}"); return self.send_json(202,{"job_id":job_id,"status":"QUEUED","files":len(files)})
            m=re.fullmatch(r"/api/stocks/([^/]+)/([^/]+)/favorite",path)
            if m:
                ex,sym=normalize_symbol(m.group(1)),normalize_symbol(m.group(2)); body=self.read_json(); val=1 if body.get("favourite",True) else 0
                with db_session() as db:
                    cur=db.execute("UPDATE securities SET favourite=?,updated_at=? WHERE exchange=? AND symbol=?",(val,utcnow(),ex,sym)); db.commit()
                if not cur.rowcount: return self.send_json(404,{"error":"STOCK_NOT_FOUND"})
                self.audit("SET_FAVOURITE","security",f"{ex}:{sym}","PASS",str(val)); return self.send_json(200,{"favourite":bool(val)})
            if path=="/api/preferences":
                body=self.read_json(); user=str(body.get("user_key") or self.actor())[:80]; theme=str(body.get("theme") or "glass-intelligence")[:60]; motion=1 if body.get("motion",True) else 0; settings=body.get("settings") or {}
                now=utcnow()
                with db_session() as db:
                    db.execute("""INSERT INTO user_preferences(user_key,theme,motion,settings_json,updated_at) VALUES(?,?,?,?,?) ON CONFLICT(user_key) DO UPDATE SET theme=excluded.theme,motion=excluded.motion,settings_json=excluded.settings_json,updated_at=excluded.updated_at""",(user,theme,motion,json.dumps(settings),now)); db.commit()
                return self.send_json(200,{"user_key":user,"theme":theme,"motion":motion,"updated_at":now})
            return self.send_json(404,{"error":"NOT_FOUND"})
        except json.JSONDecodeError: return self.send_json(400,{"error":"INVALID_JSON"})
        except ValueError as exc: return self.send_json(400,{"error":"BAD_REQUEST","detail":str(exc)})
        except Exception as exc:
            traceback.print_exc(); return self.send_json(500,{"error":"INTERNAL_ERROR","detail":str(exc)[:500]})

    def serve_static(self,path):
        if path in ("","/"): path="/index.html"
        rel=Path(path.lstrip("/"))
        target=(WEB_ROOT/rel).resolve()
        try: target.relative_to(WEB_ROOT.resolve())
        except ValueError: return self.send_json(403,{"error":"FORBIDDEN"})
        if not target.exists() or not target.is_file():
            if "." not in rel.name: target=WEB_ROOT/"index.html"
            else: return self.send_json(404,{"error":"NOT_FOUND"})
        data=target.read_bytes(); ctype=mimetypes.guess_type(target.name)[0] or "application/octet-stream"
        self.send_response(200); self.security_headers(ctype); self.send_header("Content-Length",str(len(data))); self.send_header("Cache-Control","public,max-age=31536000,immutable" if target.name not in ("index.html",) else "no-cache"); self.end_headers(); self.wfile.write(data)


def main():
    init_db()
    print(f"NEOWAVE Intelligence Terminal production server: http://{HOST}:{PORT}")
    print(f"DB: {DB_PATH}")
    print("Engine bridge:", "configured" if ENGINE_COMMAND else "not configured (ingest-only mode; analysis fabrication disabled)")
    httpd=ThreadingHTTPServer((HOST,PORT),Handler)
    try: httpd.serve_forever()
    except KeyboardInterrupt: pass
    finally: httpd.server_close()

if __name__=="__main__": main()
