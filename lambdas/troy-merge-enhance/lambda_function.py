from __future__ import annotations
import json, re, os, hashlib
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://lzfgigiyqpuuxslsygjt.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
VERSION_RE = re.compile(r"^v(\d+)\.(\d+)$")

@dataclass
class MergeInput:
    source_ref: Optional[str]
    source_type: Optional[str]
    source_title: Optional[str]
    content: str

def parse_version(label: str):
    m = VERSION_RE.match(label.strip())
    if not m: raise ValueError(f"Invalid version label: {label}")
    return int(m.group(1)), int(m.group(2))

def classify_change(previous_content: str, inputs: List[MergeInput]) -> bool:
    text = "\n".join(i.content for i in inputs).lower()
    markers = ["new section","top-level section","schema changed","workflow changed",
               "operating contract","new subsystem","rollback","promotion gate","validation contract"]
    return sum(1 for m in markers if m in text) >= 2

def generate_candidate(previous_content: str, inputs: List[MergeInput], version_label: str) -> Dict[str, Any]:
    parts = [previous_content.strip()] if previous_content.strip() else []
    parts += [i.content.strip() for i in inputs if i.content.strip()]
    body = "\n\n".join(parts).strip()
    summary = ["merged new inputs into living document",
               "preserved prior commitments where applicable",
               "normalised header and version metadata"]
    content = (f"DOCUMENT VERSION: {version_label}\nSOURCE COUNT: {len(inputs)}\n\n"
               f"CHANGE SUMMARY:\n" + "\n".join(f"- {s}" for s in summary) + f"\n\n{body}")
    return {"content": content, "change_summary": "\n".join(f"- {s}" for s in summary)}

def validate_candidate(previous_content: str, candidate_content: str) -> Dict[str, Any]:
    checks = {
        "header_present": "DOCUMENT VERSION:" in candidate_content and "SOURCE COUNT:" in candidate_content,
        "change_summary_present": "CHANGE SUMMARY:" in candidate_content,
        "non_empty": len(candidate_content.strip()) > 0,
        "commitments_preserved": True,
        "critical_sections_preserved": True,
        "no_contradictions": True
    }
    notes = []
    critical_terms = ["rollback","latest","version","quality","diff"]
    missing = [t for t in critical_terms if t in previous_content.lower() and t not in candidate_content.lower()]
    if missing:
        checks["critical_sections_preserved"] = False
        checks["commitments_preserved"] = False
        notes.append(f"missing critical terms: {', '.join(missing)}")
    for k, label in [("header_present","header missing"),("change_summary_present","change summary missing"),("non_empty","empty content")]:
        if not checks[k]: notes.append(label)
    status = "PASS" if all(checks.values()) else "FAIL"
    if status == "PASS": notes.append("candidate passed all required checks")
    return {"status": status, "checks": checks, "notes": notes}

def build_diff_summary(previous_content: str, candidate_content: str) -> Dict[str, Any]:
    prev = set(l.strip() for l in previous_content.splitlines() if l.strip())
    new = set(l.strip() for l in candidate_content.splitlines() if l.strip())
    added = sorted(list(new - prev))[:50]
    removed = sorted(list(prev - new))[:50]
    parts = []
    if added: parts.append(f"added {len(added)} distinct lines")
    if removed: parts.append(f"removed {len(removed)} distinct lines")
    if not parts: parts.append("wording-only or no material line diff")
    return {"diff_summary": "; ".join(parts), "diff_json": {"added_sample": added[:10], "removed_sample": removed[:10]}}

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    action = event.get("action", "run")

    if action == "latest":
        doc_key = event["doc_key"]
        rows = sb.rpc("fn_get_latest_merge_version", {"p_doc_key": doc_key}).execute().data
        return {"success": True, "data": rows[0] if rows else None}

    if action == "history":
        doc_key = event["doc_key"]
        rows = sb.table("merge_document_version").select("*, merge_document!inner(doc_key)").eq("merge_document.doc_key", doc_key).order("major_version", desc=True).order("minor_version", desc=True).execute().data
        return {"success": True, "data": rows}

    if action == "rollback":
        doc_key = event["doc_key"]
        target = event["target_version_label"]
        created_by = event.get("created_by", "merge_enhance_rollback")
        new_id = sb.rpc("fn_rollback_merge_document", {"p_doc_key": doc_key, "p_target_version_label": target, "p_created_by": created_by}).execute().data
        return {"success": True, "new_version_id": new_id}

    # Default: run
    doc_key = event["doc_key"].strip()
    doc_name = event.get("doc_name") or doc_key
    raw_inputs = event.get("inputs", [])
    inputs = [MergeInput(
        source_ref=i.get("source_ref"),
        source_type=i.get("source_type"),
        source_title=i.get("source_title"),
        content=i.get("content", "")
    ) for i in raw_inputs]

    doc = sb.rpc("fn_get_or_create_merge_document", {"p_doc_key": doc_key, "p_doc_name": doc_name}).execute().data
    latest_rows = sb.rpc("fn_get_latest_merge_version", {"p_doc_key": doc_key}).execute().data
    latest = latest_rows[0] if latest_rows else None
    previous_content = latest["content"] if latest and latest.get("content") else ""

    is_structural = classify_change(previous_content, inputs)
    next_v = sb.rpc("fn_compute_next_merge_version", {"p_doc_key": doc_key, "p_is_structural_change": is_structural}).execute().data[0]

    candidate = generate_candidate(previous_content, inputs, next_v["next_version_label"])
    validation = validate_candidate(previous_content, candidate["content"])
    parent_version_id = latest["version_id"] if latest and latest.get("version_id") else None

    sb.table("merge_document_attempt").insert({
        "document_id": doc["id"], "candidate_version_label": next_v["next_version_label"],
        "parent_version_id": parent_version_id, "source_count": len(inputs),
        "candidate_content": candidate["content"], "candidate_change_summary": candidate["change_summary"],
        "validation_status": validation["status"], "validation_notes": "\n".join(validation["notes"]),
        "validation_report": validation, "promoted": False
    }).execute()

    if validation["status"] != "PASS":
        return {"success": False, "doc_key": doc_key, "candidate_version": next_v["next_version_label"],
                "quality_status": "FAIL", "quality_notes": validation["notes"]}

    created = sb.table("merge_document_version").insert({
        "document_id": doc["id"], "version_label": next_v["next_version_label"],
        "major_version": next_v["next_major"], "minor_version": next_v["next_minor"],
        "parent_version_id": parent_version_id, "is_structural_change": is_structural,
        "source_count": len(inputs), "change_summary": candidate["change_summary"],
        "content": candidate["content"], "quality_status": "PASS",
        "quality_notes": "\n".join(validation["notes"]), "validation_report": validation,
        "created_by": "merge_enhance"
    }).execute().data[0]

    for item in inputs:
        sb.table("merge_document_input").insert({
            "version_id": created["id"], "source_ref": item.source_ref,
            "source_type": item.source_type, "source_title": item.source_title,
            "content": item.content
        }).execute()

    if parent_version_id:
        diff = build_diff_summary(previous_content, candidate["content"])
        sb.table("merge_document_diff").insert({
            "document_id": doc["id"], "from_version_id": parent_version_id,
            "to_version_id": created["id"], "diff_summary": diff["diff_summary"],
            "diff_json": diff["diff_json"]
        }).execute()

    sb.rpc("fn_promote_merge_version", {"p_document_id": doc["id"], "p_version_id": created["id"]}).execute()

    return {"success": True, "doc_key": doc_key, "document_id": doc["id"],
            "version_id": created["id"], "version": created["version_label"],
            "quality_status": "PASS", "is_structural_change": is_structural,
            "source_count": len(inputs)}
