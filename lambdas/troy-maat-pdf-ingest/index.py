"""
troy-maat-pdf-ingest
Reads CBA bank statement PDFs from Supabase Storage 'statements' bucket,
extracts transactions, inserts into maat_transactions.

Invoke payload:
{
  "action": "ingest_source",        // ingest a specific source_id
  "source_id": "<uuid>",
  --- OR ---
  "action": "ingest_pending",       // ingest all REGISTERED sources with 0 loaded txns
  "bank": "CBA",                    // optional filter
  --- OR ---
  "action": "upload_and_ingest",    // base64 PDF + register + ingest
  "pdf_base64": "<base64>",
  "filename": "CBA_HomeLoan_8563_Stmt21.pdf",
  "source_id": "<existing source_id to link>"
}
"""

import json, os, re, io, uuid, urllib.request, urllib.error, base64
from datetime import datetime, date

SUPA_URL = os.environ.get("SUPABASE_URL", "https://lzfgigiyqpuuxslsygjt.supabase.co")
SUPA_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
BUCKET   = "statements"


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def supa_get(path):
    req = urllib.request.Request(
        f"{SUPA_URL}{path}",
        headers={"apikey": SUPA_KEY, "Authorization": f"Bearer {SUPA_KEY}"}
    )
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())

def supa_post(path, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"{SUPA_URL}{path}", data=data, method="POST",
        headers={
            "apikey": SUPA_KEY, "Authorization": f"Bearer {SUPA_KEY}",
            "Content-Type": "application/json", "Prefer": "return=representation"
        }
    )
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read()), r.status
    except urllib.error.HTTPError as e:
        return {"error": e.read().decode()}, e.code

def supa_sql(sql):
    return supa_post("/rest/v1/rpc/exec_sql", {"query": sql})

def storage_download(path):
    """Download file from Supabase Storage, returns bytes."""
    req = urllib.request.Request(
        f"{SUPA_URL}/storage/v1/object/{BUCKET}/{path}",
        headers={"apikey": SUPA_KEY, "Authorization": f"Bearer {SUPA_KEY}"}
    )
    with urllib.request.urlopen(req) as r:
        return r.read()

def storage_upload(path, data_bytes, content_type="application/pdf"):
    req = urllib.request.Request(
        f"{SUPA_URL}/storage/v1/object/{BUCKET}/{path}",
        data=data_bytes, method="POST",
        headers={
            "apikey": SUPA_KEY, "Authorization": f"Bearer {SUPA_KEY}",
            "Content-Type": content_type, "x-upsert": "true"
        }
    )
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())


# ── PDF parsing ───────────────────────────────────────────────────────────────

def parse_cba_pdf(pdf_bytes, account_label, account_number_last4):
    """
    Parse CBA home loan / investment loan PDF statements.
    Returns list of dicts: {posted_at, description, amount, category}
    """
    try:
        import pdfplumber
    except ImportError:
        raise RuntimeError("pdfplumber not available — add Lambda layer")

    transactions = []
    pdf_file = io.BytesIO(pdf_bytes)

    # Patterns for CBA loan statements
    # Line format: "01 Jan 2024  Interest Charged  $7,084.77 Dr"
    # or:          "04 Jan 2024  NetBank Transfer   $1,000.00 Cr"
    date_pat = re.compile(
        r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})'
        r'\s+(.+?)\s+'
        r'\$?([\d,]+\.\d{2})\s+(Dr|Cr)',
        re.IGNORECASE
    )
    # Alternative: amounts in debit/credit columns
    col_pat = re.compile(
        r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})'
        r'\s+(.+?)\s+'
        r'([\d,]+\.\d{2})?'
        r'\s*([\d,]+\.\d{2})?',
        re.IGNORECASE
    )

    month_map = {
        'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
        'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12
    }

    def parse_date(s):
        parts = s.strip().split()
        if len(parts) == 3:
            d, m, y = int(parts[0]), month_map[parts[1].lower()], int(parts[2])
            return date(y, m, d).isoformat()
        return None

    def categorise(desc, amount):
        desc_l = desc.lower()
        if 'interest' in desc_l:
            return 'Home Loan Interest'
        if any(x in desc_l for x in ['repayment', 'payment', 'transfer', 'netbank']):
            return 'Loan Repayment'
        if 'fee' in desc_l or 'charge' in desc_l:
            return 'Bank Fees'
        if 'redraw' in desc_l:
            return 'Loan Redraw'
        return 'Loan - Other'

    with pdfplumber.open(pdf_file) as pdf:
        full_text = ""
        for page in pdf.pages:
            full_text += (page.extract_text() or "") + "\n"

    # Try pattern match
    for m in date_pat.finditer(full_text):
        dt_str, desc, amt_str, dr_cr = m.groups()
        dt = parse_date(dt_str)
        if not dt:
            continue
        amt = float(amt_str.replace(',', ''))
        # Dr = debit = money out = negative
        if dr_cr.upper() == 'DR':
            amt = -amt
        desc_clean = desc.strip()
        transactions.append({
            "posted_at": dt,
            "description": f"{desc_clean} - CBA {account_label} {account_number_last4 or ''}".strip(),
            "raw_description": desc_clean,
            "amount": amt,
            "category": categorise(desc_clean, amt),
            "source_type": "PDF_LAMBDA"
        })

    return transactions


# ── Ingest logic ──────────────────────────────────────────────────────────────

def get_source(source_id):
    result, status = supa_sql(f"""
        SELECT sr.source_id, sr.bank, sr.account_label, sr.account_number_last4,
               sr.account_id, sr.statement_period_start, sr.statement_period_end,
               sr.filename, sr.supabase_path, sr.supabase_bucket, sr.txn_count,
               e.id as entity_id
        FROM maat_source_registry sr
        LEFT JOIN maat_bank_accounts ba ON ba.id = sr.account_id
        LEFT JOIN maat_entities e ON e.id = ba.entity_id
        WHERE sr.source_id = '{source_id}'
        LIMIT 1
    """)
    rows = result.get("rows", [])
    return rows[0] if rows else None

def already_loaded(source_id, account_id, period_start, period_end):
    result, _ = supa_sql(f"""
        SELECT COUNT(*) as c FROM maat_transactions
        WHERE source_id = '{source_id}'
        OR (account_id = '{account_id}'
            AND posted_at BETWEEN '{period_start}' AND '{period_end}'
            AND is_estimate IS NOT TRUE)
    """)
    rows = result.get("rows", [])
    return int(rows[0].get("c", 0)) > 0 if rows else False

def insert_transactions(txns, source, source_id, import_run_id):
    if not txns:
        return 0

    entity_id = source.get("entity_id") or ""
    account_id = source.get("account_id") or ""
    filename = source.get("filename") or ""

    rows_sql = []
    for t in txns:
        posted = t["posted_at"]
        desc = t["description"].replace("'", "''")
        raw  = t.get("raw_description", "").replace("'", "''")
        amt  = t["amount"]
        cat  = t.get("category", "Uncategorised").replace("'", "''")
        sid  = source_id
        irid = import_run_id
        sf   = filename.replace("'", "''")

        rows_sql.append(
            f"(gen_random_uuid(), '{entity_id}', '{account_id}', '{posted}', "
            f"'{desc}', {amt}, 0, NULL, NULL, '{cat}', NULL, NULL, "
            f"false, NULL, NULL, 'unclassified', '{sf}', NULL, '{raw}', "
            f"NOW(), NOW(), NULL, NULL, false, NULL, 0, false, NULL, NULL, "
            f"'{sid}', NULL, 'PDF_LAMBDA', NULL, '{irid}', NULL, false)"
        )

    # Insert in batches of 50
    inserted = 0
    batch_size = 50
    cols = ("id, entity_id, account_id, posted_at, description, amount, gst_amount, "
            "gst_type, bas_label, category, subcategory, vendor, is_rd, rd_project_id, "
            "rd_evidence_id, classification_status, source_file, source_row, raw_description, "
            "created_at, updated_at, classified_at, classified_by_rule_id, rd_eligible, "
            "usage_type, personal_pct, is_tax_deductible, deduction_category, account_code, "
            "source_id, bas_provenance, source_type, rd_work_item_id, import_run_id, "
            "business_slug, is_estimate")

    for i in range(0, len(rows_sql), batch_size):
        batch = rows_sql[i:i+batch_size]
        sql = f"INSERT INTO maat_transactions ({cols}) VALUES {','.join(batch)} ON CONFLICT DO NOTHING"
        result, status = supa_sql(sql)
        if result.get("success"):
            inserted += len(batch)
        else:
            print(f"Insert batch error: {result.get('error')}")

    return inserted

def ingest_source_id(source_id, pdf_bytes=None):
    source = get_source(source_id)
    if not source:
        return {"error": f"source_id {source_id} not found"}

    account_id   = source["account_id"]
    period_start = source["statement_period_start"]
    period_end   = source["statement_period_end"]
    filename     = source["filename"]
    supa_path    = source.get("supabase_path")

    if already_loaded(source_id, account_id, period_start, period_end):
        return {"skipped": True, "reason": "already loaded", "source_id": source_id}

    # Get PDF bytes
    if pdf_bytes is None:
        if not supa_path:
            return {"error": f"No supabase_path for {filename} — upload PDF first"}
        pdf_bytes = storage_download(supa_path)

    # Parse
    txns = parse_cba_pdf(
        pdf_bytes,
        source["account_label"],
        source["account_number_last4"]
    )

    if not txns:
        return {"error": f"No transactions extracted from {filename}", "source_id": source_id}

    # Filter to statement period
    txns = [t for t in txns if period_start <= t["posted_at"] <= period_end]

    import_run_id = str(uuid.uuid4())
    inserted = insert_transactions(txns, source, source_id, import_run_id)

    # Update source_registry txn_count
    supa_sql(f"""
        UPDATE maat_source_registry
        SET txn_count = {len(txns)}, status = 'INGESTED', updated_at = NOW()
        WHERE source_id = '{source_id}'
    """)

    return {
        "success": True,
        "source_id": source_id,
        "filename": filename,
        "period": f"{period_start} → {period_end}",
        "extracted": len(txns),
        "inserted": inserted,
        "import_run_id": import_run_id
    }


def ingest_pending(bank_filter=None):
    """Find all source_registry entries with 0 loaded txns and ingest them."""
    bank_clause = f"AND sr.bank = '{bank_filter}'" if bank_filter else ""
    result, _ = supa_sql(f"""
        SELECT sr.source_id, sr.filename, sr.bank, sr.account_id,
               sr.statement_period_start, sr.statement_period_end,
               sr.supabase_path, sr.txn_count as registered_txns,
               COALESCE((
                 SELECT COUNT(*) FROM maat_transactions t
                 WHERE t.account_id = sr.account_id
                 AND t.posted_at BETWEEN sr.statement_period_start AND sr.statement_period_end
                 AND t.is_estimate IS NOT TRUE
               ), 0) as loaded_txns
        FROM maat_source_registry sr
        WHERE sr.txn_count > 0
        {bank_clause}
        HAVING COALESCE((
          SELECT COUNT(*) FROM maat_transactions t
          WHERE t.account_id = sr.account_id
          AND t.posted_at BETWEEN sr.statement_period_start AND sr.statement_period_end
          AND t.is_estimate IS NOT TRUE
        ), 0) = 0
        ORDER BY sr.bank, sr.statement_period_start
    """)
    pending = result.get("rows", [])

    results = []
    for p in pending:
        if not p.get("supabase_path"):
            results.append({
                "source_id": p["source_id"],
                "filename": p["filename"],
                "status": "NEEDS_UPLOAD",
                "note": "No supabase_path — PDF not in storage"
            })
            continue
        r = ingest_source_id(p["source_id"])
        results.append(r)

    return {"processed": len(results), "results": results}


# ── Lambda handler ────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    action = event.get("action", "ingest_pending")

    try:
        if action == "ingest_source":
            source_id = event.get("source_id")
            if not source_id:
                return {"success": False, "error": "source_id required"}
            # Optional: accept base64 PDF directly
            pdf_bytes = None
            if event.get("pdf_base64"):
                pdf_bytes = base64.b64decode(event["pdf_base64"])
            return ingest_source_id(source_id, pdf_bytes)

        elif action == "ingest_pending":
            bank = event.get("bank")
            return ingest_pending(bank)

        elif action == "upload_and_ingest":
            # Accept base64 PDF, upload to storage, then ingest
            pdf_b64  = event.get("pdf_base64")
            filename = event.get("filename")
            source_id = event.get("source_id")
            if not all([pdf_b64, filename, source_id]):
                return {"success": False, "error": "pdf_base64, filename, source_id required"}

            pdf_bytes = base64.b64decode(pdf_b64)
            storage_path = f"cba/{filename}"

            # Upload to Supabase storage
            storage_upload(storage_path, pdf_bytes)

            # Update source_registry with path
            supa_sql(f"""
                UPDATE maat_source_registry
                SET supabase_path = '{storage_path}',
                    supabase_bucket = 'statements',
                    updated_at = NOW()
                WHERE source_id = '{source_id}'
            """)

            return ingest_source_id(source_id, pdf_bytes)

        elif action == "register_and_ingest":
            # Full pipeline: base64 PDF → detect account → register → ingest
            pdf_b64  = event.get("pdf_base64")
            filename = event.get("filename")
            account_id = event.get("account_id")
            period_start = event.get("period_start")
            period_end   = event.get("period_end")
            bank = event.get("bank", "CBA")
            account_label = event.get("account_label", "")
            account_number_last4 = event.get("account_number_last4", "")
            txn_count_hint = event.get("txn_count", 0)

            if not all([pdf_b64, filename, account_id, period_start, period_end]):
                return {"success": False, "error": "pdf_base64, filename, account_id, period_start, period_end required"}

            pdf_bytes = base64.b64decode(pdf_b64)
            storage_path = f"cba/{filename}"
            storage_upload(storage_path, pdf_bytes)

            new_source_id = str(uuid.uuid4())
            supa_sql(f"""
                INSERT INTO maat_source_registry
                  (source_id, bank, account_label, account_number_last4, account_id,
                   statement_period_start, statement_period_end, provider,
                   filename, txn_count, status, supabase_bucket, supabase_path,
                   ingestion_method, created_at, updated_at)
                VALUES
                  ('{new_source_id}', '{bank}', '{account_label}', '{account_number_last4}',
                   '{account_id}', '{period_start}', '{period_end}', '{bank}',
                   '{filename}', {txn_count_hint}, 'REGISTERED', 'statements',
                   '{storage_path}', 'PDF_LAMBDA', NOW(), NOW())
                ON CONFLICT DO NOTHING
            """)

            return ingest_source_id(new_source_id, pdf_bytes)

        else:
            return {"success": False, "error": f"Unknown action: {action}"}

    except Exception as e:
        import traceback
        return {"success": False, "error": str(e), "trace": traceback.format_exc()}
