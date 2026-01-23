import csv
import io
import os
import hashlib
from datetime import date, datetime
from typing import List, Tuple, Optional

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError


# ----------------------------
# Config
# ----------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is required")

# Save space by default: store only batch summary, not every row.
# Set STORE_ROW_AUDIT=1 if you want per-row history.
STORE_ROW_AUDIT = os.getenv("STORE_ROW_AUDIT", "0").strip() == "1"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

app = FastAPI(title="NPC EA Household Upload")
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")


# ----------------------------
# DB init
# ----------------------------
def init_db():
    with engine.begin() as conn:
        # Master table
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS ea_frame (
          NAT_EA_SN TEXT PRIMARY KEY,
          HOUSEHOLD_COUNT INTEGER,
          last_updated_by TEXT,
          last_updated_project TEXT,
          last_updated_date DATE,
          last_updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        """))

        # Batch table
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS upload_batches (
          id BIGSERIAL PRIMARY KEY,
          client_name TEXT NOT NULL,
          client_project TEXT NOT NULL,
          collection_date DATE NOT NULL,
          file_hash TEXT NOT NULL,
          file_name TEXT,
          total_rows INTEGER NOT NULL,
          valid_rows INTEGER NOT NULL,
          invalid_rows INTEGER NOT NULL,
          duplicate_in_file INTEGER NOT NULL,
          master_inserted INTEGER NOT NULL,
          master_updated INTEGER NOT NULL,
          master_skipped INTEGER NOT NULL,
          created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """))

        conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_batch_dedupe
        ON upload_batches (client_name, client_project, collection_date, file_hash);
        """))

        # Upload history (may already exist from old versions)
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS ea_uploads (
          id BIGSERIAL PRIMARY KEY,
          NAT_EA_SN TEXT NOT NULL,
          HOUSEHOLD_COUNT INTEGER NOT NULL,
          client_name TEXT NOT NULL,
          client_project TEXT NOT NULL,
          collection_date DATE NOT NULL,
          uploaded_at TIMESTAMPTZ DEFAULT NOW(),
          status TEXT NOT NULL,
          note TEXT
        );
        """))

        # ✅ Migration: add batch_id column if missing
        conn.execute(text("""
        ALTER TABLE ea_uploads
        ADD COLUMN IF NOT EXISTS batch_id BIGINT;
        """))

        # ✅ Add FK only if not already present
        # (Postgres doesn't have IF NOT EXISTS for ADD CONSTRAINT, so we guard it)
        conn.execute(text("""
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'fk_ea_uploads_batch_id'
          ) THEN
            ALTER TABLE ea_uploads
              ADD CONSTRAINT fk_ea_uploads_batch_id
              FOREIGN KEY (batch_id) REFERENCES upload_batches(id)
              ON DELETE CASCADE;
          END IF;
        END $$;
        """))

        # Indexes
        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS ix_upload_batch_id
        ON ea_uploads (batch_id);
        """))

        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS ix_upload_nat_ea_sn
        ON ea_uploads (NAT_EA_SN);
        """))



@app.on_event("startup")
def _startup():
    init_db()


# ----------------------------
# Helpers
# ----------------------------
def normalize_csv_bytes(file_bytes: bytes) -> bytes:
    """
    Normalize CSV for stable hashing:
    - decode utf-8-sig to drop BOM
    - normalize line endings to \n
    - strip trailing spaces on lines
    """
    txt = file_bytes.decode("utf-8-sig", errors="replace")
    txt = txt.replace("\r\n", "\n").replace("\r", "\n")
    lines = [ln.rstrip() for ln in txt.split("\n")]
    norm = "\n".join(lines).strip() + "\n"
    return norm.encode("utf-8")


def sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def parse_csv_file(file_bytes: bytes) -> Tuple[Optional[List[Tuple[str, int]]], Optional[str], int]:
    """
    Expected columns: NAT_EA_SN,HOUSEHOLD_COUNT
    Returns: (rows, error_message, duplicates_in_file)
    """
    text_stream = io.StringIO(file_bytes.decode("utf-8-sig", errors="replace"))
    reader = csv.DictReader(text_stream)

    required = {"NAT_EA_SN", "HOUSEHOLD_COUNT"}
    if not reader.fieldnames:
        return None, "Your CSV looks empty. Please use the provided template.", 0

    cols = set([c.strip() for c in reader.fieldnames])
    missing = required - cols
    if missing:
        return None, f"Missing column(s): {', '.join(sorted(missing))}. Required: NAT_EA_SN, HOUSEHOLD_COUNT.", 0

    rows: List[Tuple[str, int]] = []
    seen_in_file = set()
    dup_in_file = 0

    for i, r in enumerate(reader, start=2):
        nat = (r.get("NAT_EA_SN") or "").strip()
        hh_raw = (r.get("HOUSEHOLD_COUNT") or "").strip()

        if not nat:
            return None, f"Row {i}: NAT_EA_SN is empty.", dup_in_file

        if nat in seen_in_file:
            dup_in_file += 1
            # skip duplicates inside file
            continue
        seen_in_file.add(nat)

        try:
            hh = int(hh_raw)
        except Exception:
            return None, f"Row {i}: HOUSEHOLD_COUNT must be a whole number.", dup_in_file

        if hh < 0:
            return None, f"Row {i}: HOUSEHOLD_COUNT cannot be negative.", dup_in_file

        rows.append((nat, hh))

    if not rows:
        return None, "No valid data rows found (after removing duplicates).", dup_in_file

    return rows, None, dup_in_file


# ----------------------------
# Routes
# ----------------------------
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat() + "Z"}


@app.get("/routes")
def routes():
    return [{"path": r.path, "name": r.name, "methods": sorted(list(r.methods or []))} for r in app.routes]


@app.post("/upload", response_class=HTMLResponse)
def upload(
    request: Request,
    client_name: str = Form(...),
    client_project: str = Form(...),
    collection_date: str = Form(...),
    file: UploadFile = File(...),
):
    client_name = (client_name or "").strip()
    client_project = (client_project or "").strip()

    if not client_name or not client_project:
        return templates.TemplateResponse(
            "result.html",
            {"request": request, "ok": False, "message": "Please provide Client Name and Client Project."},
        )

    try:
        cdate = date.fromisoformat(collection_date.strip())
    except Exception:
        return templates.TemplateResponse(
            "result.html",
            {"request": request, "ok": False, "message": "Collection Date is invalid. Please use the date picker."},
        )

    try:
        raw_bytes = file.file.read()
    except Exception:
        return templates.TemplateResponse(
            "result.html",
            {"request": request, "ok": False, "message": "Could not read the uploaded file. Please try again."},
        )

    # stable hash for "same file reupload" protection
    norm_bytes = normalize_csv_bytes(raw_bytes)
    file_hash = sha256_hex(norm_bytes)

    rows, err, dup_in_file = parse_csv_file(raw_bytes)
    if err:
        return templates.TemplateResponse("result.html", {"request": request, "ok": False, "message": err})

    total_rows = len(rows) + dup_in_file
    valid_rows = len(rows)
    invalid_rows = 0  # parse_csv returns errors immediately

    master_inserted = 0
    master_updated = 0
    master_skipped = 0
    notes_preview = []

    with engine.begin() as conn:
        # 1) Deduplicate same-file uploads (same client/project/date)
        existing_batch = conn.execute(text("""
            SELECT id, created_at
            FROM upload_batches
            WHERE client_name=:cn AND client_project=:cp AND collection_date=:cd AND file_hash=:fh
            LIMIT 1
        """), {"cn": client_name, "cp": client_project, "cd": cdate, "fh": file_hash}).fetchone()

        if existing_batch:
            bid, created_at = existing_batch
            msg = (
                "This file was already uploaded earlier for the same Client/Project/Date.\n\n"
                f"Batch ID: {bid}\n"
                f"Uploaded at: {created_at}\n\n"
                "No changes were applied again."
            )
            return templates.TemplateResponse(
                "result.html",
                {"request": request, "ok": True, "message": msg, "notes": []},
            )

        # 2) Create batch record first (we'll update counts at the end)
        batch_id = conn.execute(text("""
            INSERT INTO upload_batches
              (client_name, client_project, collection_date, file_hash, file_name,
               total_rows, valid_rows, invalid_rows, duplicate_in_file,
               master_inserted, master_updated, master_skipped)
            VALUES
              (:cn, :cp, :cd, :fh, :fn,
               :tr, :vr, :ir, :df,
               0, 0, 0)
            RETURNING id;
        """), {
            "cn": client_name, "cp": client_project, "cd": cdate,
            "fh": file_hash, "fn": getattr(file, "filename", None),
            "tr": total_rows, "vr": valid_rows, "ir": invalid_rows, "df": dup_in_file,
        }).fetchone()[0]

        # 3) Apply “safe update” rule:
        #    update master only if collection_date is NEWER than last_updated_date
        for nat, hh in rows:
            master = conn.execute(text("""
                SELECT HOUSEHOLD_COUNT, last_updated_by, last_updated_project, last_updated_date
                FROM ea_frame WHERE NAT_EA_SN=:nat
            """), {"nat": nat}).fetchone()

            status = "received"
            note = None

            if master is None:
                conn.execute(text("""
                    INSERT INTO ea_frame
                      (NAT_EA_SN, HOUSEHOLD_COUNT, last_updated_by, last_updated_project, last_updated_date, last_updated_at)
                    VALUES
                      (:nat, :hh, :cn, :cp, :cd, NOW())
                """), {"nat": nat, "hh": hh, "cn": client_name, "cp": client_project, "cd": cdate})
                master_inserted += 1
                status = "master_inserted"
                note = "Inserted new EA into master."
            else:
                _, last_by, last_proj, last_date = master
                last_date = last_date  # may be None for legacy rows

                # If no last_date exists, treat incoming as newer
                can_update = (last_date is None) or (cdate > last_date)

                if can_update:
                    conn.execute(text("""
                        UPDATE ea_frame
                        SET HOUSEHOLD_COUNT=:hh,
                            last_updated_by=:cn,
                            last_updated_project=:cp,
                            last_updated_date=:cd,
                            last_updated_at=NOW()
                        WHERE NAT_EA_SN=:nat
                    """), {"nat": nat, "hh": hh, "cn": client_name, "cp": client_project, "cd": cdate})
                    master_updated += 1
                    status = "master_updated"
                    note = f"Master updated (newer date). Previous: {last_by} ({last_proj}) on {last_date}."
                else:
                    master_skipped += 1
                    status = "master_skipped"
                    note = f"Not applied to master (older/equal date). Current master: {last_by} ({last_proj}) on {last_date}."

            # Optional per-row audit (OFF by default to save DB space)
            if STORE_ROW_AUDIT:
                conn.execute(text("""
                    INSERT INTO ea_uploads
                      (batch_id, NAT_EA_SN, HOUSEHOLD_COUNT, client_name, client_project, collection_date, status, note)
                    VALUES
                      (:bid, :nat, :hh, :cn, :cp, :cd, :st, :note)
                """), {
                    "bid": batch_id, "nat": nat, "hh": hh,
                    "cn": client_name, "cp": client_project, "cd": cdate,
                    "st": status, "note": note
                })

            if note and len(notes_preview) < 15:
                notes_preview.append(f"{nat}: {note}")

        # 4) Update batch counters
        conn.execute(text("""
            UPDATE upload_batches
            SET master_inserted=:mi,
                master_updated=:mu,
                master_skipped=:ms
            WHERE id=:bid
        """), {"mi": master_inserted, "mu": master_updated, "ms": master_skipped, "bid": batch_id})

    message = (
        f"Upload processed successfully.\n\n"
        f"Batch ID: {batch_id}\n"
        f"Total rows (incl. duplicates in file): {total_rows}\n"
        f"Valid unique rows processed: {valid_rows}\n"
        f"Duplicates removed inside file: {dup_in_file}\n\n"
        f"Master inserted: {master_inserted}\n"
        f"Master updated: {master_updated}\n"
        f"Not applied to master (older/equal date): {master_skipped}\n\n"
        f"Thank you for supporting NPC data updates."
    )

    return templates.TemplateResponse(
        "result.html",
        {"request": request, "ok": True, "message": message, "notes": notes_preview},
    )
