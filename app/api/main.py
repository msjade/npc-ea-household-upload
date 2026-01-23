import csv
import io
import os
import hashlib
from datetime import date, datetime
from typing import List, Tuple, Optional

from fastapi import FastAPI, File, Form, Request, UploadFile, Header
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from sqlalchemy import create_engine, text

# -----------------------------
# Config
# -----------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is required")

# Optional admin key (only if you later want admin override features)
ADMIN_KEY = (os.getenv("ADMIN_KEY") or "").strip()

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)

app = FastAPI(title="NPC EA Household Upload")
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

BUILD_ID = os.getenv("BUILD_ID", "npc-ea-household-upload")


# -----------------------------
# DB Init
# -----------------------------
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

        # Upload batches (tiny, helps dedupe file re-uploads + summary)
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS ea_upload_batches (
          id BIGSERIAL PRIMARY KEY,
          client_name TEXT NOT NULL,
          client_project TEXT NOT NULL,
          collection_date DATE NOT NULL,
          file_hash TEXT NOT NULL,
          uploaded_at TIMESTAMPTZ DEFAULT NOW(),
          rows_total INT NOT NULL,
          rows_valid INT NOT NULL,
          rows_applied INT NOT NULL,
          rows_skipped INT NOT NULL,
          duplicates_in_file INT NOT NULL,
          note TEXT
        );
        """))

        # Prevent re-uploading the exact same file in same context
        conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_batch_dedupe
        ON ea_upload_batches (client_name, client_project, collection_date, file_hash);
        """))

        # Upload items (optional audit, but still lightweight)
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS ea_upload_items (
          batch_id BIGINT NOT NULL REFERENCES ea_upload_batches(id) ON DELETE CASCADE,
          NAT_EA_SN TEXT NOT NULL,
          HOUSEHOLD_COUNT INTEGER NOT NULL,
          status TEXT NOT NULL,
          note TEXT,
          PRIMARY KEY (batch_id, NAT_EA_SN)
        );
        """))

        # Prevent duplicate EA uploads by same client/project/date
        # (We keep it in items table via PK, and also can enforce in SQL if needed)
        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS ix_items_nat_ea_sn
        ON ea_upload_items (NAT_EA_SN);
        """))


@app.on_event("startup")
def _startup():
    init_db()


# -----------------------------
# Helpers
# -----------------------------
def sha256_bytes(b: bytes) -> str:
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

    header = {c.strip() for c in reader.fieldnames}
    missing = required - header
    if missing:
        return None, f"Missing column(s): {', '.join(sorted(missing))}. Use: NAT_EA_SN, HOUSEHOLD_COUNT.", 0

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
            # We do NOT fail — we just ignore duplicates inside same CSV
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
        return None, "No valid data rows found. Please add at least one EA record.", dup_in_file

    return rows, None, dup_in_file


def is_admin(x_admin_key: Optional[str]) -> bool:
    return bool(ADMIN_KEY) and (x_admin_key or "").strip() == ADMIN_KEY


# -----------------------------
# Routes
# -----------------------------
@app.get("/build")
def build():
    return {"build": BUILD_ID}


@app.get("/routes")
def routes():
    return [{"path": r.path, "name": r.name, "methods": sorted(list(r.methods or []))} for r in app.routes]


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat() + "Z"}


@app.post("/upload", response_class=HTMLResponse)
def upload(
    request: Request,
    client_name: str = Form(...),
    client_project: str = Form(...),
    collection_date: str = Form(...),
    file: UploadFile = File(...),
    # Hidden admin-only override (not exposed on UI)
    overwrite: str = Form("no"),
    x_admin_key: Optional[str] = Header(default=None),
):
    # Clean inputs
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
        file_bytes = file.file.read()
    except Exception:
        return templates.TemplateResponse(
            "result.html",
            {"request": request, "ok": False, "message": "Could not read the uploaded file. Please try again."},
        )

    if not file_bytes:
        return templates.TemplateResponse(
            "result.html",
            {"request": request, "ok": False, "message": "Uploaded file is empty."},
        )

    file_hash = sha256_bytes(file_bytes)

    rows, err, dup_in_file = parse_csv_file(file_bytes)
    if err:
        return templates.TemplateResponse("result.html", {"request": request, "ok": False, "message": err})

    # Admin-only overwrite ability (still not shown to client UI)
    allow_overwrite = (overwrite == "yes") and is_admin(x_admin_key)

    # Counters
    total = len(rows)
    applied = 0
    skipped = 0
    duplicates_context = 0
    notes_preview = []

    with engine.begin() as conn:
        # 1) block exact same file re-upload for same context
        existing_batch = conn.execute(text("""
            SELECT id FROM ea_upload_batches
            WHERE client_name=:cn AND client_project=:cp AND collection_date=:cd AND file_hash=:fh
            LIMIT 1
        """), {"cn": client_name, "cp": client_project, "cd": cdate, "fh": file_hash}).fetchone()

        if existing_batch:
            msg = (
                "Duplicate upload blocked.\n"
                "This exact same file was already uploaded earlier for the same Client/Project/Date.\n"
                "If you intended a correction, export a fresh CSV (so the file hash changes)."
            )
            return templates.TemplateResponse(
                "result.html",
                {"request": request, "ok": False, "message": msg, "notes": []},
            )

        # 2) create batch record early (we'll update summary later)
        batch_id = conn.execute(text("""
            INSERT INTO ea_upload_batches
              (client_name, client_project, collection_date, file_hash,
               rows_total, rows_valid, rows_applied, rows_skipped, duplicates_in_file, note)
            VALUES
              (:cn, :cp, :cd, :fh, :rt, :rv, 0, 0, :df, :note)
            RETURNING id
        """), {
            "cn": client_name, "cp": client_project, "cd": cdate, "fh": file_hash,
            "rt": total, "rv": total, "df": dup_in_file,
            "note": "Batch created."
        }).scalar_one()

        # 3) Process each row
        for nat, hh in rows:
            # Prevent duplicates for same client/project/date (even across batches)
            # We'll use upload_items join batch to enforce context uniqueness:
            exists_context = conn.execute(text("""
                SELECT 1
                FROM ea_upload_items i
                JOIN ea_upload_batches b ON b.id = i.batch_id
                WHERE b.client_name=:cn AND b.client_project=:cp AND b.collection_date=:cd
                  AND i.NAT_EA_SN=:nat
                LIMIT 1
            """), {"cn": client_name, "cp": client_project, "cd": cdate, "nat": nat}).fetchone()

            if exists_context:
                duplicates_context += 1
                skipped += 1
                if len(notes_preview) < 15:
                    notes_preview.append(f"{nat}: duplicate for same Client/Project/Date (ignored).")
                continue

            # Read master state
            master = conn.execute(text("""
                SELECT HOUSEHOLD_COUNT, last_updated_date
                FROM ea_frame WHERE NAT_EA_SN=:nat
            """), {"nat": nat}).fetchone()

            status = "saved_only"
            note = None

            if master is None:
                # Insert new master
                conn.execute(text("""
                    INSERT INTO ea_frame (NAT_EA_SN, HOUSEHOLD_COUNT, last_updated_by, last_updated_project, last_updated_date, last_updated_at)
                    VALUES (:nat, :hh, :cn, :cp, :cd, NOW())
                """), {"nat": nat, "hh": hh, "cn": client_name, "cp": client_project, "cd": cdate})
                applied += 1
                status = "master_inserted"
                note = "Inserted into master."
            else:
                _, master_date = master

                # Rule: newer collection_date wins (unless admin override)
                can_update = allow_overwrite or (master_date is None) or (cdate > master_date)

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
                    applied += 1
                    status = "master_updated"
                    note = "Master updated (newer date rule)."
                else:
                    skipped += 1
                    status = "not_applied_older_date"
                    note = f"Not applied: master has newer/equal date ({master_date})."

            # Store minimal audit item (1 row per EA per batch)
            conn.execute(text("""
                INSERT INTO ea_upload_items (batch_id, NAT_EA_SN, HOUSEHOLD_COUNT, status, note)
                VALUES (:bid, :nat, :hh, :st, :note)
            """), {"bid": batch_id, "nat": nat, "hh": hh, "st": status, "note": note})

            if note and len(notes_preview) < 15:
                notes_preview.append(f"{nat}: {note}")

        # 4) Update batch summary (compact)
        conn.execute(text("""
            UPDATE ea_upload_batches
            SET rows_applied=:ap, rows_skipped=:sk, note=:note
            WHERE id=:bid
        """), {
            "bid": batch_id,
            "ap": applied,
            "sk": (skipped + duplicates_context),
            "note": f"Completed. applied={applied}, skipped={skipped}, duplicates_context={duplicates_context}, dup_in_file={dup_in_file}"
        })

    # Friendly summary
    msg = (
        "Upload processed successfully.\n"
        f"Rows read (unique in file): {total}\n"
        f"Duplicates inside file ignored: {dup_in_file}\n"
        f"Duplicates for same Client/Project/Date ignored: {duplicates_context}\n"
        f"Applied to master: {applied}\n"
        f"Skipped (older date or duplicates): {skipped + duplicates_context}\n"
        f"Batch ID: {batch_id}\n"
        "Thank you for supporting NPC data updates."
    )

    return templates.TemplateResponse(
        "result.html",
        {"request": request, "ok": True, "message": msg, "notes": notes_preview},
    )
