import csv
import io
import os
from datetime import date, datetime

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from sqlalchemy import create_engine, text

# -------------------------
# Config
# -------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is required")

BUILD_ID = os.getenv("BUILD_ID", "npc-ea-household-upload-2026-01-20")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

app = FastAPI(title="NPC EA Household Upload")
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")


# -------------------------
# DB Init (SQLAlchemy 2.0 safe)
# -------------------------
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

        # Upload history (per client)
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

        # Prevent duplicates from same client (same project + date + EA)
        conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_upload_unique
        ON ea_uploads (client_name, client_project, collection_date, NAT_EA_SN);
        """))

        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS ix_upload_nat_ea_sn
        ON ea_uploads (NAT_EA_SN);
        """))


@app.on_event("startup")
def _startup():
    init_db()


# -------------------------
# Helpers
# -------------------------
def parse_csv_file(file_bytes: bytes):
    """
    Expected columns: NAT_EA_SN,HOUSEHOLD_COUNT
    Returns: (rows, error)
      rows = list[(nat_ea_sn:str, household_count:int)]
    """
    text_stream = io.StringIO(file_bytes.decode("utf-8-sig", errors="replace"))
    reader = csv.DictReader(text_stream)

    required = {"NAT_EA_SN", "HOUSEHOLD_COUNT"}
    if not reader.fieldnames:
        return None, "Your CSV looks empty. Please use the provided template."

    normalized = [c.strip() for c in reader.fieldnames]
    missing = required - set(normalized)
    if missing:
        return None, (
            f"Missing column(s): {', '.join(sorted(missing))}. "
            f"Use template columns: NAT_EA_SN, HOUSEHOLD_COUNT."
        )

    rows = []
    seen_in_file = set()

    for i, r in enumerate(reader, start=2):
        nat = (r.get("NAT_EA_SN") or "").strip()
        hh_raw = (r.get("HOUSEHOLD_COUNT") or "").strip()

        if not nat:
            return None, f"Row {i}: NAT_EA_SN is empty."
        if nat in seen_in_file:
            return None, f"Row {i}: Duplicate NAT_EA_SN '{nat}' found inside this CSV."
        seen_in_file.add(nat)

        try:
            hh = int(hh_raw)
        except Exception:
            return None, f"Row {i}: HOUSEHOLD_COUNT must be a whole number."

        if hh < 0:
            return None, f"Row {i}: HOUSEHOLD_COUNT cannot be negative."

        rows.append((nat, hh))

    if not rows:
        return None, "No data rows found. Please add at least one EA record."

    return rows, None


# -------------------------
# Routes
# -------------------------
@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/home", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/build")
def build():
    return {"build": BUILD_ID}


@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat() + "Z", "build": BUILD_ID}


@app.get("/stats")
def stats():
    # quick verification that DB writes are happening
    with engine.begin() as conn:
        m = conn.execute(text("SELECT COUNT(*) FROM ea_frame")).scalar()
        u = conn.execute(text("SELECT COUNT(*) FROM ea_uploads")).scalar()
    return {"ea_frame": int(m or 0), "ea_uploads": int(u or 0), "build": BUILD_ID}


@app.get("/routes")
def routes():
    return [
        {"path": r.path, "name": r.name, "methods": sorted(list(r.methods or []))}
        for r in app.routes
    ]


@app.post("/upload", response_class=HTMLResponse)
def upload(
    request: Request,
    client_name: str = Form(...),
    client_project: str = Form(...),
    collection_date: str = Form(...),
    file: UploadFile = File(...),
):
    allow_overwrite = False
    ...

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

    rows, err = parse_csv_file(file_bytes)
    if err:
        return templates.TemplateResponse(
            "result.html",
            {"request": request, "ok": False, "message": err},
        )

    allow_overwrite = (overwrite == "yes")

    updated = 0
    blocked = 0
    duplicates = 0
    inserted_uploads = 0
    notes_preview = []

    with engine.begin() as conn:
        for nat, hh in rows:
            # Prevent same-client duplicates (same project/date/EA)
            exists_same = conn.execute(text("""
                SELECT 1 FROM ea_uploads
                WHERE client_name=:cn AND client_project=:cp AND collection_date=:cd AND NAT_EA_SN=:nat
                LIMIT 1
            """), {"cn": client_name, "cp": client_project, "cd": cdate, "nat": nat}).fetchone()

            if exists_same:
                duplicates += 1
                if len(notes_preview) < 15:
                    notes_preview.append(f"{nat}: already uploaded earlier by you for this same project/date.")
                continue

            # Check if EA exists in master
            master = conn.execute(text("""
                SELECT HOUSEHOLD_COUNT, last_updated_by, last_updated_project
                FROM ea_frame WHERE NAT_EA_SN=:nat
            """), {"nat": nat}).fetchone()

            status = "saved_only"
            note = None

            if master is None:
                # Insert new EA into master
                conn.execute(text("""
                    INSERT INTO ea_frame (NAT_EA_SN, HOUSEHOLD_COUNT, last_updated_by, last_updated_project, last_updated_date, last_updated_at)
                    VALUES (:nat, :hh, :cn, :cp, :cd, NOW())
                """), {"nat": nat, "hh": hh, "cn": client_name, "cp": client_project, "cd": cdate})
                updated += 1
                status = "master_updated"
                note = "Inserted new EA into master."
            else:
                _, last_by, last_proj = master
                same_owner = (last_by == client_name and last_proj == client_project)

                if same_owner or allow_overwrite:
                    conn.execute(text("""
                        UPDATE ea_frame
                        SET HOUSEHOLD_COUNT=:hh,
                            last_updated_by=:cn,
                            last_updated_project=:cp,
                            last_updated_date=:cd,
                            last_updated_at=NOW()
                        WHERE NAT_EA_SN=:nat
                    """), {"nat": nat, "hh": hh, "cn": client_name, "cp": client_project, "cd": cdate})
                    updated += 1
                    status = "master_updated"
                    note = "Master updated."
                else:
                    blocked += 1
                    status = "blocked_policy_a"
                    note = f"Blocked by Policy A: master already updated by '{last_by}' ({last_proj})."

            # Always keep upload history record
            conn.execute(text("""
                INSERT INTO ea_uploads
                  (NAT_EA_SN, HOUSEHOLD_COUNT, client_name, client_project, collection_date, status, note)
                VALUES
                  (:nat, :hh, :cn, :cp, :cd, :st, :note)
            """), {"nat": nat, "hh": hh, "cn": client_name, "cp": client_project, "cd": cdate, "st": status, "note": note})
            inserted_uploads += 1

            if note and len(notes_preview) < 15:
                notes_preview.append(f"{nat}: {note}")

    message = (
        "Upload processed successfully.\n"
        f"Saved submissions: {inserted_uploads}\n"
        f"Master updated: {updated}\n"
        f"Blocked (Policy A): {blocked}\n"
        f"Duplicates ignored: {duplicates}\n"
        "Thank you for supporting NPC data updates."
    )

    return templates.TemplateResponse(
        "result.html",
        {"request": request, "ok": True, "message": message, "notes": notes_preview},
    )
