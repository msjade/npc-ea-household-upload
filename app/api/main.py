import csv
import io
import os
from datetime import date, datetime

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is required")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

app = FastAPI(title="NPC EA Household Upload")
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")


def init_db():
    # SQLAlchemy 2.0: use a connection (engine.execute is removed)
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
@app.get("/routes")
def routes():
    return [{"path": r.path, "name": r.name, "methods": sorted(list(r.methods or []))} for r in app.routes]


@app.on_event("startup")
def _startup():
    init_db()
