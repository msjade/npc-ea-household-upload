(function () {
  const form = document.querySelector("form[action='/upload']");
  if (!form) return;

  const fileInput = form.querySelector("input[type='file'][name='file']");
  const btn = form.querySelector("button[type='submit']");

  // UI containers (we’ll create them if not present)
  let panel = document.getElementById("csvPreviewPanel");
  if (!panel) {
    panel = document.createElement("div");
    panel.id = "csvPreviewPanel";
    panel.className = "preview-panel";
    panel.innerHTML = `
      <div class="preview-head">
        <div class="preview-title">CSV Preview</div>
        <div class="preview-meta" id="csvMeta">No file selected</div>
      </div>
      <div class="preview-alert muted" id="csvAlert"></div>
      <div class="preview-table-wrap">
        <table class="preview-table">
          <thead id="csvThead"></thead>
          <tbody id="csvTbody"></tbody>
        </table>
      </div>
    `;
    // Insert before submit button
    btn.parentElement.insertBefore(panel, btn);
  }

  const meta = document.getElementById("csvMeta");
  const alertBox = document.getElementById("csvAlert");
  const thead = document.getElementById("csvThead");
  const tbody = document.getElementById("csvTbody");

  const REQUIRED = ["NAT_EA_SN", "HOUSEHOLD_COUNT"];
  const PREVIEW_ROWS = 20;

  function setAlert(msg, ok) {
    alertBox.textContent = msg || "";
    alertBox.classList.toggle("alert-ok", !!ok);
    alertBox.classList.toggle("alert-bad", ok === false);
  }

  function disableSubmit(disabled) {
    btn.disabled = !!disabled;
    btn.classList.toggle("btn-disabled", !!disabled);
  }

  function parseCSV(text) {
    // Minimal CSV parser (handles commas + quotes reasonably for simple files)
    const lines = text.replace(/\r\n/g, "\n").replace(/\r/g, "\n").split("\n").filter(l => l.trim() !== "");
    if (!lines.length) return { headers: [], rows: [] };

    const splitLine = (line) => {
      const out = [];
      let cur = "";
      let inQuotes = false;
      for (let i = 0; i < line.length; i++) {
        const ch = line[i];
        if (ch === '"' ) {
          // toggle quotes unless escaped ""
          if (inQuotes && line[i + 1] === '"') { cur += '"'; i++; }
          else inQuotes = !inQuotes;
        } else if (ch === "," && !inQuotes) {
          out.push(cur);
          cur = "";
        } else {
          cur += ch;
        }
      }
      out.push(cur);
      return out.map(s => s.trim());
    };

    const headers = splitLine(lines[0]).map(h => h.replace(/^"|"$/g, "").trim());
    const rows = lines.slice(1).map(l => splitLine(l).map(v => v.replace(/^"|"$/g, "").trim()));
    return { headers, rows };
  }

  function buildPreview(headers, rows) {
    thead.innerHTML = "";
    tbody.innerHTML = "";

    const trh = document.createElement("tr");
    headers.forEach(h => {
      const th = document.createElement("th");
      th.textContent = h;
      trh.appendChild(th);
    });
    thead.appendChild(trh);

    rows.slice(0, PREVIEW_ROWS).forEach(r => {
      const tr = document.createElement("tr");
      headers.forEach((_, idx) => {
        const td = document.createElement("td");
        td.textContent = r[idx] ?? "";
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
  }

  function validate(headers, rows) {
    if (!headers.length) {
      return { ok: false, msg: "CSV is empty or unreadable.", stats: null };
    }

    const headerSet = new Set(headers.map(h => h.trim()));
    const missing = REQUIRED.filter(r => !headerSet.has(r));
    if (missing.length) {
      return { ok: false, msg: `Missing column(s): ${missing.join(", ")}. Required: NAT_EA_SN, HOUSEHOLD_COUNT.`, stats: null };
    }

    const colNat = headers.indexOf("NAT_EA_SN");
    const colHh = headers.indexOf("HOUSEHOLD_COUNT");

    let dupInFile = 0;
    let invalid = 0;
    let emptyNat = 0;
    let neg = 0;

    const seen = new Set();

    rows.forEach((r, i) => {
      const nat = (r[colNat] ?? "").trim();
      const hhRaw = (r[colHh] ?? "").trim();

      if (!nat) { emptyNat++; invalid++; return; }
      if (seen.has(nat)) { dupInFile++; return; }
      seen.add(nat);

      const hh = Number(hhRaw);
      if (!Number.isInteger(hh)) { invalid++; return; }
      if (hh < 0) { neg++; invalid++; return; }
    });

    const uniqueRows = seen.size;
    return {
      ok: invalid === 0 && emptyNat === 0,
      msg:
        `Rows: ${rows.length} • Unique NAT_EA_SN: ${uniqueRows} • Duplicates in file: ${dupInFile}` +
        (invalid ? ` • Invalid rows: ${invalid}` : "") +
        (neg ? ` • Negative HH: ${neg}` : ""),
      stats: { totalRows: rows.length, uniqueRows, dupInFile, invalid, emptyNat, neg }
    };
  }

  fileInput.addEventListener("change", async () => {
    setAlert("", null);
    disableSubmit(true);

    const f = fileInput.files?.[0];
    if (!f) {
      meta.textContent = "No file selected";
      thead.innerHTML = "";
      tbody.innerHTML = "";
      disableSubmit(true);
      return;
    }

    meta.textContent = `${f.name} • ${(f.size / 1024).toFixed(1)} KB`;

    // Basic size guard (adjust if you like)
    if (f.size > 10 * 1024 * 1024) {
      setAlert("File too large (>10MB). Please split and upload in batches.", false);
      disableSubmit(true);
      return;
    }

    try {
      const text = await f.text();
      const parsed = parseCSV(text);
      buildPreview(parsed.headers, parsed.rows);

      const v = validate(parsed.headers, parsed.rows);
      if (v.ok) {
        setAlert(v.msg + " • Ready to upload ✅", true);
        disableSubmit(false);
      } else {
        setAlert(v.msg + " • Fix and re-upload ❌", false);
        disableSubmit(true);
      }
    } catch (e) {
      setAlert("Could not read this file. Please re-export as CSV (UTF-8).", false);
      disableSubmit(true);
    }
  });

  // Initial: require file validation before submit
  disableSubmit(true);
})();
