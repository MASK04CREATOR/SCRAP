import io, re, time, socket, threading, calendar, datetime as dt
from concurrent.futures import ThreadPoolExecutor
import requests, pandas as pd, streamlit as st
from pypdf import PdfReader

st.set_page_config(page_title="SCRAP X", page_icon="✖", layout="wide")
MAXURL, WORKERS = 10000, 32
MON = {m.lower(): i for i, m in enumerate(calendar.month_abbr) if m}
MP = r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
PAT = [  # (regex, builder)
 (rf"(\d{{1,2}})(?:st|nd|rd|th)?[\s\-/.,]+{MP}\.?[\s\-/.,]+(\d{{4}})", lambda m: (int(m[3]), MON[m[2][:3].lower()], int(m[1]))),
 (rf"{MP}\.?\s+(\d{{1,2}})(?:st|nd|rd|th)?,?\s+(\d{{4}})", lambda m: (int(m[3]), MON[m[1][:3].lower()], int(m[2]))),
 (r"(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})", lambda m: (int(m[1]), int(m[2]), int(m[3]))),
 (r"(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})", lambda m: (int(m[3]), int(m[2]), int(m[1])) if int(m[2]) <= 12 else (int(m[3]), int(m[1]), int(m[2]))),
 (rf"\b{MP}\.?,?\s+(\d{{4}})\b", lambda m: (int(m[2]), MON[m[1][:3].lower()], 0)),  # month-year -> month end
]
ANCH = re.compile(r"(as\s+(?:at|of|on)|period\s+ended|year\s+ended|month\s+ended|quarter\s+ended|valuation\s+date|data\s+as|portfolio\s+date|reporting\s+date|nav\s+date|record\s+date|for\s+the\s+(?:month|year|period|quarter)|ended|dated|date:)\s*[:\-]?\s*$", re.I)
BAD = re.compile(r"(©|copyright|printed|downloaded|generated|registered|incorporated|established|launched|inception)\s*\S*\s*$", re.I)
KINDS = [("annual", r"annual|\bar\b|yearly"), ("semi-annual", r"semi|half[\s-]?year|interim"), ("quarterly", r"quarter|\bqr\b"),
         ("monthly", r"month|\bmr\b|factsheet|fact\s?sheet|\bfs\b"), ("kiid/kid", r"kiid|\bkid\b|key\s+(?:investor|information)"), ("prospectus", r"prospectus|\bsid\b|\bkim\b")]
PERIOD = {"annual", "semi-annual", "quarterly", "monthly"}

def mk(y, m, d):
    try:
        if d == 0: d = calendar.monthrange(y, m)[1]
        v = dt.date(y, m, d)
        return v if dt.date(1995, 1, 1) <= v <= dt.date.today() + dt.timedelta(days=1) else None
    except Exception: return None

def find_dates(text):
    out = []
    for rx, f in PAT:
        for m in re.finditer(rx, text, re.I):
            d = mk(*f(m))
            if d: out.append((d, m.start(), text[max(0, m.start() - 45):m.start()]))
    return out

def kind_of(*s):
    t = " ".join(s).lower()
    for k, rx in KINDS:
        if re.search(rx, t): return k
    return "other"

def best_content_date(text):
    sc = {}
    for d, pos, pre in find_dates(text):
        s = (6 if ANCH.search(pre) else 0) + (2 if pos < 1500 else 0) - (5 if BAD.search(pre) else 0) + 0.2
        sc[d] = sc.get(d, 0) + s if d in sc else s
    if not sc: return None, 0
    d = max(sc, key=lambda k: (sc[k], k)); return d, sc[d]

def pdf_date(v):
    m = re.search(r"(\d{4})(\d{2})(\d{2})", str(v or ""))
    return mk(int(m[1]), int(m[2]), int(m[3])) if m else None

class Job:
    def __init__(s, urls, hints):
        s.urls, s.hints, s.res, s.done, s.status = urls, hints, {}, 0, "Running"
        s.paused, s.t0, s.finished, s.lock = False, time.time(), False, threading.Lock()
    def online(s):
        try: socket.create_connection(("1.1.1.1", 53), 3).close(); return True
        except OSError: return False
    def wait_online(s):
        while not s.online():
            s.paused = True; s.status = "Paused – no internet. Resumes automatically"; time.sleep(5)
        s.paused = False; s.status = "Running"
    def fetch(s, url):
        err = "unknown"
        for a in range(5):
            try:
                r = requests.get(url, timeout=(10, 45), headers={"User-Agent": "Mozilla/5.0"}, stream=True)
                if r.status_code == 429 or r.status_code >= 500: err = f"HTTP {r.status_code}"; time.sleep(2 * (a + 1)); continue
                if r.status_code != 200: raise ValueError(f"HTTP {r.status_code}")
                b = r.raw.read(40_000_000, decode_content=True)
                if b"%PDF" not in b[:1024]: raise ValueError("Link is not a PDF (landing page or blocked)")
                return b
            except (requests.ConnectionError, requests.Timeout) as e:
                err = "Connection/timeout"; s.wait_online(); time.sleep(1)
        raise ValueError(f"Failed after retries: {err}")
    def ocr(s, b):
        import pytesseract; from pdf2image import convert_from_bytes
        return "\n".join(pytesseract.image_to_string(i) for i in convert_from_bytes(b, dpi=170, first_page=1, last_page=3))
    def work(s, url):
        r = dict(date="", source="", conf="", kind=s.hints.get(url, "other"), error="", text="")
        try:
            b = s.fetch(url); rd = PdfReader(io.BytesIO(b))
            if rd.is_encrypted: rd.decrypt("")
            md = pdf_date((rd.metadata or {}).get("/CreationDate") or (rd.metadata or {}).get("/ModDate"))
            text = ""
            for p in rd.pages[:8]:
                try: text += (p.extract_text() or "") + "\n"
                except Exception: pass
            used_ocr = False
            if len(text.strip()) < 80:
                try: text, used_ocr = s.ocr(b), True
                except Exception as e: r["error"] = f"OCR failed: {e}"
            if r["kind"] == "other": r["kind"] = kind_of(url, text[:800])
            cd, sc = best_content_date(text)
            period = r["kind"] in PERIOD
            if cd and sc >= 6 and (period or not md or abs((cd - md).days) > 45): d, src, cf = cd, "OCR content" if used_ocr else "Content", "High" if sc >= 8 else "Medium"
            elif md: d, src, cf = md, "Metadata", "Medium" if (cd and abs((cd - md).days) > 45) else "High"
            elif cd: d, src, cf = cd, "OCR content" if used_ocr else "Content", "Low"
            else: d = None
            if d: r.update(date=d.isoformat(), source=src, conf=cf, error="")
            elif not r["error"]: r["error"] = "No date found in metadata or content"
            r["text"] = text[:8000]
        except Exception as e: r["error"] = str(e)[:200]
        with s.lock: s.res[url] = r; s.done += 1
    def run(s):
        with ThreadPoolExecutor(WORKERS) as ex: list(ex.map(s.work, s.urls))
        s.finished, s.status = True, "Completed"

def prepare(file):
    raw = pd.read_excel(file, header=None, dtype=object)
    has_h = not raw.iloc[0].astype(str).str.startswith("http").any() and raw.iloc[0].notna().any()
    hdr = [str(x).strip() if pd.notna(x) else "" for x in raw.iloc[0]] if has_h else [""] * raw.shape[1]
    body = raw.iloc[1:] if has_h else raw
    cols, n = [], 0
    for i in range(raw.shape[1]):
        isurl = body[i].astype(str).str.strip().str.startswith("http")
        if isurl.any():
            n += 1; cols.append((i, hdr[i] or f"Document {n}", True))
        else: cols.append((i, hdr[i] or f"Column {i+1}", False))
    return body.reset_index(drop=True), cols

def build(body, cols, res):
    out = {}
    for i, h, u in cols:
        out[h if h not in out else f"{h}_{i}"] = body[i].values
        if u:
            f = lambda v: (res.get(str(v).strip(), {}).get("date") or "NOT FOUND") if str(v).strip().startswith("http") else ""
            out[f"{h} record date"] = [f(v) for v in body[i]]
    return pd.DataFrame(out)

# ---------- UI ----------
dark = st.sidebar.toggle("Dark theme", True)
BG, FG, RED = ("#050505", "#f2f2f2", "#ff1a1a") if dark else ("#ffffff", "#000000", "#000000")
st.markdown(f"""<style>
.stApp{{background:{BG};color:{FG}}} h1,h2,h3,p,label,span,div,li{{color:{FG}}}
[data-testid=stHeader]{{background:transparent}} [data-testid=stSidebar]{{background:{BG};border-right:1px solid {RED}}}
[data-testid=stFileUploader] section,[data-testid=stMetric],.stTextInput input,.stTextArea textarea,[data-testid=stDataFrame],[data-testid=stChatInput],.stAlert,.stDownloadButton button,.stButton button{{
 border:1px solid {RED}!important;border-radius:6px;background:transparent!important;box-shadow:0 0 {10 if dark else 0}px {RED}66}}
[data-testid=stMetric]{{padding:10px 14px}}
.bgx{{position:fixed;inset:0;z-index:0;pointer-events:none;display:flex;align-items:center;justify-content:center;opacity:{.16 if dark else .06}}}
.bgx svg{{width:min(80vh,80vw);animation:spin 40s linear infinite,pulse 5s ease-in-out infinite}}
@keyframes spin{{to{{transform:rotate(360deg)}}}} @keyframes pulse{{50%{{filter:drop-shadow(0 0 22px {RED})}}}}
@media (prefers-reduced-motion:reduce){{.bgx svg{{animation:none}}}}
section.main,.block-container{{position:relative;z-index:1}}
</style><div class="bgx"><svg viewBox="0 0 100 100"><path d="M12 12L88 88M88 12L12 88" stroke="{RED}" stroke-width="7" stroke-linecap="square" fill="none"/>
<path d="M12 12L88 88M88 12L12 88" stroke="{FG}" stroke-width="1" fill="none"/></svg></div>""", unsafe_allow_html=True)

st.title("SCRAP X")
st.caption("Fund document record-date extractor · metadata → content → OCR")
job = st.session_state.get("job")

if not job:
    up = st.file_uploader("Drop your Excel file here or browse (.xlsx / .xls)", type=["xlsx", "xls"])
    with st.expander("Quick check – test a single PDF link"):
        one = st.text_input("PDF URL")
        if one and st.button("Check this link"):
            j = Job([one], {}); j.work(one); r = j.res[one]; st.write({k: v for k, v in r.items() if k != "text"})
    if up:
        body, cols = prepare(up)
        urls, hints = [], {}
        for i, h, u in cols:
            if u:
                for v in body[i].dropna().astype(str).str.strip():
                    if v.startswith("http") and v not in hints: hints[v] = kind_of(h); urls.append(v)
        st.write(f"**{len(body)} rows · {sum(c[2] for c in cols)} document columns · {len(urls)} unique URLs**")
        if len(urls) > MAXURL: st.error(f"Limit is {MAXURL} unique links. Please split the file.")
        elif st.button("Start extraction", type="primary"):
            j = Job(urls, hints); st.session_state.update(job=j, body=body, cols=cols, name=up.name)
            threading.Thread(target=j.run, daemon=True).start(); st.rerun()
else:
    body, cols = st.session_state.body, st.session_state.cols
    n = len(job.urls); el = time.time() - job.t0
    st.progress(job.done / max(n, 1), text=f"{job.status} – {job.done}/{n} · {el/60:.1f} min elapsed")
    if job.paused: st.warning("Internet is down. Work is paused and will resume automatically.")
    ok = [r for r in job.res.values() if r["date"]]; bad = {u: r for u, r in job.res.items() if r["error"] and not r["date"]}
    c = st.columns(6)
    for col, (a, b) in zip(c, [("Rows", len(body)), ("Unique URLs", n), ("Processed", job.done), ("Dates found", len(ok)),
                               ("Errors", len(bad)), ("Low confidence", sum(r["conf"] == "Low" for r in ok))]): col.metric(a, b)
    if job.finished:
        df = build(body, cols, job.res); st.subheader("Result"); st.dataframe(df, use_container_width=True)
        buf = io.BytesIO(); df.to_excel(buf, index=False)
        st.download_button("Download Excel with record dates", buf.getvalue(), "scrapx_output.xlsx")
        if bad: st.subheader("Errors"); st.dataframe(pd.DataFrame([dict(url=u, reason=r["error"]) for u, r in bad.items()]), use_container_width=True)
        st.subheader("Source summary"); st.write(pd.Series([r["source"] for r in ok]).value_counts().to_frame("count"))
        st.subheader("Ask SCRAP X")
        q = st.chat_input("Ask about the documents (single or bulk)")
        if q:
            ctx = "\n\n".join(f"URL: {u}\nType: {r['kind']}\nRecord date: {r['date']}\nText: {r['text'][:1500]}" for u, r in list(job.res.items())[:60])
            try: key = st.secrets["GEMINI_API_KEY"]
            except Exception: key = None
            if not key: st.info("Add GEMINI_API_KEY in app Secrets (free key from aistudio.google.com) to enable AI answers.")
            else:
                p = f"Answer ONLY from the document data below. If not present, say so. Be exact.\n\n{ctx}\n\nQuestion: {q}"
                r = requests.post(f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={key}", json={"contents": [{"parts": [{"text": p}]}]}, timeout=60)
                st.write(r.json().get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", r.text[:300]))
        if st.button("Start new file"): st.session_state.clear(); st.rerun()
    else:
        time.sleep(2); st.rerun()
