"""
AE Deepdive Dashboard — v4 Full Production
========================================================
Features:
  - Upload raw CSV/xlsx → auto-detect months from service_created_at
  - Choose: overwrite / append-new-only per month
  - Run rate for incomplete months
  - Full dashboard: GMV MoM, Category, New User, Store Health, Action List
  - Multi-user online (Supabase storage)
  - Filter by AM, Category, Priority, Search
"""

import streamlit as st
import pandas as pd
import numpy as np
import io, json, calendar, gzip
from datetime import datetime, date
from supabase import create_client

# ─── Page config ─────────────────────────────────────────────────────────────
st.set_page_config(page_title="AE Portfolio Deepdive", page_icon="📊",
                   layout="wide", initial_sidebar_state="expanded")
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Sora:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');

/* ── Base ── */
html, body, [class*="css"] {
  font-family: 'Sora', sans-serif;
  font-size: 13px;
  color: #1a1a1a;
  background: #fafafa;
  -webkit-font-smoothing: antialiased;
}
#MainMenu, footer, header { visibility: hidden }

/* ── Layout ── */
.block-container { padding: 1.5rem 2rem 3rem; max-width: 1480px }
[data-testid="stSidebar"] {
  background: #fff;
  border-right: 1px solid #ebebeb;
}
[data-testid="stSidebar"] * { font-size: 12px }

/* ── Metric cards ── */
[data-testid="metric-container"] {
  background: #fff;
  border: 1px solid #ebebeb;
  border-radius: 8px;
  padding: 1rem 1.25rem;
  transition: border-color .15s;
}
[data-testid="metric-container"]:hover { border-color: #d0d0d0 }
[data-testid="metric-container"] label {
  font-size: 10px !important;
  font-weight: 500 !important;
  letter-spacing: .06em !important;
  text-transform: uppercase !important;
  color: #aaa !important;
}
[data-testid="metric-container"] [data-testid="metric-value"] {
  font-size: 22px !important;
  font-weight: 600 !important;
  letter-spacing: -.02em !important;
  font-family: 'Sora', sans-serif !important;
}
[data-testid="metric-container"] [data-testid="metric-delta"] {
  font-size: 11px !important;
}

/* ── Tabs ── */
[data-testid="stTabs"] button {
  font-size: 11px !important;
  font-weight: 500 !important;
  letter-spacing: .04em !important;
  text-transform: uppercase !important;
  padding: 6px 14px !important;
}
[data-testid="stTabs"] [aria-selected="true"] {
  font-weight: 600 !important;
}

/* ── Buttons ── */
[data-testid="stButton"] button {
  font-family: 'Sora', sans-serif !important;
  font-size: 11px !important;
  font-weight: 500 !important;
  letter-spacing: .02em !important;
  border-radius: 6px !important;
  border: 1px solid #e0e0e0 !important;
  transition: all .12s !important;
}
[data-testid="stButton"] button:hover {
  border-color: #aaa !important;
  background: #f5f5f5 !important;
}
[data-testid="stButton"] button[kind="primary"] {
  border-color: #1a1a1a !important;
  background: #1a1a1a !important;
  color: #fff !important;
}

/* ── Inputs & selects ── */
[data-testid="stSelectbox"] div[data-baseweb="select"] > div,
[data-testid="stTextInput"] input,
[data-testid="stMultiSelect"] div[data-baseweb="select"] > div {
  font-size: 12px !important;
  border-radius: 6px !important;
  border-color: #e0e0e0 !important;
  background: #fff !important;
}

/* ── Dataframe ── */
[data-testid="stDataFrame"] {
  border: 1px solid #ebebeb !important;
  border-radius: 8px !important;
  overflow: hidden !important;
}
.stDataFrame thead tr th {
  font-size: 10px !important;
  font-weight: 600 !important;
  letter-spacing: .06em !important;
  text-transform: uppercase !important;
  color: #888 !important;
  background: #fafafa !important;
  border-bottom: 1px solid #ebebeb !important;
  padding: 8px 10px !important;
}
.stDataFrame tbody tr td {
  font-size: 12px !important;
  padding: 7px 10px !important;
  border-bottom: 1px solid #f5f5f5 !important;
  font-family: 'Sora', sans-serif !important;
}
.stDataFrame tbody tr:last-child td { border-bottom: none !important }
.stDataFrame tbody tr:hover td { background: #fafafa !important }

/* ── Charts ── */
[data-testid="stVegaLiteChart"], [data-testid="stArrowVegaLiteChart"] {
  border-radius: 8px !important;
}

/* ── Section titles ── */
.section-title {
  font-size: 10px;
  font-weight: 600;
  letter-spacing: .10em;
  text-transform: uppercase;
  color: #bbb;
  margin: 1.25rem 0 .6rem;
}

/* ── RR badge ── */
.rr-badge {
  display: inline-block;
  background: #EFF6FF;
  color: #2563EB;
  font-size: 10px;
  font-weight: 500;
  padding: 2px 8px;
  border-radius: 4px;
  margin-left: 8px;
  letter-spacing: .03em;
}

/* ── Priority badges ── */
.bxr { background:#FEF2F2; color:#DC2626; font-size:10px; font-weight:600; padding:2px 8px; border-radius:4px; letter-spacing:.03em }
.bxa { background:#FFFBEB; color:#D97706; font-size:10px; font-weight:600; padding:2px 8px; border-radius:4px; letter-spacing:.03em }
.bxg { background:#F0FDF4; color:#16A34A; font-size:10px; font-weight:600; padding:2px 8px; border-radius:4px; letter-spacing:.03em }

/* ── Expander ── */
[data-testid="stExpander"] {
  border: 1px solid #ebebeb !important;
  border-radius: 8px !important;
}
[data-testid="stExpander"] summary {
  font-size: 12px !important;
  font-weight: 500 !important;
}

/* ── Info / warning boxes ── */
[data-testid="stInfo"], [data-testid="stAlert"] {
  border-radius: 6px !important;
  font-size: 12px !important;
}

/* ── Mono numbers ── */
.num { font-family: 'JetBrains Mono', monospace; font-size: 12px }

/* ── Divider ── */
hr { border: none; border-top: 1px solid #ebebeb; margin: 1rem 0 }
</style>
""", unsafe_allow_html=True)

# ─── Constants ────────────────────────────────────────────────────────────────
REAL_AMS = {
    # Core AMs
    "Mameaw","Chertam","Geem","Wan","Pui","Fah KAM","Seeiw","Amm",
    "Nahm","Aum","Get KAM","Puinoon",
    # Additional AMs
    "THINEEKARN BD","Samir","Lanna","Kittitut BD",
    "Pym","Femille","Pop SSU","Amm Junior","Sasithorn BD",
    "Peerawat Dangsupa","Amy","Muk BD","Phornphannarai",
    # Accounts included per request
    "UN", "VPM", "SSU", "No owner",
    "Temporarily closed", "Permanently closed", "End Contract ",
    "Invalid",
}
EXCLUDED  = {"cancelled","refunded","expired","no_show"}
PILLAR_COLS  = ["sku_score","price_score","view_score","cvr_score"]
PILLAR_NAMES = ["Price","View MoM","CVR MoM"]
MONTH_LABELS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

# ─── Supabase ─────────────────────────────────────────────────────────────────
@st.cache_resource
def get_sb():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

def db_ensure_table():
    """Create dashboard_kv table if not exists via RPC or direct insert check."""
    try:
        get_sb().table("dashboard_kv").select("key").limit(1).execute()
    except Exception:
        pass  # Table exists or will be created on first insert

def sb_upload(key: str, data_str: str):
    """Store data in Supabase database table (key-value)."""
    import base64
    compressed = gzip.compress(data_str.encode("utf-8"), compresslevel=6)
    b64 = base64.b64encode(compressed).decode("utf-8")
    try:
        get_sb().table("dashboard_kv").upsert({
            "key": key,
            "value": b64,
            "updated_at": datetime.now().isoformat(),
        }, on_conflict="key").execute()
    except Exception as e:
        err_msg = str(e)
        if "relation" in err_msg.lower() or "does not exist" in err_msg.lower() or "42p01" in err_msg.lower():
            raise RuntimeError(
                "❌ Table 'dashboard_kv' ยังไม่ได้สร้างใน Supabase\n\n"
                "กรุณาไปที่ Supabase → SQL Editor แล้วรัน SQL นี้:\n\n"
                "create table if not exists dashboard_kv (\n"
                "  key text primary key,\n"
                "  value text not null,\n"
                "  updated_at timestamptz default now()\n"
                ");\n"
                "alter table dashboard_kv enable row level security;\n"
                "create policy \"allow all\" on dashboard_kv\n"
                "  for all using (true) with check (true);"
            ) from e
        raise RuntimeError(f"Supabase error: {err_msg}") from e

def sb_download(key: str) -> bytes:
    """Load data from Supabase database table."""
    import base64
    res = get_sb().table("dashboard_kv").select("value").eq("key", key).single().execute()
    b64 = res.data["value"]
    compressed = base64.b64decode(b64)
    return gzip.decompress(compressed)

def sb_delete(key: str):
    """Delete a key from database."""
    get_sb().table("dashboard_kv").delete().eq("key", key).execute()

# ─── Comment system ──────────────────────────────────────────────────────────
def save_comment(shop_id: str, month: str, kam_name: str, text: str, status: str,
                  task_type: str = "", due_date: str = ""):
    """Save a comment for a shop. Key = comments_{shop_id}_{month}"""
    key = f"comments_{shop_id}_{month}"
    try:
        raw = sb_download(key)
        existing = json.loads(raw)
    except Exception:
        existing = []
    existing.append({
        "kam":       kam_name,
        "text":      text,
        "status":    status,
        "task_type": task_type,
        "due_date":  due_date,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
    })
    sb_upload(key, json.dumps(existing, ensure_ascii=False))

def load_comments(shop_id: str, month: str) -> list:
    """Load comments for a shop."""
    key = f"comments_{shop_id}_{month}"
    try:
        raw = sb_download(key)
        return json.loads(raw)
    except Exception:
        return []

def load_all_comments_for_month(month: str) -> dict:
    """Load all comments for a month. Returns {shop_id: [comments]}"""
    try:
        res = get_sb().table("dashboard_kv").select("key,value").like("key", f"comments_%_{month}").execute()
        result = {}
        for row in res.data:
            shop_id = row["key"].replace(f"comments_","").replace(f"_{month}","")
            try:
                import gzip as _gz, base64
                compressed = base64.b64decode(row["value"])
                data = json.loads(_gz.decompress(compressed))
                result[shop_id] = data
            except Exception:
                result[shop_id] = []
        return result
    except Exception:
        return {}

@st.cache_data(ttl=30)
def load_all_comments_cached(month: str) -> dict:
    return load_all_comments_for_month(month)


# ─── Run Rate calculation ─────────────────────────────────────────────────────
def compute_run_rate(df_month: pd.DataFrame, period: pd.Period) -> dict:
    """
    Returns dict with actual GMV, run-rate GMV, coverage%, is_complete.
    Run rate = actual_gmv / (days_with_data / days_in_month)
    """
    days_in_month = period.days_in_month
    if df_month.empty:
        return {"actual": 0, "run_rate": 0, "coverage": 0, "is_complete": False, "days": 0}

    first_day = period.start_time
    last_day  = df_month["service_created_at"].max()
    days_with_data = max(1, (last_day - first_day).days + 1)
    coverage   = days_with_data / days_in_month
    is_complete = days_with_data >= days_in_month - 1
    actual_gmv = df_month["gmv"].sum()
    run_rate   = actual_gmv / coverage

    return {
        "actual":      round(actual_gmv),
        "run_rate":    round(run_rate),
        "coverage":    round(coverage * 100, 1),
        "is_complete": is_complete,
        "days":        days_with_data,
        "days_in_month": days_in_month,
    }


# ─── Core processing ─────────────────────────────────────────────────────────
def process_raw(file_bytes: bytes, view_bytes: bytes | None = None) -> dict:
    """
    Process raw transaction file → returns dict keyed by month string (YYYY-MM).
    Each month gets: shop_scores, am_summary, monthly_stats, run_rate_info.
    Also returns cross-month trend data.
    """
    # ── Load & clean ──────────────────────────────────────────────────────
    try:
        df = pd.read_csv(io.BytesIO(file_bytes), low_memory=False)
    except Exception:
        df = pd.read_excel(io.BytesIO(file_bytes), engine="openpyxl")

    df = df[df["kam"].apply(lambda x: isinstance(x, str) and x in REAL_AMS)]
    df = df[~df["order_status"].isin(EXCLUDED)]
    df = df.drop_duplicates(subset=["booking_id","sku_id"], keep="first")

    df["service_created_at"] = pd.to_datetime(df["service_created_at"], errors="coerce")
    df["month_period"] = df["service_created_at"].dt.to_period("M")

    for c in ["gmv","selling_price","original_price","lowest_price_12m"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["is_new"]    = df["is_first_booking"].isin([True,"TRUE","true","True",1])
    df["shop_id_s"] = df["shop_id"].astype(str).str.replace(".0","",regex=False).str.strip()

    # ── View/CR file — keep per-month for MoM comparison ─────────────────────
    view_map = {}        # shop_id → {avg_view, avg_cr}         (latest month)
    view_map_by_month = {}  # "YYYY-MM" → {shop_id → {view, cr}} (all months)
    if view_bytes:
        vdf = pd.read_csv(io.BytesIO(view_bytes))
        vdf["shop_id"] = vdf["shop_id"].astype(str).str.strip()
        vdf_dedup = vdf.drop_duplicates(subset="shop_id", keep="first")

        # Detect all month columns (e.g. "2026_Mar User-View", "2026-Mar CR%")
        import re
        month_cols = {}  # "2026-03" → {"view": col, "cr": col}
        for col in vdf.columns:
            m = re.search(r'(\d{4})[_-](Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)', col)
            if not m: continue
            yr, mo_str = m.group(1), m.group(2)
            mo_map = {"Jan":"01","Feb":"02","Mar":"03","Apr":"04","May":"05","Jun":"06",
                      "Jul":"07","Aug":"08","Sep":"09","Oct":"10","Nov":"11","Dec":"12"}
            mk = f"{yr}-{mo_map[mo_str]}"
            if mk not in month_cols: month_cols[mk] = {}
            if "View" in col:  month_cols[mk]["view"] = col
            elif "CR%" in col and "growth" not in col.lower(): month_cols[mk]["cr"] = col

        sorted_mks = sorted(month_cols.keys())
        for mk, cols in month_cols.items():
            if "view" not in cols: continue
            vdf[cols["view"]] = pd.to_numeric(vdf[cols["view"]], errors="coerce").fillna(0)
            cr_col = cols.get("cr")
            if cr_col: vdf[cr_col] = pd.to_numeric(vdf[cr_col], errors="coerce").fillna(0)
            view_map_by_month[mk] = vdf_dedup.set_index("shop_id").apply(
                lambda row: {
                    "view": float(row.get(cols["view"], 0) or 0),
                    "cr":   float(row.get(cr_col, 0) or 0) * 100 if cr_col else 0.0
                }, axis=1
            ).to_dict()

        # Latest available month as default
        if sorted_mks:
            last_mk = sorted_mks[-1]
            view_map = {sid: {"avg_view": v["view"], "avg_cr": v["cr"]}
                        for sid, v in view_map_by_month.get(last_mk, {}).items()}

    # ── Process each month ────────────────────────────────────────────────
    months_found = sorted(df["month_period"].dropna().unique())
    result = {"months": {}, "trend": {}}

    for period in months_found:
        mkey = str(period)   # "2026-01"
        mdf  = df[df["month_period"] == period].copy()
        rr   = compute_run_rate(mdf, period)

        # Shop aggregation
        agg = mdf.groupby(["shop_id_s","organization_name","kam"]).agg(
            total_orders        = ("booking_id","count"),
            gmv                 = ("gmv","sum"),
            sku_count           = ("sku_id","nunique"),
            selling_price_mean  = ("selling_price","mean"),
            original_price_mean = ("original_price","mean"),
            lowest_price_12m    = ("lowest_price_12m","mean"),
            unique_customers    = ("user_id","nunique"),
            new_customers       = ("is_new","sum"),
            category            = ("category","first"),
        ).reset_index()

        agg["price_above"] = ((agg["selling_price_mean"]-agg["lowest_price_12m"])/agg["lowest_price_12m"].replace(0,np.nan)*100).round(1).fillna(0)
        agg["repeat_rate"] = ((agg["unique_customers"]-agg["new_customers"])/agg["unique_customers"].replace(0,np.nan)*100).round(1).fillna(0)
        agg["opc"]         = (agg["total_orders"]/agg["unique_customers"].replace(0,np.nan)).round(2).fillna(1)
        agg["gmv"]         = agg["gmv"].round(0).astype(int)

        # ── Service-level price data (for action list detail) ──────────────
        # Find top 3 services per shop that are most overpriced vs lowest_12m
        svc_price = mdf.groupby(["shop_id_s","service_name"]).agg(
            svc_sell  = ("selling_price",  "mean"),
            svc_low12 = ("lowest_price_12m","mean"),
            svc_gmv   = ("gmv",            "sum"),
        ).reset_index()
        svc_price["svc_sell"]  = svc_price["svc_sell"].round(0)
        svc_price["svc_low12"] = svc_price["svc_low12"].round(0)
        svc_price["svc_pct"]   = ((svc_price["svc_sell"]-svc_price["svc_low12"])/svc_price["svc_low12"].replace(0,np.nan)*100).round(0).fillna(0)
        svc_price = svc_price[svc_price["svc_pct"] > 10]  # only overpriced services

        # Build map: shop_id → list of top 3 overpriced services
        overpriced_map = {}
        for sid, grp in svc_price.sort_values("svc_pct", ascending=False).groupby("shop_id_s"):
            top3 = grp.head(3)[["service_name","svc_sell","svc_low12","svc_pct"]].to_dict("records")
            overpriced_map[sid] = top3

        agg["overpriced_svcs"] = agg["shop_id_s"].map(lambda x: overpriced_map.get(x, []))

        # Merge view data — use this month's data specifically, fallback to avg
        use_real = False
        if view_map_by_month:
            # Use current month's view data if available, else avg
            cur_view_for_month = view_map_by_month.get(mkey, {})
            if cur_view_for_month:
                agg["avg_view"] = agg["shop_id_s"].map(lambda x: cur_view_for_month.get(x,{}).get("view",0)).fillna(0)
                agg["avg_cr"]   = agg["shop_id_s"].map(lambda x: cur_view_for_month.get(x,{}).get("cr",0)).fillna(0)
            elif view_map:
                agg["avg_view"] = agg["shop_id_s"].map(lambda x: view_map.get(x,{}).get("avg_view",0)).fillna(0)
                agg["avg_cr"]   = agg["shop_id_s"].map(lambda x: view_map.get(x,{}).get("avg_cr",0)).fillna(0)
            else:
                agg["avg_view"] = 0.0
                agg["avg_cr"]   = 0.0
            use_real = (agg["avg_view"] > 0).sum() > 50
        elif view_map:
            agg["avg_view"] = agg["shop_id_s"].map(lambda x: view_map.get(x,{}).get("avg_view",0)).fillna(0)
            agg["avg_cr"]   = agg["shop_id_s"].map(lambda x: view_map.get(x,{}).get("avg_cr",0)).fillna(0)
            use_real = (agg["avg_view"] > 0).sum() > 50
        else:
            agg["avg_view"] = 0.0
            agg["avg_cr"]   = 0.0

        # 4 Pillar scores (Operation removed)
        def pr(s): return s.rank(pct=True).mul(100).round(0)
        agg["sku_score"]   = agg.groupby("category")["sku_count"].transform(lambda x: x.rank(pct=True)*100).round(0)
        agg["price_score"] = (100-(agg["price_above"].clip(0,30)/30*100)).round(0).clip(0,100)

        # View MoM: compare each shop vs its own previous month
        # Previous month key
        sorted_mks_list = sorted(view_map_by_month.keys()) if view_map_by_month else []
        prev_mk_idx = sorted_mks_list.index(mkey)-1 if mkey in sorted_mks_list and sorted_mks_list.index(mkey)>0 else -1
        prev_view_mk = sorted_mks_list[prev_mk_idx] if prev_mk_idx >= 0 else None

        if use_real and prev_view_mk:
            prev_view_data = view_map_by_month.get(prev_view_mk, {})
            cur_view_data  = view_map_by_month.get(mkey, {})
            # Run rate coverage for current month — scale up partial month
            _cov = rr["coverage"] / 100 if rr["coverage"] > 0 else 1.0
            _is_partial = not rr["is_complete"]
            def view_mom_score(shop_id):
                cur_raw = cur_view_data.get(shop_id,  {}).get("view", 0)
                prev    = prev_view_data.get(shop_id, {}).get("view", 0)
                # Apply run rate to current month view
                cur = cur_raw / _cov if _is_partial and _cov > 0 and cur_raw > 0 else cur_raw
                if prev <= 0 and cur <= 0: return 50.0
                if prev <= 0: return 75.0
                pct = (cur - prev) / prev * 100
                return float(np.clip(50 + pct, 0, 100))
            def cvr_mom_score(shop_id):
                # CVR is a rate — no run rate needed
                cur  = cur_view_data.get(shop_id,  {}).get("cr", 0)
                prev = prev_view_data.get(shop_id, {}).get("cr", 0)
                if prev <= 0 and cur <= 0: return 50.0
                if prev <= 0: return 75.0
                pct = (cur - prev) / prev * 100
                return float(np.clip(50 + pct, 0, 100))
            agg["view_score"] = agg["shop_id_s"].map(view_mom_score).fillna(50).round(0)
            agg["cvr_score"]  = agg["shop_id_s"].map(cvr_mom_score).fillna(50).round(0)
            # Store MoM % for display in action list
            agg["view_mom_pct"] = agg["shop_id_s"].map(lambda sid: round(
                (cur_view_data.get(sid,{}).get("view",0) - view_map_by_month.get(prev_view_mk,{}).get(sid,{}).get("view",0))
                / max(view_map_by_month.get(prev_view_mk,{}).get(sid,{}).get("view",1),1)*100, 1))
            agg["cvr_mom_pct"]  = agg["shop_id_s"].map(lambda sid: round(
                (cur_view_data.get(sid,{}).get("cr",0) - view_map_by_month.get(prev_view_mk,{}).get(sid,{}).get("cr",0))
                / max(view_map_by_month.get(prev_view_mk,{}).get(sid,{}).get("cr",0.01),0.01)*100, 1))
        elif use_real:
            # No prev month in view file → use percentile rank
            agg["view_score"]   = pr(agg["avg_view"])
            agg["cvr_score"]    = pr(agg["avg_cr"])
            agg["view_mom_pct"] = 0.0
            agg["cvr_mom_pct"]  = 0.0
        else:
            # No view file → proxy
            agg["view_score"]   = agg.groupby("category")["total_orders"].transform(lambda x: x.rank(pct=True)*100).round(0)
            agg["cvr_score"]    = pr(agg["opc"])
            agg["view_mom_pct"] = 0.0
            agg["cvr_mom_pct"]  = 0.0

        agg["health_score"] = agg[PILLAR_COLS].mean(axis=1).round(1)
        agg["priority"]     = pd.cut(agg["health_score"],bins=[0,40,60,100],labels=["critical","warning","healthy"]).astype(str)

        def mk_alerts(r):
            a=[]
            if r["sku_score"]  <30: a.append(f"SKU น้อย ({int(r['sku_count'])} SKUs)")
            if r["price_score"]<50: a.append(f"ราคาสูงกว่า lowest +{r['price_above']:.0f}%")
            if r["view_score"] <40:
                mom = r.get("view_mom_pct", 0)
                if use_real and mom != 0:
                    a.append(f"View ลด {abs(mom):.0f}% MoM ({int(r['avg_view'])} views)")
                elif use_real:
                    a.append(f"View ต่ำ ({int(r['avg_view'])} views)")
                else:
                    a.append(f"Volume ต่ำ ({int(r['total_orders'])} orders)")
            if r["cvr_score"]  <40:
                mom = r.get("cvr_mom_pct", 0)
                if use_real and mom != 0:
                    a.append(f"CR% ลด {abs(mom):.0f}% MoM ({r['avg_cr']:.2f}%)")
                elif use_real:
                    a.append(f"CR% ต่ำ ({r['avg_cr']:.2f}%)")
                else:
                    a.append(f"Orders/cust ต่ำ ({r['opc']:.1f}x)")
            return " | ".join(a)

        agg["alerts"]      = agg.apply(mk_alerts, axis=1)
        agg["alert_count"] = agg["alerts"].apply(lambda x: len(x.split(" | ")) if x else 0)

        # AM summary
        # Per-AM stats from transaction data (accurate unique counts)
        am_tx = mdf.groupby("kam").agg(
            unique_customers = ("user_id",         "nunique"),
            new_customers    = ("is_new",           "sum"),
            total_orders     = ("booking_id",       "nunique"),
        ).reset_index()

        am = agg.groupby("kam").agg(
            shops          = ("shop_id_s","count"),
            gmv            = ("gmv","sum"),
            critical_shops = ("priority", lambda x:(x=="critical").sum()),
            warning_shops  = ("priority", lambda x:(x=="warning").sum()),
            avg_health     = ("health_score","mean"),
            avg_sku        = ("sku_score","mean"),
            avg_price      = ("price_score","mean"),
            avg_view       = ("view_score","mean"),
            avg_cvr        = ("cvr_score","mean"),
            avg_view_mom   = ("view_mom_pct","mean"),
            avg_cvr_mom    = ("cvr_mom_pct","mean"),
            total_alerts   = ("alert_count","sum"),
            avg_page_view  = ("avg_view","mean"),
            avg_cr_pct     = ("avg_cr","mean"),
        ).reset_index().round(1)
        am["gmv"] = am["gmv"].astype(int)
        # Merge accurate unique counts from transaction data
        am = am.merge(am_tx, on="kam", how="left")
        am["unique_customers"] = am["unique_customers"].fillna(0).astype(int)
        am["new_customers"]    = am["new_customers"].fillna(0).astype(int)
        am["total_orders"]     = am["total_orders"].fillna(0).astype(int)
        am["basket_size"]      = (am["gmv"] / am["unique_customers"].replace(0,1)).round(0).astype(int)
        # total_page_view = SUM of avg_view per shop (not mean of mean)
        am_view_sum = agg.groupby("kam")["avg_view"].sum().round(0).astype(int).reset_index()
        am_view_sum.columns = ["kam","total_page_view"]
        am = am.merge(am_view_sum, on="kam", how="left")
        am["total_page_view"] = am["total_page_view"].fillna(0).astype(int)

        # Category stats
        cat = mdf.groupby("category").agg(
            gmv=("gmv","sum"), orders=("booking_id","nunique"),
            new_customers=("is_new","sum"), unique_customers=("user_id","nunique"),
        ).reset_index()
        cat["gmv"] = cat["gmv"].round(0).astype(int)

        # Monthly stats
        # Which months have view data
        has_view_for_month = mkey in view_map_by_month if view_map_by_month else False

        monthly_stat = {
            "month":           mkey,
            "label":           MONTH_LABELS[period.month-1] + " " + str(period.year),
            "gmv":             rr["actual"],
            "gmv_run_rate":    rr["run_rate"],
            "is_complete":     rr["is_complete"],
            "coverage_pct":    rr["coverage"],
            "days":            rr["days"],
            "days_in_month":   rr["days_in_month"],
            "orders":          int(mdf["booking_id"].nunique()),
            "unique_customers":int(mdf["user_id"].nunique()),
            "new_customers":   int(mdf["is_new"].sum()),
            "use_real_view":   use_real,
            "has_view_data":   has_view_for_month,
        }

        # Build full service-level price table for Price pillar drill-down
        svc_all = mdf.groupby(["shop_id_s","organization_name","kam","category","service_name"]).agg(
            sell  = ("selling_price",   "mean"),
            low12 = ("lowest_price_12m","mean"),
            gmv   = ("gmv",             "sum"),
            orders= ("booking_id",      "count"),
        ).reset_index()
        svc_all["sell"]  = svc_all["sell"].round(0)
        svc_all["low12"] = svc_all["low12"].round(0)
        svc_all["pct"]   = ((svc_all["sell"]-svc_all["low12"])/svc_all["low12"].replace(0,np.nan)*100).round(1).fillna(0)
        svc_all["gmv"]   = svc_all["gmv"].round(0).astype(int)
        svc_price_table  = svc_all[svc_all["pct"] > 0].sort_values("pct", ascending=False)

        # Full service performance (all services, not just overpriced)
        # Rebuild svc_perf with richer price stats
        svc_perf = mdf.groupby(["shop_id_s","organization_name","kam","category","service_name"]).agg(
            gmv    = ("gmv",            "sum"),
            orders = ("booking_id",     "count"),
            sell_mean = ("selling_price","mean"),
            sell_min  = ("selling_price","min"),
            sell_max  = ("selling_price","max"),
            low12  = ("lowest_price_12m","mean"),
        ).reset_index()
        svc_perf["sell_mean"] = svc_perf["sell_mean"].round(0)
        svc_perf["sell_min"]  = svc_perf["sell_min"].round(0)
        svc_perf["sell_max"]  = svc_perf["sell_max"].round(0)
        svc_perf["low12"]     = svc_perf["low12"].round(0)
        svc_perf["gmv"]       = svc_perf["gmv"].round(0).astype(int)
        svc_perf["basket"]    = (svc_perf["gmv"] / svc_perf["orders"].replace(0,np.nan)).round(0).fillna(0).astype(int)
        # Keep sell as mean for backward compat
        svc_perf["sell"]      = svc_perf["sell_mean"]
        svc_perf["pct"]       = ((svc_perf["sell_mean"]-svc_perf["low12"])/svc_perf["low12"].replace(0,np.nan)*100).round(1).fillna(0)

        # Shop SKU concentration: % GMV from top-1 SKU per shop
        shop_top_sku = svc_all.sort_values("gmv",ascending=False).groupby("shop_id_s").first().reset_index()[["shop_id_s","service_name","gmv"]]
        shop_top_sku.columns = ["shop_id_s","top_sku","top_sku_gmv"]
        agg2 = agg.merge(shop_top_sku, on="shop_id_s", how="left")
        agg2["top_sku_pct"] = (agg2["top_sku_gmv"] / agg2["gmv"].replace(0,np.nan)*100).round(1).fillna(0)
        # Category avg SKU
        cat_avg_sku = agg.groupby("category")["sku_count"].mean().round(1).to_dict()
        agg2["cat_avg_sku"] = agg2["category"].map(cat_avg_sku).round(1)
        agg2["sku_gap"] = (agg2["cat_avg_sku"] - agg2["sku_count"]).round(1)

        # Demographics — store per-KAM for filtering at display time
        mdf3 = mdf.copy()
        if "age" in mdf3.columns:
            mdf3["age"] = pd.to_numeric(mdf3["age"], errors="coerce")
            mdf3["age_group"] = pd.cut(mdf3["age"],
                bins=[0,24,34,44,54,200],
                labels=["<25","25–34","35–44","45–54","55+"]
            ).astype(str)
            mdf3.loc[mdf3["age_group"].isin(["nan","<NA>"]),"age_group"] = "Unknown"

        def agg_demo(df_sub):
            d = {}
            if "gender" in df_sub.columns:
                g = df_sub.groupby("gender")["gmv"].sum().reset_index()
                g["gmv"] = g["gmv"].round(0).astype(int)
                d["gender"] = g.to_dict("records")
            if "age_group" in df_sub.columns:
                ag = df_sub[df_sub["age_group"]!="Unknown"].groupby("age_group")["gmv"].sum().reset_index()
                ag["gmv"] = ag["gmv"].round(0).astype(int)
                d["age"] = ag.to_dict("records")
            if "payment_type" in df_sub.columns:
                pt = df_sub.groupby("payment_type")["gmv"].sum().reset_index()
                pt["gmv"] = pt["gmv"].round(0).astype(int)
                d["payment"] = pt.to_dict("records")
            if "category" in df_sub.columns:
                ct = df_sub.groupby("category")["gmv"].sum().reset_index()
                ct["gmv"] = ct["gmv"].round(0).astype(int)
                d["category"] = ct.to_dict("records")
            if "subcategory" in df_sub.columns:
                sc_df = df_sub.groupby("subcategory")["gmv"].sum().nlargest(10).reset_index()
                sc_df["gmv"] = sc_df["gmv"].round(0).astype(int)
                d["subcategory"] = sc_df.to_dict("records")
            return d

        # Total + per-KAM
        demo = {"all": agg_demo(mdf3)}
        for kam_name, kam_df in mdf3.groupby("kam"):
            demo[kam_name] = agg_demo(kam_df)

        result["months"][mkey] = {
            "shops":       agg2[list(agg.columns)+["top_sku","top_sku_pct","cat_avg_sku","sku_gap"]].to_dict("records"),
            "am":          am.to_dict("records"),
            "category":    cat.to_dict("records"),
            "stats":       monthly_stat,
            "demo":        demo,
            "svc_price":   svc_price_table[["shop_id_s","organization_name","kam","category",
                                             "service_name","sell","low12","pct","gmv","orders"]].to_dict("records"),
            "svc_perf":    svc_perf[["shop_id_s","organization_name","kam","category",
                                      "service_name","gmv","orders","basket",
                                      "sell","sell_min","sell_max","low12","pct"]].to_dict("records"),
        }

    # ── Cross-month trend: KAM ────────────────────────────────────────────
    kam_trend = df.groupby(["kam","month_period"])["gmv"].sum().reset_index()
    kam_trend["month"] = kam_trend["month_period"].astype(str)
    kam_trend["gmv"]   = kam_trend["gmv"].round(0).astype(int)
    result["trend"]["kam"] = kam_trend[["kam","month","gmv"]].to_dict("records")

    # ── Cross-month trend: Top shops ──────────────────────────────────────
    shop_top = df.groupby("organization_name")["gmv"].sum().nlargest(30).index
    sm = df[df["organization_name"].isin(shop_top)].groupby(["organization_name","month_period"])["gmv"].sum().reset_index()
    sm["month"] = sm["month_period"].astype(str)
    sm["kam"]   = df.groupby("organization_name")["kam"].first().reindex(sm["organization_name"]).values
    sm["gmv"]   = sm["gmv"].round(0).astype(int)
    result["trend"]["shops"] = sm[["organization_name","kam","month","gmv"]].to_dict("records")

    # ── Cross-month trend: Top services ──────────────────────────────────
    # Store all services (no limit) for GMV by month table
    sv = df.groupby(["service_name","month_period"])["gmv"].sum().reset_index()
    sv["month"] = sv["month_period"].astype(str)
    sv["gmv"]   = sv["gmv"].round(0).astype(int)
    result["trend"]["services"] = sv[["service_name","month","gmv"]].to_dict("records")

    # ── Cross-month trend: Category ───────────────────────────────────────
    cm = df.groupby(["category","month_period"]).agg(gmv=("gmv","sum"),orders=("booking_id","nunique"),new=("is_new","sum"),customers=("user_id","nunique")).reset_index()
    cm["month"] = cm["month_period"].astype(str)
    cm["gmv"]   = cm["gmv"].round(0).astype(int)
    result["trend"]["category"] = cm[["category","month","gmv","orders","new","customers"]].to_dict("records")

    # ── Summary of months found ───────────────────────────────────────────
    result["month_keys"]  = sorted(result["months"].keys())
    result["upload_time"] = datetime.now().strftime("%Y-%m-%d %H:%M")

    return result


# ─── Supabase index helpers ───────────────────────────────────────────────────
def load_index() -> dict:
    """Global index: {month_key: {label, upload_time, stats}}"""
    try:
        raw = sb_download("index.json")
        return json.loads(raw)
    except:
        return {}

def save_index(idx: dict):
    sb_upload("index.json", json.dumps(idx, ensure_ascii=False, default=str))

@st.cache_data(ttl=60)
def load_index_cached():
    return load_index()

@st.cache_data(ttl=60)
def load_month_data(mkey: str):
    try:
        raw = sb_download(f"month_{mkey}.json")
        return json.loads(raw)
    except:
        return None

@st.cache_data(ttl=60)
def load_trend_data():
    try:
        raw = sb_download("trend.json")
        return json.loads(raw)
    except:
        return {}

def save_month(mkey: str, data: dict):
    sb_upload(f"month_{mkey}.json", json.dumps(data, ensure_ascii=False, default=str))

def save_trend(data: dict):
    sb_upload("trend.json", json.dumps(data, ensure_ascii=False, default=str))


# ─── Helpers ──────────────────────────────────────────────────────────────────
def sc(v): return "#E24B4A" if v<40 else "#EF9F27" if v<60 else "#639922"
def fmt_gmv(v, rr=False):
    try: v = float(v)
    except: return "–"
    s = f"฿{int(v):,}"
    if rr: s += " (RR)"
    return s
def to_csv(df): return df.to_csv(index=False,encoding="utf-8-sig").encode("utf-8-sig")
def css(v):
    try: return f"color:{sc(float(v))};font-weight:500"
    except: return ""
def cprio(v):
    return {"critical":"background:#FCEBEB;color:#A32D2D","warning":"background:#FAEEDA;color:#854F0B","healthy":"background:#EAF3DE;color:#3B6D11"}.get(str(v),"")


# ─── Sidebar ──────────────────────────────────────────────────────────────────
is_admin = True  # Admin panel always visible — protected by password
idx      = load_index_cached()

with st.sidebar:
    st.markdown("### 📊 AE Portfolio")
    st.markdown("**Deepdive**")
    st.markdown("---")

    # ── Admin panel ──────────────────────────────────────────────────────
    st.markdown("#### 🔐 Upload Data")
    pw = st.text_input("Password", type="password", placeholder="ใส่ password เพื่อ upload")

    if pw == st.secrets.get("ADMIN_PASSWORD","gowabi2024"):
            st.success("✓ Authenticated")

            with st.expander("📤 Upload Raw Data", expanded=True):
                st.markdown("**ไฟล์ที่ 1 — Transaction (csv/xlsx)** ✱")
                tx_file   = st.file_uploader("Transaction file", type=["csv","xlsx"], key="tx")
                st.markdown("**ไฟล์ที่ 2 — View/CR (csv)** — ไม่บังคับ")
                view_file = st.file_uploader("View/Conversion csv", type=["csv"], key="view")

                if tx_file:
                    # Preview months in file
                    with st.spinner("วิเคราะห์ไฟล์…"):
                        try:
                            tmp = pd.read_csv(io.BytesIO(tx_file.read()), usecols=["service_created_at","kam"], low_memory=False)
                            tx_file.seek(0)
                            tmp["ts"] = pd.to_datetime(tmp["service_created_at"], errors="coerce")
                            tmp = tmp[tmp["kam"].isin(REAL_AMS)]
                            found_months = sorted(tmp["ts"].dt.to_period("M").dropna().unique())
                            st.info(f"พบข้อมูล **{len(found_months)} เดือน** ในไฟล์: " +
                                    ", ".join([MONTH_LABELS[p.month-1]+f" {p.year}" for p in found_months]))
                        except:
                            found_months = []
                            st.warning("ไม่สามารถ preview ได้ — จะ process ทั้งไฟล์")

                    # Upload mode
                    st.markdown("**โหมด Upload**")
                    upload_mode = st.radio(
                        "",
                        ["🔄 Overwrite — แทนที่ข้อมูลเดือนที่มีอยู่ทั้งหมด",
                         "➕ Append — เพิ่มเฉพาะเดือนที่ยังไม่มีข้อมูล"],
                        label_visibility="collapsed"
                    )
                    is_overwrite = upload_mode.startswith("🔄")

                    # Show which months will be affected
                    if found_months and idx:
                        existing = set(idx.keys())
                        new_months = [str(m) for m in found_months if str(m) not in existing]
                        exist_months = [str(m) for m in found_months if str(m) in existing]
                        if is_overwrite and exist_months:
                            st.warning(f"⚠️ จะ overwrite: {', '.join([MONTH_LABELS[int(m.split('-')[1])-1]+' '+m.split('-')[0] for m in exist_months])}")
                        if new_months:
                            st.success(f"✓ เพิ่มใหม่: {', '.join([MONTH_LABELS[int(m.split('-')[1])-1]+' '+m.split('-')[0] for m in new_months])}")
                        if not is_overwrite and not new_months:
                            st.info("ข้อมูลทุกเดือนในไฟล์นี้มีอยู่แล้ว")

                if tx_file and st.button("🚀 Process & Upload", type="primary", use_container_width=True):
                    with st.spinner("Processing… อาจใช้เวลา 1–3 นาที"):
                        tx_file.seek(0)
                        result = process_raw(
                            tx_file.read(),
                            view_file.read() if view_file else None
                        )

                    # Save months
                    idx = load_index()
                    saved, skipped = [], []
                    for mkey, mdata in result["months"].items():
                        if not is_overwrite and mkey in idx:
                            skipped.append(mkey)
                            continue
                        save_month(mkey, mdata)
                        idx[mkey] = {
                            "label":       mdata["stats"]["label"],
                            "upload_time": result["upload_time"],
                            "stats":       mdata["stats"],
                        }
                        saved.append(mkey)

                    # Save trend (always update)
                    if result.get("trend"):
                        save_trend(result["trend"])

                    save_index(idx)
                    load_index_cached.clear()
                    load_month_data.clear()
                    load_trend_data.clear()

                    if saved:
                        mlabels = [MONTH_LABELS[int(m.split('-')[1])-1]+' '+m.split('-')[0] for m in sorted(saved)]
                        st.success(f"✓ บันทึกแล้ว: {', '.join(mlabels)}")
                    if skipped:
                        mlabels = [MONTH_LABELS[int(m.split('-')[1])-1]+' '+m.split('-')[0] for m in sorted(skipped)]
                        st.info(f"ข้ามเพราะมีอยู่แล้ว: {', '.join(mlabels)}")
                    st.balloons()

            with st.expander("🗄️ จัดการข้อมูล"):
                idx2 = load_index_cached()
                if not idx2:
                    st.caption("ยังไม่มีข้อมูล")
                else:
                    st.caption(f"มี **{len(idx2)} เดือน** ใน Supabase")
                    for mkey in sorted(idx2.keys(), reverse=True):
                        info = idx2[mkey]
                        stats = info.get("stats",{})
                        rr_str = "" if stats.get("is_complete") else f" — RR ฿{stats.get('gmv_run_rate',0)/1e6:.1f}M"
                        c1,c2 = st.columns([3,1])
                        c1.markdown(f"**{info['label']}** {rr_str}")
                        c1.caption(f"{stats.get('days',0)}/{stats.get('days_in_month',30)} days · อัพ {info['upload_time']}")
                        if c2.button("🗑️", key=f"del_{mkey}"):
                            st.session_state[f"confirm_{mkey}"] = True
                        if st.session_state.get(f"confirm_{mkey}"):
                            st.warning(f"ยืนยันลบ {info['label']}?")
                            cc1,cc2 = st.columns(2)
                            if cc1.button("✓ ลบ", key=f"yes_{mkey}", type="primary"):
                                try:
                                    sb_delete(f"month_{mkey}.json")
                                except Exception:
                                    pass
                                idx2.pop(mkey)
                                save_index(idx2)
                                load_index_cached.clear(); load_month_data.clear()
                                st.rerun()
                            if cc2.button("ยกเลิก", key=f"no_{mkey}"):
                                del st.session_state[f"confirm_{mkey}"]
                                st.rerun()
                    st.markdown("---")
                    st.caption("Supabase free tier: 500MB — เก็บได้นานหลายปี")

    elif pw:
        st.error("Password ไม่ถูกต้อง")
    st.markdown("---")

    # ── KAM identity selector ─────────────────────────────────────────────
    st.markdown("**ฉันคือ**")
    all_kams_for_sel = ["–"] + sorted(REAL_AMS)
    my_kam = st.selectbox("เลือกชื่อของคุณ", all_kams_for_sel,
                           key="my_kam_identity", label_visibility="collapsed")
    if my_kam != "–":
        st.caption(f"คุณคือ {my_kam} · comment จะบันทึกชื่อนี้")
    st.markdown("---")

    # ── Filters ──────────────────────────────────────────────────────────
    idx_now = load_index_cached()
    if idx_now:
        # Month pill selector (show in sidebar)
        st.markdown("**เดือน**")
        month_keys = sorted(idx_now.keys())

        if "sel_month" not in st.session_state or st.session_state.sel_month not in month_keys:
            st.session_state.sel_month = month_keys[-1]

        m_cols = st.columns(min(len(month_keys), 3))
        for i, mkey in enumerate(month_keys):
            info = idx_now[mkey]
            is_sel = st.session_state.sel_month == mkey
            is_rr  = not info.get("stats",{}).get("is_complete", True)
            label  = info["label"].split(" ")[0]
            if is_rr: label += "*"
            if m_cols[i%3].button(label, key=f"mb_{mkey}", type="primary" if is_sel else "secondary", use_container_width=True):
                st.session_state.sel_month = mkey
                st.rerun()

        if any(not idx_now[m].get("stats",{}).get("is_complete",True) for m in month_keys):
            st.caption("* = ข้อมูลยังไม่ครบเดือน (แสดง Run Rate)")

        st.markdown("---")
        st.markdown("**Filters**")
        sel_am     = st.selectbox("AM", ["ทั้งหมด"]+sorted(REAL_AMS))
        sel_prio   = st.multiselect("Priority", ["critical","warning","healthy"], default=["critical","warning"])
        sel_search = st.text_input("ค้นหาร้าน", placeholder="ชื่อร้าน…")
        st.markdown("---")
        pass  # admin always shown


# ─── No data ──────────────────────────────────────────────────────────────────
idx_now = load_index_cached()
if not idx_now:
    if True:
        # Always show upload panel — protected by password
        st.markdown("## 📊 AE Portfolio Deepdive")
        st.info("ยังไม่มีข้อมูล — upload ไฟล์แรกได้เลยครับ")
        st.markdown("---")

        col_main, col_side = st.columns([2, 1])
        with col_main:
            st.markdown("### 📤 Upload Raw Data")

            # Check password
            pw_main = st.text_input("Admin Password", type="password", key="pw_main")
            if pw_main and pw_main != st.secrets.get("ADMIN_PASSWORD", "gowabi2024"):
                st.error("Password ไม่ถูกต้อง")
                st.stop()
            elif pw_main:
                st.success("✓ Authenticated")

                tx_file_main = st.file_uploader("ไฟล์ที่ 1 — Transaction (csv/xlsx) ✱",
                                                type=["csv","xlsx"], key="tx_main")
                view_file_main = st.file_uploader("ไฟล์ที่ 2 — View/CR (csv) — ไม่บังคับ",
                                                   type=["csv"], key="view_main")

                upload_mode_main = st.radio(
                    "โหมด Upload",
                    ["🔄 Overwrite — แทนที่ข้อมูลเดือนที่มีอยู่",
                     "➕ Append — เพิ่มเฉพาะเดือนใหม่"],
                    key="mode_main"
                )
                is_overwrite_main = upload_mode_main.startswith("🔄")

                if tx_file_main:
                    # Preview months
                    try:
                        tmp = pd.read_csv(io.BytesIO(tx_file_main.read()),
                                          usecols=["service_created_at","kam"],
                                          low_memory=False)
                        tx_file_main.seek(0)
                        tmp["ts"] = pd.to_datetime(tmp["service_created_at"], errors="coerce")
                        tmp = tmp[tmp["kam"].isin(REAL_AMS)]
                        found_m = sorted(tmp["ts"].dt.to_period("M").dropna().unique())
                        st.info(f"พบข้อมูล **{len(found_m)} เดือน**: " +
                                ", ".join([MONTH_LABELS[p.month-1]+f" {p.year}" for p in found_m]))
                    except Exception:
                        pass

                    if st.button("🚀 Process & Upload", type="primary", use_container_width=True,
                                 key="btn_main"):
                        with st.spinner("Processing… อาจใช้เวลา 1–3 นาที"):
                            tx_file_main.seek(0)
                            result = process_raw(
                                tx_file_main.read(),
                                view_file_main.read() if view_file_main else None
                            )
                        idx_fresh = load_index()
                        saved = []
                        for mkey, mdata_item in result["months"].items():
                            if not is_overwrite_main and mkey in idx_fresh:
                                continue
                            save_month(mkey, mdata_item)
                            idx_fresh[mkey] = {
                                "label":       mdata_item["stats"]["label"],
                                "upload_time": result["upload_time"],
                                "stats":       mdata_item["stats"],
                            }
                            saved.append(mkey)
                        if result.get("trend"):
                            save_trend(result["trend"])
                        save_index(idx_fresh)
                        load_index_cached.clear()
                        load_month_data.clear()
                        load_trend_data.clear()
                        if saved:
                            mlabels = [MONTH_LABELS[int(m.split('-')[1])-1]+' '+m.split('-')[0]
                                       for m in sorted(saved)]
                            st.success(f"✓ บันทึกแล้ว: {', '.join(mlabels)}")
                            st.balloons()
                            st.rerun()
            else:
                st.info("ใส่ Admin Password เพื่อ upload ข้อมูล")

        with col_side:
            st.markdown("### คำแนะนำ")
            st.markdown("""
**ไฟล์ที่ต้องการ:**
- Transaction CSV/xlsx (raw data จาก Gowabi)
- View/CR CSV (optional)

**ระบบจะ:**
- Auto-detect เดือนจาก `service_created_at`
- คำนวณ Run Rate สำหรับเดือนที่ยังไม่ครบ
- คำนวณ 5-pillar scores ทุกร้าน
            """)
    st.stop()


# ─── Load selected month data ─────────────────────────────────────────────────
sel_month = st.session_state.get("sel_month", sorted(idx_now.keys())[-1])
mdata     = load_month_data(sel_month)
trend     = load_trend_data()

if mdata is None:
    st.error("โหลดข้อมูลไม่ได้ กรุณา refresh"); st.stop()

shops_df = pd.DataFrame(mdata["shops"])
# Ensure numeric columns
for _nc in ["gmv","health_score","sku_score","price_score","view_score","cvr_score",
            "total_orders","unique_customers","new_customers","avg_view","avg_cr"]:
    if _nc in shops_df.columns:
        shops_df[_nc] = pd.to_numeric(shops_df[_nc], errors="coerce").fillna(0)
am_df    = pd.DataFrame(mdata["am"])
cat_df   = pd.DataFrame(mdata["category"])
stats    = mdata["stats"]
is_rr    = not stats.get("is_complete", True)

# Apply AM filter
if sel_am != "ทั้งหมด":
    shops_df = shops_df[shops_df["kam"] == sel_am]
    am_df    = am_df[am_df["kam"] == sel_am]
    cat_df_f = pd.DataFrame(trend.get("category",[])) if trend else cat_df
    # filter category trend too
else:
    pass

# Apply priority + search filter to shops
if sel_prio:
    shops_df = shops_df[shops_df["priority"].isin(sel_prio)]
if sel_search:
    shops_df = shops_df[shops_df["organization_name"].str.contains(sel_search, case=False, na=False)]

shops_df = shops_df.sort_values("health_score")


# ─── Header ───────────────────────────────────────────────────────────────────
sel_info = idx_now[sel_month]
rr_text  = f' <span class="rr-badge">Run Rate ฿{stats["gmv_run_rate"]/1e6:.1f}M ({stats["coverage_pct"]}% of month)</span>' if is_rr else ""
st.markdown(f'## AE Deepdive Dashboard {rr_text}', unsafe_allow_html=True)
st.caption(f'อัพโหลด {sel_info["upload_time"]} · {"จริง ✓" if stats.get("use_real_view") else "proxy"} View/CVR')


# ─── Tabs ─────────────────────────────────────────────────────────────────────
tab_ov, tab_gmv, tab_cat, tab_new, tab_health, tab_action, tab_sku, tab_portfolio, tab_upload = st.tabs([
    "📊 Overview", "📈 GMV MoM", "🗂️ Category", "👥 New User", "🏪 Store Health", "⚡ Action List", "🔬 SKU Analysis", "📋 Portfolio Review", "📤 Upload"
])


# ══ TAB 0: Overview ══════════════════════════════════════════════════════════
with tab_ov:

    # ── Month filter (inline at top) ─────────────────────────────────────────
    all_mk_ov  = sorted(idx_now.keys())
    ov_m_col1, ov_m_col2 = st.columns([3, 1])
    with ov_m_col1:
        ov_month_opts = [idx_now[m]["label"] for m in all_mk_ov]
        ov_month_sel  = st.selectbox(
            "เลือกเดือน",
            options=ov_month_opts,
            index=ov_month_opts.index(idx_now[sel_month]["label"]) if idx_now[sel_month]["label"] in ov_month_opts else len(ov_month_opts)-1,
            key="ov_month",
            label_visibility="collapsed"
        )
    # Resolve selected month key
    ov_mk   = next((m for m in all_mk_ov if idx_now[m]["label"]==ov_month_sel), sel_month)
    ov_prev = all_mk_ov[all_mk_ov.index(ov_mk)-1] if all_mk_ov.index(ov_mk)>0 else None

    # Load data for selected month (may differ from main sel_month)
    ov_mdata = load_month_data(ov_mk) if ov_mk != sel_month else mdata
    ov_stats  = ov_mdata["stats"] if ov_mdata else stats
    ov_shops  = pd.DataFrame(ov_mdata["shops"]) if ov_mdata else shops_df
    ov_is_rr  = not ov_stats.get("is_complete", True)

    # Load prev month for comparison
    ov_prev_stats = idx_now[ov_prev]["stats"] if ov_prev else None

    # ── KPI helpers ───────────────────────────────────────────────────────────
    def mom_delta(curr, prev, is_float=False):
        """Return (delta_str, delta_pct_str) for st.metric delta param."""
        if prev is None or prev == 0: return None
        d = curr - prev
        pct = d / prev * 100
        sign = "+" if d >= 0 else ""
        pct_str = f"{sign}{pct:.1f}%"
        return pct_str

    # KPI values
    gmv_show  = ov_stats["gmv_run_rate"] if ov_is_rr else ov_stats["gmv"]
    prev_gmv  = ov_prev_stats["gmv_run_rate"] if ov_prev_stats and not ov_prev_stats.get("is_complete",True) else (ov_prev_stats["gmv"] if ov_prev_stats else None)
    prev_ord  = ov_prev_stats["orders"] if ov_prev_stats else None
    prev_new  = ov_prev_stats["new_customers"] if ov_prev_stats else None

    cr_count  = (ov_shops["priority"]=="critical").sum() if len(ov_shops) else 0
    wa_count  = (ov_shops["priority"]=="warning").sum()  if len(ov_shops) else 0
    ah        = ov_shops["health_score"].mean() if len(ov_shops) else 0

    # ── KPI row ───────────────────────────────────────────────────────────────
    c1,c2,c3,c4,c5,c6 = st.columns(6)
    c1.metric(
        "GMV" + (" (RR)" if ov_is_rr else ""),
        fmt_gmv(gmv_show),
        delta=mom_delta(gmv_show, prev_gmv),
        help=f"เทียบกับ {idx_now[ov_prev]['label']}" if ov_prev else "ไม่มีข้อมูลเดือนก่อน"
    )
    c2.metric("Shops", f"{len(ov_mdata['shops']):,}")
    c3.metric(
        "Orders",
        f"{ov_stats['orders']:,}",
        delta=mom_delta(ov_stats['orders'], prev_ord),
        help=f"เทียบกับ {idx_now[ov_prev]['label']}" if ov_prev else "ไม่มีข้อมูลเดือนก่อน"
    )
    c4.metric("Critical 🔴", f"{cr_count}")
    c5.metric("Warning 🟡",  f"{wa_count}")
    c6.metric("Avg Health",  f"{ah:.1f}")

    if ov_is_rr:
        st.info(f"📅 {ov_month_sel}: ข้อมูล {ov_stats['days']}/{ov_stats['days_in_month']} วัน ({ov_stats['coverage_pct']}%) — Run Rate GMV = ฿{ov_stats['gmv_run_rate']/1e6:.1f}M")
    if ov_prev:
        prev_lbl = idx_now[ov_prev]["label"]
        gmv_diff = gmv_show - (prev_gmv or 0)
        ord_diff = ov_stats["orders"] - (prev_ord or 0)
        gmv_pct  = gmv_diff/(prev_gmv or 1)*100
        ord_pct  = ord_diff/(prev_ord or 1)*100
        st.caption(
            f"เทียบกับ {prev_lbl}: "
            f"GMV {'▲' if gmv_diff>=0 else '▼'} {abs(gmv_pct):.1f}% ({fmt_gmv(abs(gmv_diff))})  ·  "
            f"Orders {'▲' if ord_diff>=0 else '▼'} {abs(ord_pct):.1f}% ({abs(ord_diff):,} orders)"
        )

    # AM scorecard — per-person filter
    am_full    = pd.DataFrame(ov_mdata["am"])
    shops_full = pd.DataFrame(ov_mdata["shops"])

    st.markdown('<div class="section-title">AM Scorecard</div>', unsafe_allow_html=True)

    # AM dropdown filter
    am_options = ["ทั้งหมด"] + sorted(am_full["kam"].unique().tolist())
    ov_am_sel  = st.selectbox("เลือก AM", am_options, key="ov_am_sel_dd",
                               label_visibility="collapsed")

    # Filter data by selected AM
    am_src_ov = am_full if ov_am_sel=="ทั้งหมด" else am_full[am_full["kam"]==ov_am_sel]
    shops_src  = shops_full if ov_am_sel=="ทั้งหมด" else shops_full[shops_full["kam"]==ov_am_sel]

    # ── Load prev month AM data for comparison ────────────────────────────────
    prev_am_map = {}   # kam → {gmv, total_orders, avg_view}
    if ov_prev:
        prev_md = load_month_data(ov_prev)
        if prev_md:
            prev_am_df  = pd.DataFrame(prev_md.get("am",[]))
            prev_sh_df  = pd.DataFrame(prev_md.get("shops",[]))
            prev_is_rr2 = not idx_now[ov_prev]["stats"].get("is_complete", True)
            prev_cov    = idx_now[ov_prev]["stats"].get("coverage_pct", 100) / 100

            for _, pa in prev_am_df.iterrows():
                psh = prev_sh_df[prev_sh_df["kam"]==pa["kam"]]
                p_orders = int(pa.get("unique_customers", 0)) if "unique_customers" in pa else (psh["unique_customers"].sum() if "unique_customers" in psh.columns else 0)
                p_view   = float(pa.get("total_page_view", psh["avg_view"].sum() if "avg_view" in psh.columns else 0))
                # Run rate adjust if prev month was incomplete
                p_gmv_rr = pa["gmv"] / prev_cov if prev_is_rr2 and prev_cov > 0 else pa["gmv"]
                p_orders = int(pa.get("total_orders", p_orders))  # prefer AM-level total_orders
                p_ord_rr = p_orders / prev_cov if prev_is_rr2 and prev_cov > 0 else p_orders
                p_view_rr= p_view    / prev_cov if prev_is_rr2 and prev_cov > 0 else p_view
                p_cr_am  = float(pa.get("avg_cr_pct", 0)) if "avg_cr_pct" in pa else float(psh["avg_cr"].mean() if "avg_cr" in psh.columns else 0)
                prev_am_map[pa["kam"]] = {
                    "gmv":    p_gmv_rr,
                    "orders": p_ord_rr,
                    "view":   p_view_rr,
                    "cr":     p_cr_am,
                }

    def delta_html(curr, prev_val, fmt_fn=None, suffix=""):
        """Return colored % change HTML string."""
        try:
            curr     = float(curr)
            prev_val = float(prev_val)
        except (TypeError, ValueError):
            return ""
        if not prev_val or prev_val == 0: return ""
        pct   = (curr - prev_val) / prev_val * 100
        color = "#3a7d2c" if pct >= 0 else "#d94040"
        arrow = "▲" if pct >= 0 else "▼"
        return f'<span style="font-size:10px;color:{color};font-weight:500">{arrow}{abs(pct):.0f}%</span>'

    def rr_html(rr_val, is_month_rr):
        """Return (RR ฿xxx) label if month is incomplete."""
        if not is_month_rr: return ""
        return f'<div style="font-size:9px;color:#185FA5;margin-top:2px">RR {fmt_gmv(rr_val)}</div>'

    # ── AM Scorecard cards with 6 metrics ──────────────────────────────────
    for _, r in am_src_ov.sort_values("avg_health").iterrows():
        am_shops = shops_src[shops_src["kam"]==r["kam"]] if ov_am_sel=="all" else shops_src
        cov = ov_stats.get("coverage_pct", 100) / 100

        # Orders: use total_orders from AM summary (unique booking_id per AM per month)
        am_orders   = int(r.get("total_orders", 0))
        new_cust    = int(r.get("new_customers", 0))
        basket_size = int(r["gmv"] / am_orders) if am_orders > 0 else 0

        # View/CVR from AM summary (new format)
        avg_page_view = float(r.get("total_page_view", r.get("avg_page_view", 0)))
        avg_cr        = float(r.get("avg_cr_pct", 0))

        # Run rate
        am_orders_rr  = am_orders    / cov if ov_is_rr and cov > 0 and am_orders > 0 else am_orders
        view_rr       = avg_page_view / cov if ov_is_rr and cov > 0 and avg_page_view > 0 else avg_page_view

        # Old data flag — show "–" for fields not in old AM summary
        _old_data = am_orders == 0 and new_cust == 0

        # Run rate (pro-rate by coverage for incomplete months)
        gmv_rr   = r["gmv"] / cov if ov_is_rr and cov > 0 else r["gmv"]

        # Prev month values
        prev_am = prev_am_map.get(r["kam"], {})
        p_gmv   = prev_am.get("gmv",    0)
        p_ord   = prev_am.get("orders", 0)
        p_view  = prev_am.get("view",   0)

        health_color = sc(r["avg_health"])

        # Helper: metric cell HTML
        # Pre-compute all metric cells before embedding in HTML
        def mk_cell(label, main_val, rr_val=None, prev_val=None, color="inherit", rr_fmt=None):
            """rr_fmt: callable to format rr_val, defaults to fmt_gmv"""
            if rr_val and ov_is_rr:
                rr_display = rr_fmt(rr_val) if rr_fmt else fmt_gmv(rr_val)
                rr_part    = f'<div style="font-size:9px;color:#185FA5;margin-top:1px">RR {rr_display}</div>'
            else:
                rr_part = ""
            d_part  = delta_html(rr_val or main_val, prev_val) if prev_val else ""
            return (
                '<div style="background:#fafafa;border-radius:6px;padding:9px 10px;text-align:center;border:1px solid #f0f0f0">'
                f'<div style="font-size:9px;font-weight:600;color:#bbb;text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px">{label}</div>'
                f'<div style="font-size:14px;font-weight:600;color:{color}">{main_val}</div>'
                f'{rr_part}{d_part}'
                '</div>'
            )

        c_gmv    = mk_cell("GMV",        fmt_gmv(r["gmv"]),         rr_val=gmv_rr,   prev_val=p_gmv  or None)
        if _old_data:
            c_ord    = mk_cell("Orders",      "–  (re-upload)", color="#bbb")
            c_basket = mk_cell("Basket Size", "–")
            c_new    = mk_cell("New User",    "–")
            p_cr_val2 = prev_am.get("cr", 0)
            c_view   = mk_cell("Shop View", f"{avg_page_view:,.0f}" if avg_page_view else "–",
                               rr_val=view_rr if avg_page_view else None,
                               prev_val=p_view or None,
                               rr_fmt=lambda v: f"{int(v):,}")
            c_cvr    = mk_cell("CVR",       f"{avg_cr:.2f}%" if avg_cr else "–",
                               prev_val=p_cr_val2 or None,
                               color=sc(r["avg_cvr"]) if avg_cr else "#bbb")
        else:
            c_ord    = mk_cell("Orders",      f"{am_orders:,}",        rr_val=am_orders_rr, prev_val=p_ord or None)
            c_basket = mk_cell("Basket Size", f"฿{basket_size:,}")
            c_new    = mk_cell("New User",    f"{new_cust:,}")
            p_cr_val = prev_am.get("cr", 0)
            c_view   = mk_cell("Shop View", f"{avg_page_view:,.0f}",
                               rr_val=view_rr if avg_page_view else None,
                               prev_val=p_view or None,
                               rr_fmt=lambda v: f"{int(v):,}")  # no ฿ for view count
            c_cvr    = mk_cell("CVR",       f"{avg_cr:.2f}%",
                               prev_val=p_cr_val or None,
                               color=sc(r["avg_cvr"]))

        cells_html = c_gmv + c_ord + c_basket + c_new + c_view + c_cvr

        card_html = (
            '<div style="background:#fff;border:1px solid #ebebeb;border-radius:8px;padding:14px 18px;margin-bottom:8px">'
            '<div style="display:flex;align-items:center;gap:12px;margin-bottom:10px">'
            f'<div style="font-size:13px;font-weight:600;letter-spacing:-.01em;min-width:80px">{r["kam"]}</div>'
            f'<div style="font-size:24px;font-weight:600;letter-spacing:-.03em;color:{health_color}">{r["avg_health"]:.1f}</div>'
            f'<div style="font-size:11px;color:#aaa">{int(r["shops"])} shops</div>'
            '<div style="margin-left:auto;font-size:11px">'
            f'<span style="color:#E24B4A">🔴 {int(r["critical_shops"])} critical</span>&nbsp;&nbsp;'
            f'<span style="color:#EF9F27">🟡 {int(r["warning_shops"])} warning</span>'
            '</div></div>'
            '<div style="display:grid;grid-template-columns:repeat(6,1fr);gap:8px">'
            + cells_html +
            '</div></div>'
        )
        st.markdown(card_html, unsafe_allow_html=True)

        # ── Shop drill-down for this AM ──────────────────────────────────────
        kam_key = f"expand_am_{r['kam'].replace(' ','_')}"
        is_expanded = st.session_state.get(kam_key, False)

        btn_lbl = f"▲ ซ่อนร้านทั้งหมด" if is_expanded else f"▼ ดูร้านทั้งหมด {int(r['shops'])} ร้าน"
        if st.button(btn_lbl, key=f"btn_{kam_key}", use_container_width=True):
            st.session_state[kam_key] = not is_expanded
            st.rerun()

        if is_expanded:
            am_shops_drill = shops_src[shops_src["kam"] == r["kam"]].copy() if ov_am_sel == "ทั้งหมด" else shops_src.copy()
            am_shops_drill = am_shops_drill.sort_values("health_score")

            # Load prev month shop data for this AM
            prev_ov_mk  = all_mk_ov[all_mk_ov.index(ov_mk)-1] if ov_mk in all_mk_ov and all_mk_ov.index(ov_mk)>0 else None
            prev_ov_lbl = idx_now[prev_ov_mk]["label"] if prev_ov_mk else None
            prev_am_shops = pd.DataFrame()
            if prev_ov_mk:
                prev_ov_md = load_month_data(prev_ov_mk)
                if prev_ov_md:
                    prev_df = pd.DataFrame(prev_ov_md.get("shops",[]))
                    if len(prev_df):
                        prev_am_shops = prev_df[prev_df["kam"]==r["kam"]].set_index("shop_id_s")

            # Coverage for run rate
            cur_cov  = idx_now[ov_mk]["stats"].get("coverage_pct",100) / 100
            prev_cov2= idx_now[prev_ov_mk]["stats"].get("coverage_pct",100) / 100 if prev_ov_mk else 1.0
            cur_inc  = not idx_now[ov_mk]["stats"].get("is_complete",True)
            prev_inc2= not idx_now[prev_ov_mk]["stats"].get("is_complete",True) if prev_ov_mk else False

            def rr(val, cov, is_inc):
                """Apply run rate if month incomplete."""
                return val / cov if is_inc and cov > 0 and val > 0 else val

            def fmt_full(v):
                """Full number no abbreviation."""
                return f"฿{int(v):,}" if v >= 0 else f"-฿{int(abs(v)):,}"

            def fmt_rr(val, cov, is_inc, fmt_fn):
                """Show actual + RR in full numbers."""
                actual_str = fmt_fn(val)
                if is_inc and cov > 0 and val > 0:
                    return f"{actual_str}  (RR {fmt_fn(val/cov)})"
                return actual_str

            # Always use fmt_full for prev GMV too
            def fmt_prev_gmv(v):
                return fmt_full(v)

            def chg_rr(c, pv, c_cov, c_inc, p_cov, p_inc):
                """% change using run rate for both sides."""
                c_rr = rr(c,  c_cov, c_inc)
                p_rr = rr(pv, p_cov, p_inc)
                if p_rr == 0: return "–"
                d = (c_rr - p_rr) / p_rr * 100
                col = "▲" if d>=0 else "▼"
                return f"{col}{abs(d):.0f}%"

            # Build comparison table
            rows_cmp = []
            for _, sh in am_shops_drill.iterrows():
                sid   = sh.get("shop_id_s", "")
                p     = prev_am_shops.loc[sid] if (len(prev_am_shops) and sid in prev_am_shops.index) else None

                c_gmv  = float(sh.get("gmv", 0))
                c_ord  = float(sh.get("total_orders", 0))
                c_view = float(sh.get("avg_view", 0))
                c_cr   = float(sh.get("avg_cr", 0))

                p_gmv  = float(p.get("gmv", 0))          if p is not None else None
                p_ord  = float(p.get("total_orders", 0))  if p is not None else None
                p_view = float(p.get("avg_view", 0))      if p is not None else None
                p_cr   = float(p.get("avg_cr", 0))        if p is not None else None

                lbl_cur  = f"{ov_month_sel}" + (" (RR)" if cur_inc else "")
                lbl_prev = prev_ov_lbl or "–"

                row = {
                    "Shop":     sh.get("organization_name",""),
                    "Category": sh.get("category",""),
                    f"GMV {lbl_cur}":    fmt_full(c_gmv),
                    f"RR {lbl_cur}":     fmt_full(c_gmv/cur_cov) if cur_inc and cur_cov>0 else "–",
                    f"GMV {lbl_prev}":   fmt_full(p_gmv) if p_gmv is not None else "–",
                    "GMV Δ":    chg_rr(c_gmv,  p_gmv,  cur_cov, cur_inc, prev_cov2, prev_inc2) if p_gmv  is not None else "–",
                    f"Orders {lbl_cur}": fmt_rr(c_ord,  cur_cov,  cur_inc,  lambda v: f"{int(v):,}"),
                    f"Orders {lbl_prev}":fmt_rr(p_ord,  prev_cov2,prev_inc2,lambda v: f"{int(v):,}") if p_ord  is not None else "–",
                    "Orders Δ": chg_rr(c_ord,  p_ord,  cur_cov, cur_inc, prev_cov2, prev_inc2) if p_ord  is not None else "–",
                    f"View {lbl_cur}":   fmt_rr(c_view, cur_cov, cur_inc, lambda v: f"{int(v):,}") if idx_now[ov_mk]["stats"].get("has_view_data") else "–",
                    f"View {lbl_prev}":  (fmt_rr(p_view, prev_cov2, prev_inc2, lambda v: f"{int(v):,}") if (p_view is not None and prev_ov_mk and idx_now[prev_ov_mk]["stats"].get("has_view_data")) else "–"),
                    "View Δ":   (chg_rr(c_view, p_view, cur_cov, cur_inc, prev_cov2, prev_inc2) if (p_view is not None and idx_now[ov_mk]["stats"].get("has_view_data") and prev_ov_mk and idx_now[prev_ov_mk]["stats"].get("has_view_data")) else "–"),
                    f"CVR {lbl_cur}":    f"{c_cr:.2f}%" if idx_now[ov_mk]["stats"].get("has_view_data") else "–",
                    f"CVR {lbl_prev}":   (f"{p_cr:.2f}%" if (p_cr is not None and prev_ov_mk and idx_now[prev_ov_mk]["stats"].get("has_view_data")) else "–"),
                    "CVR Δ":    (chg_rr(c_cr, p_cr, 1, False, 1, False) if (p_cr is not None and idx_now[ov_mk]["stats"].get("has_view_data") and prev_ov_mk and idx_now[prev_ov_mk]["stats"].get("has_view_data")) else "–"),
                }
                rows_cmp.append(row)

            tbl = pd.DataFrame(rows_cmp)

            def _cdelta(v):
                if not isinstance(v, str) or v == "–": return "color:#aaa"
                return "color:#16A34A;font-weight:600" if "▲" in v else "color:#DC2626;font-weight:600"

            delta_cols = ["GMV Δ","Orders Δ","View Δ","CVR Δ"]
            styled = tbl.style.map(_cdelta, subset=[c for c in delta_cols if c in tbl.columns])
            styled = styled.set_properties(**{"font-size":"11px"})

            st.dataframe(styled, use_container_width=True,
                         height=min(520, 44 + len(tbl)*35))
            st.download_button(
                f"⬇ {r['kam']}_shops.csv",
                to_csv(tbl),
                f"{r['kam'].replace(' ','_')}_{ov_mk}.csv",
                "text/csv", key=f"dl_{kam_key}"
            )
            st.markdown("---")

    # ── Pillar scores — clickable drill-down ────────────────────────────────
    st.markdown('<div class="section-title">4 Pillar Scores — คลิกเพื่อดูร้านที่ต้องแก้</div>', unsafe_allow_html=True)

    PILLAR_META = [
        {"name": "Price",       "score_col": "price_score", "am_key": "avg_price",
         "desc": "ร้านที่ราคาสูงกว่า lowest 12m",
         "detail_cols": ["organization_name","kam","category","selling_price_mean","lowest_price_12m","price_above","price_score"],
         "detail_labels": {"organization_name":"Shop","kam":"AM","category":"Category","selling_price_mean":"Selling","lowest_price_12m":"Lowest 12m","price_above":"Above%","price_score":"Score"},
         "sort_asc": True, "threshold": 50, "use_svc_level": True, "add_prev_price": True},
        {"name": "View MoM",   "score_col": "view_score",  "am_key": "avg_view",
         "desc": "ร้านที่ view ลดลงจากเดือนก่อน",
         "detail_cols": ["organization_name","kam","category","avg_view","view_mom_pct"],
         "detail_labels": {"organization_name":"Shop","kam":"AM","category":"Category","avg_view":"View (RR)","view_mom_pct":"MoM%"},
         "sort_asc": True, "threshold": 40, "use_mom_compare": True},
        {"name": "CVR MoM",    "score_col": "cvr_score",   "am_key": "avg_cvr",
         "desc": "ร้านที่ conversion rate ลดลง",
         "detail_cols": ["organization_name","kam","category","avg_cr","cvr_mom_pct"],
         "detail_labels": {"organization_name":"Shop","kam":"AM","category":"Category","avg_cr":"CVR (now)","cvr_mom_pct":"MoM%"},
         "sort_asc": True, "threshold": 40, "use_mom_compare": True},
    ]

    sel_pillar = st.session_state.get("sel_pillar_ov", None)
    pcols = st.columns(3)

    for col, pm in zip(pcols, PILLAR_META):
        avg   = am_src_ov[pm["am_key"]].mean() if len(am_src_ov) else 0
        label = "ต้องแก้" if avg<40 else "ปรับปรุง" if avg<60 else "ดี"
        bg,tc = ("#FCEBEB","#A32D2D") if avg<40 else ("#FAEEDA","#854F0B") if avg<60 else ("#EAF3DE","#3B6D11")
        is_sel = sel_pillar == pm["name"]
        border = "2px solid #1a5fa8" if is_sel else "1px solid #e8e5e0"

        col.markdown(f"""<div style="background:#fff;border-radius:8px;padding:1rem 1.1rem;border:{border}">
          <div style="font-size:9px;color:#999;font-weight:500;text-transform:uppercase;letter-spacing:.05em;margin-bottom:3px">{pm['name']}</div>
          <div style="font-size:20px;font-weight:500;color:{sc(avg)}">{avg:.0f}</div>
          <div style="height:4px;background:#e5e3de;border-radius:2px;margin:4px 0">
            <div style="width:{avg:.0f}%;height:100%;background:{sc(avg)};border-radius:2px"></div>
          </div>
          <span style="font-size:9px;background:{bg};color:{tc};padding:1px 6px;border-radius:8px">{label}</span>
        </div>""", unsafe_allow_html=True)

        btn_label = "✕ ปิด" if is_sel else f"ดูร้าน ↓"
        if col.button(btn_label, key=f"pil_{pm['name']}", use_container_width=True,
                      type="primary" if is_sel else "secondary"):
            st.session_state["sel_pillar_ov"] = None if is_sel else pm["name"]
            st.rerun()

    # ── Pillar drill-down table ───────────────────────────────────────────────
    if sel_pillar:
        pm = next(p for p in PILLAR_META if p["name"] == sel_pillar)
        score_col = pm["score_col"]
        threshold = pm["threshold"]

        drill_src = ov_shops.copy() if ov_am_sel == "ทั้งหมด" else ov_shops[ov_shops["kam"] == ov_am_sel].copy()
        drill = drill_src[drill_src[score_col] < threshold].copy() if score_col in drill_src.columns else drill_src.copy()
        drill = drill.sort_values(score_col, ascending=True)

        n_total = len(drill_src)
        n_low   = len(drill)
        st.markdown(
            f'<div class="section-title">{pm["name"]} — {n_low} ร้านที่ต้องปรับ '
            f'({n_low/max(n_total,1)*100:.0f}% ของทั้งหมด) · {pm["desc"]}</div>',
            unsafe_allow_html=True
        )

        if len(drill) == 0:
            st.success(f"✓ ไม่มีร้านที่ต้องแก้ใน {sel_pillar} ครับ")
        else:
            # ── Price pillar: service-level view ──────────────────────────
            if pm.get("use_svc_level") and "svc_price" in ov_mdata:
                svc_df = pd.DataFrame(ov_mdata["svc_price"])
                if len(svc_df):
                    # Filter by AM
                    if ov_am_sel != "ทั้งหมด":
                        svc_df = svc_df[svc_df["kam"]==ov_am_sel]
                    # Only services with pct > 0
                    svc_df = svc_df[svc_df["pct"] > 0].sort_values("pct", ascending=False)

                    # Toggle shop/service view
                    view_mode = st.radio("แสดงแบบ", ["Service Level", "Shop Level"],
                                         horizontal=True, key="price_drill_mode")

                    if view_mode == "Service Level":
                        tbl_svc = svc_df[["service_name","organization_name","kam","category","sell","low12","pct","gmv","orders"]].copy()
                        tbl_svc.columns = ["Service","Shop","AM","Category","Selling","Lowest 12m","Above%","GMV","Orders"]
                        tbl_svc["Selling"]    = tbl_svc["Selling"].apply(lambda x: f"฿{x:,.0f}")
                        tbl_svc["Lowest 12m"] = tbl_svc["Lowest 12m"].apply(lambda x: f"฿{x:,.0f}")
                        tbl_svc["Above%"]     = tbl_svc["Above%"].apply(lambda x: f"+{x:.0f}%")
                        tbl_svc["GMV"]        = tbl_svc["GMV"].apply(fmt_gmv)

                        def css_above(v):
                            if not isinstance(v,str): return ""
                            try:
                                f = float(v.replace("+","").replace("%",""))
                                return "color:#d94040;font-weight:600" if f>=30 else "color:#EF9F27;font-weight:500" if f>=15 else "color:#888"
                            except: return ""

                        styled_svc = (tbl_svc.style
                            .map(css_above, subset=["Above%"])
                            .set_properties(**{"font-size":"11px"}))
                        st.dataframe(styled_svc, hide_index=True, use_container_width=True,
                                     height=min(500, 44 + len(tbl_svc)*36))
                        st.download_button("⬇ overpriced_services.csv", to_csv(svc_df),
                                           f"price_services_{ov_mk}.csv", "text/csv")
                    else:
                        # Shop level — add prev month price
                        available_cols = [c for c in pm["detail_cols"] if c in drill.columns]
                        tbl_drill = drill[available_cols].rename(columns=pm["detail_labels"]).copy()

                        # Add prev month selling price
                        if ov_prev:
                            prev_sh_price = load_month_data(ov_prev)
                            if prev_sh_price:
                                prev_sh_df2 = pd.DataFrame(prev_sh_price.get("shops",[]))
                                if len(prev_sh_df2) and "selling_price_mean" in prev_sh_df2.columns:
                                    prev_price_map = prev_sh_df2.set_index("shop_id_s")["selling_price_mean"].to_dict()
                                    prev_lbl_price = idx_now[ov_prev]["label"]
                                    tbl_drill.insert(
                                        list(tbl_drill.columns).index("Selling")+1,
                                        f"Prev ({prev_lbl_price})",
                                        drill["shop_id_s"].map(lambda x: f"฿{prev_price_map.get(str(x),0):,.0f}")
                                    )

                        tbl_drill["Selling"]    = tbl_drill["Selling"].apply(lambda x: f"฿{x:,.0f}" if isinstance(x,(int,float)) else x)
                        tbl_drill["Lowest 12m"] = tbl_drill["Lowest 12m"].apply(lambda x: f"฿{x:,.0f}" if isinstance(x,(int,float)) else x)
                        tbl_drill["Above%"]     = tbl_drill["Above%"].apply(lambda x: f"+{x:.0f}%" if isinstance(x,(int,float)) else x)
                        if "Score" in tbl_drill.columns:
                            tbl_drill["Score"]  = tbl_drill["Score"].apply(lambda x: f"{int(x)}" if isinstance(x,(int,float)) else x)
                        styled_drill = tbl_drill.style.set_properties(**{"font-size":"12px"})
                        st.dataframe(styled_drill, use_container_width=True,
                                     height=min(400, 40 + len(tbl_drill)*36))
                        st.download_button("⬇ price_shops.csv", to_csv(drill[available_cols]),
                                           f"price_shops_{ov_mk}.csv", "text/csv")
            else:
                # ── Other pillars (SKU, View MoM, CVR MoM) ─────────────────
                available_cols = [c for c in pm["detail_cols"] if c in drill.columns]
                tbl_drill = drill[available_cols].rename(columns=pm["detail_labels"]).copy()

                # Add prev month comparison from Supabase data
                if pm.get("use_mom_compare") and ov_prev:
                    prev_md_drill  = load_month_data(ov_prev)
                    prev_lbl_drill = idx_now[ov_prev]["label"]
                    if prev_md_drill:
                        prev_sh = pd.DataFrame(prev_md_drill.get("shops",[]))
                        if len(prev_sh):
                            id_col     = "shop_id_s"
                            cur_id_col = "shop_id_s" if "shop_id_s" in drill.columns else "organization_name"

                            # View MoM
                            if "avg_view" in drill.columns and "avg_view" in prev_sh.columns:
                                prev_v = prev_sh.set_index(id_col)["avg_view"].to_dict()
                                cur_v  = drill.set_index(cur_id_col)["avg_view"].to_dict()
                                cur_ids = drill[cur_id_col].values

                                prev_col_vals, mom_vals = [], []
                                for sid in cur_ids:
                                    pv = prev_v.get(str(sid), 0)
                                    cv = cur_v.get(str(sid), 0)
                                    prev_col_vals.append(f"{int(pv):,}")
                                    if pv > 0:
                                        mom_vals.append(f"{(cv-pv)/pv*100:+.0f}%")
                                    elif cv > 0:
                                        mom_vals.append("new")
                                    else:
                                        mom_vals.append("–")

                                # Insert prev + MoM after Views/mo
                                if "Views/mo" in tbl_drill.columns:
                                    insert_pos = list(tbl_drill.columns).index("Views/mo") + 1
                                    if prev_lbl_drill not in tbl_drill.columns:
                                        tbl_drill.insert(insert_pos, prev_lbl_drill, prev_col_vals)
                                    if "MoM%" not in tbl_drill.columns:
                                        tbl_drill.insert(list(tbl_drill.columns).index(prev_lbl_drill)+1, "MoM%", mom_vals)
                                else:
                                    tbl_drill[prev_lbl_drill] = prev_col_vals
                                    tbl_drill["MoM%"]         = mom_vals

                            # CVR MoM
                            if "avg_cr" in drill.columns and "avg_cr" in prev_sh.columns:
                                prev_cr = prev_sh.set_index(id_col)["avg_cr"].to_dict()
                                cur_cr  = drill.set_index(cur_id_col)["avg_cr"].to_dict()
                                cur_ids = drill[cur_id_col].values

                                prev_cr_vals, mom_cr_vals = [], []
                                for sid in cur_ids:
                                    pc = prev_cr.get(str(sid), 0)
                                    cc = cur_cr.get(str(sid), 0)
                                    prev_cr_vals.append(f"{pc:.2f}%")
                                    if pc > 0:
                                        mom_cr_vals.append(f"{(cc-pc)/pc*100:+.0f}%")
                                    elif cc > 0:
                                        mom_cr_vals.append("new")
                                    else:
                                        mom_cr_vals.append("–")

                                if "CR%" in tbl_drill.columns:
                                    insert_pos = list(tbl_drill.columns).index("CR%") + 1
                                    if prev_lbl_drill not in tbl_drill.columns:
                                        tbl_drill.insert(insert_pos, prev_lbl_drill, prev_cr_vals)
                                    if "MoM%" not in tbl_drill.columns:
                                        tbl_drill.insert(list(tbl_drill.columns).index(prev_lbl_drill)+1, "MoM%", mom_cr_vals)
                                else:
                                    tbl_drill[prev_lbl_drill] = prev_cr_vals
                                    tbl_drill["MoM%"]         = mom_cr_vals

                # Format numbers
                for c in tbl_drill.columns:
                    if c == "Score":
                        tbl_drill[c] = tbl_drill[c].apply(lambda x: f"{int(x)}" if isinstance(x,(int,float)) and str(x) not in ['nan'] else x)
                    elif c == "SKUs":
                        tbl_drill[c] = tbl_drill[c].apply(lambda x: f"{int(x):,}" if isinstance(x,(int,float)) else x)
                    elif c in ["View (RR)","Views/mo"]:
                        # Apply run rate if current month is incomplete
                        _dcov = idx_now.get(ov_mk,{}).get("stats",{}).get("coverage_pct",100)/100
                        _dinc = not idx_now.get(ov_mk,{}).get("stats",{}).get("is_complete",True)
                        def _fmt_view_rr(v):
                            try:
                                raw = float(v)
                                if _dinc and _dcov>0 and raw>0:
                                    return f"{int(raw):,} (RR {int(raw/_dcov):,})"
                                return f"{int(raw):,}"
                            except: return str(v)
                        tbl_drill[c] = tbl_drill[c].apply(_fmt_view_rr)
                    elif c == "MoM%":
                        tbl_drill[c] = tbl_drill[c].apply(lambda x: f"{x:+.0f}%" if isinstance(x,(int,float)) else x)
                    elif c == "CR%":
                        tbl_drill[c] = tbl_drill[c].apply(lambda x: f"{x:.2f}%" if isinstance(x,(int,float)) else x)

                def css_score2(v):
                    try: return f"color:{sc(float(v))};font-weight:500"
                    except: return ""
                def css_pct2(v):
                    if not isinstance(v,str): return ""
                    try:
                        f2 = float(v.replace("%","").replace("+",""))
                        return "color:#d94040;font-weight:500" if f2 < 0 else "color:#3a7d2c;font-weight:500" if f2 > 0 else "color:#aaa"
                    except: return ""

                subset_cols = [c for c in ["Score","MoM%"] if c in tbl_drill.columns]
                styled_drill = tbl_drill.style
                if "Score" in tbl_drill.columns:
                    styled_drill = styled_drill.map(css_score2, subset=["Score"])
                if "MoM%" in tbl_drill.columns:
                    styled_drill = styled_drill.map(css_pct2, subset=["MoM%"])
                styled_drill = styled_drill.set_properties(**{"font-size":"12px"})

                st.dataframe(styled_drill, use_container_width=True,
                             height=min(460, 40 + len(tbl_drill)*36))
                st.download_button(
                    f"⬇ {sel_pillar}_shops.csv",
                    to_csv(drill[available_cols]),
                    f"{sel_pillar.lower().replace(' ','_')}_{ov_mk}.csv",
                    "text/csv"
                )


# ══ TAB 1: GMV MoM ════════════════════════════════════════════════════════════
with tab_gmv:
    if not trend:
        st.info("Upload ข้อมูลหลายเดือนเพื่อดู trend")
    else:
        trend_kam  = pd.DataFrame(trend.get("kam",[]))
        trend_shop = pd.DataFrame(trend.get("shops",[]))
        trend_svc  = pd.DataFrame(trend.get("services",[]))

        # ── Filters ────────────────────────────────────────────────────────
        gf1, gf2 = st.columns([1,2])
        with gf1:
            gmv_am_filt = st.selectbox("Filter by KAM", ["ทั้งหมด"]+sorted(REAL_AMS), key="gmv_am")
        with gf2:
            # Shop search filter
            # Shop names loaded from all months on demand
            all_shop_names = sorted(
                set(s.get("organization_name","") for mk in idx_now
                    for s in ((load_month_data(mk) or {}).get("shops",[])))
            ) if idx_now else []
            gmv_shop_filt  = st.multiselect("Filter by Shop (เว้นว่าง = ทั้งหมด)",
                                             all_shop_names, default=[], key="gmv_shop",
                                             placeholder="เลือกร้านที่ต้องการ…")

        # Build monthly summary — filter by KAM if selected
        all_months = sorted(idx_now.keys())
        all_labels = [idx_now[m]["label"] for m in all_months]

        # Build per-month GMV respecting both KAM and Shop filters
        if gmv_shop_filt:
            # Shop filter: sum GMV per month from shop trend data
            shop_gmv_by_month = {}
            for row in trend_shop.to_dict("records"):
                if row["organization_name"] in gmv_shop_filt:
                    mk = row["month"]
                    shop_gmv_by_month[mk] = shop_gmv_by_month.get(mk, 0) + row["gmv"]
            all_gmv    = [shop_gmv_by_month.get(m, 0) for m in all_months]
            cov_map    = {m: idx_now[m]["stats"].get("coverage_pct",100)/100 for m in all_months}
            all_rr_gmv = [
                int(v / max(cov_map[m], 0.01)) if not idx_now[m]["stats"].get("is_complete",True) else v
                for m,v in zip(all_months, all_gmv)
            ]
            shop_names = ", ".join(gmv_shop_filt[:2]) + ("…" if len(gmv_shop_filt)>2 else "")
            gmv_title  = f"GMV — {shop_names} รายเดือน (฿M)"
        elif gmv_am_filt != "ทั้งหมด":
            # KAM filter — load from per-month Supabase data for accuracy
            with st.spinner(f"โหลด GMV {gmv_am_filt}…"):
                km_gmv_map = {}
                for _mk in all_months:
                    _md = load_month_data(_mk)
                    if _md:
                        _am_df = pd.DataFrame(_md.get("am",[]))
                        if len(_am_df):
                            _row = _am_df[_am_df["kam"]==gmv_am_filt]
                            km_gmv_map[_mk] = float(_row["gmv"].sum()) if len(_row) else 0
            all_gmv    = [km_gmv_map.get(m, 0) for m in all_months]
            all_rr_gmv = [
                int(v / max(idx_now[m]["stats"].get("coverage_pct",100)/100, 0.01))
                if not idx_now[m]["stats"].get("is_complete", True) else int(v)
                for m, v in zip(all_months, all_gmv)
            ]
            gmv_title = f"GMV {gmv_am_filt} รายเดือน (฿M)"
        else:
            all_gmv    = [idx_now[m]["stats"]["gmv"] for m in all_months]
            all_rr_gmv = [idx_now[m]["stats"]["gmv_run_rate"] for m in all_months]
            gmv_title  = "GMV รายเดือน (฿M)"

        col1,col2 = st.columns(2)
        with col1:
            st.markdown(f'<div class="section-title">{gmv_title}</div>', unsafe_allow_html=True)
            st.bar_chart(pd.DataFrame({"GMV":[v/1e6 for v in all_gmv]}, index=all_labels),
                         use_container_width=True, height=180)
        with col2:
            st.markdown('<div class="section-title">MoM Table</div>', unsafe_allow_html=True)
            mom_rows = []
            for i,(m,label) in enumerate(zip(all_months, all_labels)):
                gmv  = all_gmv[i]
                rr   = all_rr_gmv[i]
                prev = all_gmv[i-1] if i>0 else None
                is_inc = not idx_now[m]["stats"].get("is_complete", True)
                mom  = f"{(gmv-prev)/prev*100:+.1f}%" if prev else "–"
                mom_rows.append({
                    "Month":    label,
                    "GMV":      fmt_gmv(gmv),
                    "Run Rate": fmt_gmv(rr) if is_inc else "–",
                    "MoM":      mom,
                })
            tbl_mom = pd.DataFrame(mom_rows)
            def css_mom(v):
                if not isinstance(v,str) or v=="–": return ""
                try:
                    f=float(v.replace("+","").replace("%",""))
                    return "color:#16A34A;font-weight:600" if f>0 else "color:#DC2626;font-weight:600" if f<0 else ""
                except: return ""
            st.dataframe(tbl_mom.style.map(css_mom, subset=["MoM"]),
                         hide_index=True, use_container_width=True)

        # GMV trend line (hide when shop-filtered, not meaningful)
        if not gmv_shop_filt:
            st.markdown('<div class="section-title">GMV by AM รายเดือน (฿M)</div>', unsafe_allow_html=True)
            if len(trend_kam):
                km = trend_kam.copy()
                if gmv_am_filt != "ทั้งหมด": km = km[km["kam"]==gmv_am_filt]
                km_pivot = km.pivot(index="month", columns="kam", values="gmv").fillna(0) / 1e6
                st.line_chart(km_pivot, use_container_width=True, height=200)

        col3,col4 = st.columns(2)
        with col3:
            st.markdown('<div class="section-title">All Shops — GMV by Month (฿K)</div>', unsafe_allow_html=True)

            # Build from per-month shop data (includes ALL shops, not just top 30)
            with st.spinner("โหลดข้อมูลทุกร้าน…"):
                all_shop_rows = []
                for mk in sorted(idx_now.keys()):
                    md_tmp = load_month_data(mk)
                    if not md_tmp: continue
                    for s in md_tmp.get("shops", []):
                        all_shop_rows.append({
                            "organization_name": s.get("organization_name",""),
                            "kam":               s.get("kam",""),
                            "month":             mk,
                            "gmv":               s.get("gmv", 0),
                        })
                ts_all = pd.DataFrame(all_shop_rows) if all_shop_rows else pd.DataFrame()

            if len(ts_all):
                ts = ts_all.copy()
                if gmv_am_filt != "ทั้งหมด": ts = ts[ts["kam"]==gmv_am_filt]
                if gmv_shop_filt: ts = ts[ts["organization_name"].isin(gmv_shop_filt)]

                # Pivot: rows=shops, columns=months — fill 0 for missing months
                tp_raw = ts.pivot_table(index="organization_name", columns="month",
                                        values="gmv", aggfunc="sum", fill_value=0)
                all_mks = sorted(idx_now.keys())
                # Ensure all months appear even if shop had 0 that month
                for mk in all_mks:
                    if mk not in tp_raw.columns:
                        tp_raw[mk] = 0
                tp_raw = tp_raw[all_mks]
                # Sort by latest month GMV desc
                tp_raw = tp_raw.sort_values(all_mks[-1], ascending=False)

                # Build display DataFrame with RR and %chg columns
                display_rows = []
                for shop, row in tp_raw.iterrows():
                    r = {"Shop": shop}
                    for i, mk in enumerate(all_mks):
                        col_lbl = idx_now.get(mk,{}).get("label", mk)
                        actual  = row[mk]
                        # Run rate for incomplete month
                        s_info  = idx_now.get(mk,{}).get("stats",{})
                        cov     = s_info.get("coverage_pct",100) / 100
                        is_inc  = not s_info.get("is_complete", True)
                        rr_val  = actual / cov if is_inc and cov > 0 else actual
                        # MoM vs previous month (use RR for both sides)
                        if i > 0:
                            prev_mk   = all_mks[i-1]
                            prev_info = idx_now.get(prev_mk,{}).get("stats",{})
                            prev_cov  = prev_info.get("coverage_pct",100)/100
                            prev_inc  = not prev_info.get("is_complete",True)
                            prev_rr   = row[prev_mk] / prev_cov if prev_inc and prev_cov > 0 else row[prev_mk]
                            mom_pct   = (rr_val - prev_rr) / prev_rr * 100 if prev_rr > 0 else 0
                            mom_str   = f"{mom_pct:+.0f}%"
                        else:
                            mom_str = "–"

                        rr_str  = f" (RR ฿{int(rr_val):,})" if is_inc else ""
                        r[col_lbl] = f"฿{int(actual):,}{rr_str}"
                        r[f"{col_lbl} Δ"] = mom_str
                    display_rows.append(r)

                disp_df = pd.DataFrame(display_rows).set_index("Shop")

                # Interleave: Jan, Jan Δ, Feb, Feb Δ, ...
                ordered_cols = []
                for mk in all_mks:
                    lbl = idx_now.get(mk,{}).get("label",mk)
                    ordered_cols += [lbl, f"{lbl} Δ"]
                ordered_cols = [c for c in ordered_cols if c in disp_df.columns]
                disp_df = disp_df[ordered_cols]

                def css_delta(v):
                    if not isinstance(v, str) or v == "–": return "color:#aaa"
                    try:
                        f = float(v.replace("%","").replace("+",""))
                        return "color:#3a7d2c;font-weight:500" if f > 0 else "color:#d94040;font-weight:500" if f < 0 else "color:#aaa"
                    except: return ""

                delta_cols = [c for c in ordered_cols if c.endswith(" Δ")]
                styled_tp  = disp_df.style.map(css_delta, subset=delta_cols).set_properties(**{"font-size":"11px"})
                st.dataframe(styled_tp, use_container_width=True, height=420)

        with col4:
            st.markdown('<div class="section-title">All Services — GMV by Month</div>', unsafe_allow_html=True)
            if len(trend_svc):
                sv = trend_svc.copy()
                sv["gmv"] = pd.to_numeric(sv["gmv"], errors="coerce").fillna(0)
                # Apply KAM filter if set (need shop→kam map)
                # Build pivot: rows=service, cols=months
                sv_pivot = sv.pivot_table(
                    index="service_name", columns="month",
                    values="gmv", aggfunc="sum", fill_value=0
                )
                all_mks_svc = sorted(idx_now.keys())
                for mk in all_mks_svc:
                    if mk not in sv_pivot.columns:
                        sv_pivot[mk] = 0
                sv_pivot = sv_pivot[all_mks_svc]
                sv_pivot["Total"] = sv_pivot.sum(axis=1)
                sv_pivot = sv_pivot[sv_pivot["Total"] > 0]
                sv_pivot = sv_pivot.sort_values("Total", ascending=False)
                # Format as full ฿ strings (not K)
                sv_pivot_fmt = sv_pivot.copy()
                # Rename month columns + add Δ
                ordered_cols = []
                prev_mk_s = None
                for mk in all_mks_svc:
                    lbl = idx_now[mk]["label"]
                    sv_pivot[lbl] = sv_pivot[mk]
                    if prev_mk_s:
                        prev_lbl = idx_now[prev_mk_s]["label"]
                        delta_col = f"{lbl} Δ"
                        sv_pivot[delta_col] = sv_pivot[[mk, prev_mk_s]].apply(
                            lambda r: f"{(r[mk]-r[prev_mk_s])/r[prev_mk_s]*100:+.0f}%" if r[prev_mk_s]>0 else ("new" if r[mk]>0 else "–"),
                            axis=1
                        )
                        ordered_cols += [lbl, delta_col]
                    else:
                        ordered_cols += [lbl]
                    prev_mk_s = mk
                ordered_cols += ["Total"]
                sv_pivot = sv_pivot[ordered_cols]
                # Build display df — use sv_pivot which has month keys as columns
                sv_disp = pd.DataFrame(index=sv_pivot.index)
                sv_disp.index.name = "Service"
                _prev_mk_svc = None
                for _mk in all_mks_svc:
                    if _mk not in sv_pivot.columns: continue
                    _lbl   = idx_now.get(_mk,{}).get("label",_mk)
                    _s_inf = idx_now.get(_mk,{}).get("stats",{})
                    _cov   = _s_inf.get("coverage_pct",100)/100
                    _inc   = not _s_inf.get("is_complete",True)
                    _col   = sv_pivot[_mk]
                    sv_disp[_lbl] = _col.apply(lambda x: f"฿{int(x):,}" if x>0 else "–")
                    if _inc and _cov > 0:
                        sv_disp[f"RR {_lbl}"] = _col.apply(lambda x: f"฿{int(x/_cov):,}" if x>0 else "–")
                    if _prev_mk_svc and _prev_mk_svc in sv_pivot.columns:
                        _prev_cov = idx_now.get(_prev_mk_svc,{}).get("stats",{}).get("coverage_pct",100)/100
                        _prev_inc = not idx_now.get(_prev_mk_svc,{}).get("stats",{}).get("is_complete",True)
                        _pc = sv_pivot[_prev_mk_svc]
                        delta_vals = []
                        for c_val, p_val in zip(_col, _pc):
                            c_rr = c_val/_cov if _inc and _cov>0 and c_val>0 else c_val
                            p_rr = p_val/_prev_cov if _prev_inc and _prev_cov>0 and p_val>0 else p_val
                            if p_rr == 0: delta_vals.append("new" if c_val>0 else "–")
                            else: delta_vals.append(f"{(c_rr-p_rr)/p_rr*100:+.0f}%")
                        sv_disp[f"{_lbl} Δ"] = delta_vals
                    _prev_mk_svc = _mk
                sv_disp["Total"] = sv_pivot["Total"].apply(lambda x: f"฿{int(x):,}" if x>0 else "–")

                def css_svc_delta(v):
                    if not isinstance(v,str) or v in ["–","new"]: return "color:#aaa"
                    try:
                        f=float(v.replace("%","").replace("+",""))
                        return "color:#16A34A;font-weight:500" if f>0 else "color:#DC2626;font-weight:500" if f<0 else ""
                    except: return ""
                delta_cols_svc = [c for c in ordered_cols if c.endswith(" Δ")]
                styled_svc = sv_pivot.style.map(css_svc_delta, subset=delta_cols_svc)
                styled_svc = styled_svc.set_properties(**{"font-size":"11px"})
                delta_cols_svc2 = [c for c in sv_disp.columns if c.endswith(" Δ")]
                styled_sv = sv_disp.style.map(css_svc_delta, subset=delta_cols_svc2)
                styled_sv = styled_sv.set_properties(**{"font-size":"11px"})
                st.dataframe(styled_sv, use_container_width=True, height=400)

        # ── Demographics Breakdown ──────────────────────────────────────────
        dm1, dm2 = st.columns([1,2])
        with dm1:
            demo_month_opts = [idx_now[m]["label"] for m in sorted(idx_now.keys())]
            demo_month_sel  = st.selectbox("เดือน", demo_month_opts,
                                            index=demo_month_opts.index(sel_info["label"]) if sel_info["label"] in demo_month_opts else len(demo_month_opts)-1,
                                            key="demo_month")
            demo_mk   = next((m for m in idx_now if idx_now[m]["label"]==demo_month_sel), sel_month)
            demo_mdata = load_month_data(demo_mk) if demo_mk != sel_month else mdata
        st.markdown(f'<div class="section-title">Demographics & Mix — {demo_month_sel}' + (f' · {gmv_am_filt}' if gmv_am_filt!="ทั้งหมด" else '') + '</div>', unsafe_allow_html=True)

        demo_all  = demo_mdata.get("demo", {}) if demo_mdata else {}
        # Select KAM slice
        demo_data = demo_all.get(gmv_am_filt, demo_all.get("all", {})) if demo_all else {}

        if not demo_data:
            st.info("Re-upload ข้อมูลเพื่อดู demographics ครับ")
        else:
            import json as _json

            def pie_html(title, rows, label_key, value_key, colors=None):
                """Build a simple SVG donut chart."""
                total = sum(r[value_key] for r in rows if r.get(value_key,0)>0)
                if total == 0: return f'<div style="color:#aaa;font-size:11px">ไม่มีข้อมูล</div>'
                default_colors = ["#3B8BD4","#E24B4A","#3a7d2c","#c47c10","#9b59b6","#e67e22","#1abc9c","#e91e63","#00bcd4","#8bc34a"]
                rows_sorted = sorted(rows, key=lambda x: x.get(value_key,0), reverse=True)

                # Build SVG donut
                cx,cy,r,ri = 80,80,60,36
                import math
                angle = -math.pi/2
                paths = []
                for i,row in enumerate(rows_sorted):
                    v = row.get(value_key,0)
                    if v <= 0: continue
                    sweep = (v/total)*2*math.pi
                    x1,y1 = cx+r*math.cos(angle), cy+r*math.sin(angle)
                    x2,y2 = cx+r*math.cos(angle+sweep), cy+r*math.sin(angle+sweep)
                    xi1,yi1 = cx+ri*math.cos(angle), cy+ri*math.sin(angle)
                    xi2,yi2 = cx+ri*math.cos(angle+sweep), cy+ri*math.sin(angle+sweep)
                    large = 1 if sweep > math.pi else 0
                    col = (colors or default_colors)[i % len(default_colors)]
                    pct = v/total*100
                    paths.append(f'<path d="M{x1:.1f},{y1:.1f} A{r},{r} 0 {large},1 {x2:.1f},{y2:.1f} L{xi2:.1f},{yi2:.1f} A{ri},{ri} 0 {large},0 {xi1:.1f},{yi1:.1f} Z" fill="{col}" opacity="0.9"><title>{row[label_key]}: {pct:.1f}% ({fmt_gmv(v)})</title></path>')
                    angle += sweep

                svg = f'''<svg width="140" height="140" viewBox="0 0 160 160">
                  {"".join(paths)}
                </svg>'''

                # Legend
                legend = "".join([
                    f'<div style="display:flex;align-items:center;gap:5px;margin-bottom:3px">'
                    f'<div style="width:8px;height:8px;border-radius:2px;background:{(colors or default_colors)[i%len(default_colors)]};flex-shrink:0"></div>'
                    f'<div style="font-size:10px;color:#444;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{row[label_key]}</div>'
                    f'<div style="font-size:10px;color:#888;font-weight:500">{row.get(value_key,0)/total*100:.0f}%</div>'
                    f'</div>'
                    for i,row in enumerate(rows_sorted[:8]) if row.get(value_key,0)>0
                ])

                return f'''<div style="background:#fff;border:1px solid #ebebeb;border-radius:8px;padding:12px 14px">
                  <div style="font-size:10px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:#bbb;margin-bottom:8px">{title}</div>
                  <div style="display:flex;align-items:flex-start;gap:10px">
                    {svg}
                    <div style="flex:1;padding-top:4px">{legend}</div>
                  </div>
                </div>'''

            # Row 1: Gender + Age
            d1c1, d1c2 = st.columns(2)
            with d1c1:
                if "gender" in demo_data:
                    st.markdown(pie_html("Gender", demo_data["gender"], "gender", "gmv",
                                        ["#3B8BD4","#E24B4A","#aaa"]), unsafe_allow_html=True)
            with d1c2:
                if "age" in demo_data:
                    age_rows = [r for r in demo_data["age"] if r["age_group"] not in ["nan","Unknown","None"] and r["gmv"]>0]
                    age_order = ["<25","25–34","35–44","45–54","55+"]
                    age_rows  = sorted(age_rows, key=lambda x: age_order.index(x["age_group"]) if x["age_group"] in age_order else 99)
                    st.markdown(pie_html("Age Group", age_rows, "age_group", "gmv",
                                        ["#1abc9c","#3B8BD4","#9b59b6","#c47c10","#E24B4A"]), unsafe_allow_html=True)

            # Row 2: Payment + Category
            d2c1, d2c2 = st.columns(2)
            with d2c1:
                if "payment" in demo_data:
                    # Clean payment type labels
                    pay_clean = []
                    label_map = {"pay_online":"Online","gbprimepay":"GBPrimePay","shopeepay":"ShopeePay",
                                 "line_pay":"LINE Pay","bank_transfer":"Bank Transfer",
                                 "true_money_2":"TrueMoney","googlepay":"Google Pay",
                                 "alipay_cn":"Alipay","applepay":"Apple Pay","promptpay":"PromptPay"}
                    for r in demo_data["payment"]:
                        pay_clean.append({"payment":label_map.get(r["payment_type"],r["payment_type"]),"gmv":r["gmv"]})
                    st.markdown(pie_html("Payment Type", pay_clean, "payment", "gmv"), unsafe_allow_html=True)
            with d2c2:
                if "category" in demo_data:
                    st.markdown(pie_html("Category", demo_data["category"], "category", "gmv"), unsafe_allow_html=True)

            # Row 3: Subcategory (full width)
            if "subcategory" in demo_data:
                st.markdown(f'<div class="section-title" style="margin-top:8px">Top Subcategories — {demo_month_sel}' + (f' · {gmv_am_filt}' if gmv_am_filt!="ทั้งหมด" else '') + '</div>', unsafe_allow_html=True)
                # Use KAM-filtered demo_data if available
                sub_rows = demo_data.get("subcategory", [])
                total_sub = sum(r["gmv"] for r in sub_rows)
                if total_sub > 0:
                    sub_tbl   = pd.DataFrame(sub_rows).sort_values("gmv",ascending=False)
                    sub_tbl["GMV"]   = sub_tbl["gmv"].apply(fmt_gmv)
                    sub_tbl["Share"] = (sub_tbl["gmv"]/total_sub*100).round(1).apply(lambda x: f"{x:.1f}%")
                    sub_tbl = sub_tbl[["subcategory","GMV","Share"]].rename(columns={"subcategory":"Subcategory"})
                    st.dataframe(sub_tbl, hide_index=True, use_container_width=True, height=min(400, 44+len(sub_tbl)*36))


# ══ TAB 2: Category ════════════════════════════════════════════════════════════
with tab_cat:
    # ── Filters ─────────────────────────────────────────────────────────────
    cf1, cf2, cf3 = st.columns(3)
    with cf1:
        cat_am_filt = st.selectbox("Filter by KAM", ["ทั้งหมด"]+sorted(REAL_AMS), key="cat_am")
    with cf2:
        all_months_cat = sorted(idx_now.keys())
        month_opts_cat = ["ทุกเดือน"] + [idx_now[m]["label"] for m in all_months_cat]
        cat_month_filt = st.selectbox("Filter by Month", month_opts_cat, key="cat_month",
                                      index=len(month_opts_cat)-1)
    with cf3:
        cat_filter_cat = st.selectbox("Filter by Category", ["ทั้งหมด"], key="cat_cat_filt",
                                       disabled=True)  # populated below

    # Resolve month key
    sel_month_cat = None
    for mk in all_months_cat:
        if idx_now[mk]["label"] == cat_month_filt:
            sel_month_cat = mk; break

    # Load month data
    mdata_cat = load_month_data(sel_month_cat) if sel_month_cat else mdata

    # Build category GMV from svc_perf (supports KAM filter properly)
    svc_cat_data = pd.DataFrame((mdata_cat or {}).get("svc_perf", []))
    if len(svc_cat_data) and cat_am_filt != "ทั้งหมด":
        svc_cat_data = svc_cat_data[svc_cat_data["kam"] == cat_am_filt]

    # Build prev month data for comparison
    prev_cat_mk   = all_months_cat[all_months_cat.index(sel_month_cat)-1] if sel_month_cat and all_months_cat.index(sel_month_cat)>0 else None
    prev_cat_data = pd.DataFrame((load_month_data(prev_cat_mk) or {}).get("svc_perf",[])) if prev_cat_mk else pd.DataFrame()
    if len(prev_cat_data) and cat_am_filt != "ทั้งหมด":
        prev_cat_data = prev_cat_data[prev_cat_data["kam"] == cat_am_filt]
    prev_cat_lbl  = idx_now[prev_cat_mk]["label"] if prev_cat_mk else None

    if len(svc_cat_data):
        cat_summary = svc_cat_data.groupby("category").agg(
            gmv=("gmv","sum"), orders=("orders","sum")
        ).reset_index().sort_values("gmv", ascending=False)
        total_gmv_cat = cat_summary["gmv"].sum()
        cat_summary["GMV"]       = cat_summary["gmv"].apply(fmt_gmv)
        cat_summary["Share%"]    = (cat_summary["gmv"]/total_gmv_cat*100).round(1).apply(lambda x: f"{x:.1f}%")

        if len(prev_cat_data):
            prev_cat_sum = prev_cat_data.groupby("category")["gmv"].sum().to_dict()
            cat_summary["Prev GMV"]  = cat_summary["category"].map(lambda c: fmt_gmv(prev_cat_sum.get(c,0)))
            cat_summary["MoM%"]      = cat_summary.apply(
                lambda r: f"{(r['gmv']-prev_cat_sum.get(r['category'],0))/prev_cat_sum.get(r['category'],1)*100:+.0f}%"
                if prev_cat_sum.get(r["category"],0)>0 else "new", axis=1)
            show_cols_cat = ["category","GMV","Share%","Prev GMV","MoM%","orders"]
        else:
            show_cols_cat = ["category","GMV","Share%","orders"]

        lbl_suffix = f" · {cat_am_filt}" if cat_am_filt!="ทั้งหมด" else ""
        st.markdown(f'<div class="section-title">Category Overview — {cat_month_filt or "ทุกเดือน"}{lbl_suffix}</div>', unsafe_allow_html=True)

        def css_mom_cat(v):
            if not isinstance(v,str) or v in ["–","new"]: return "color:#aaa"
            try:
                f=float(v.replace("+","").replace("%",""))
                return "color:#16A34A;font-weight:500" if f>0 else "color:#DC2626;font-weight:500" if f<0 else ""
            except: return ""

        tbl_cat = cat_summary[show_cols_cat].rename(columns={"category":"Category","orders":"Orders"})
        styled_cat = tbl_cat.style.set_properties(**{"font-size":"12px"})
        if "MoM%" in tbl_cat.columns:
            styled_cat = styled_cat.map(css_mom_cat, subset=["MoM%"])
        st.dataframe(styled_cat, hide_index=True, use_container_width=True, height=min(400, 44+len(tbl_cat)*36))

        # ── Subcategory table ──────────────────────────────────────────────
        st.markdown(f'<div class="section-title">Subcategory Detail{lbl_suffix}</div>', unsafe_allow_html=True)
        cat_opts_sub = ["ทั้งหมด"] + sorted(svc_cat_data["category"].unique())
        sub_cat_filt = st.selectbox("Filter by Category", cat_opts_sub, key="sub_cat_sel")
        sub_data = svc_cat_data.copy()
        if sub_cat_filt != "ทั้งหมด":
            sub_data = sub_data[sub_data["category"]==sub_cat_filt]

        if "subcategory" not in sub_data.columns:
            # fallback: use service_name grouping
            sub_sum = sub_data.groupby("service_name").agg(gmv=("gmv","sum"),orders=("orders","sum")).reset_index()
            sub_sum = sub_sum.sort_values("gmv",ascending=False).head(30)
            sub_sum.columns = ["Service","GMV raw","Orders"]
            sub_sum["GMV"]   = sub_sum["GMV raw"].apply(fmt_gmv)
            sub_sum["Share%"]= (sub_sum["GMV raw"]/sub_sum["GMV raw"].sum()*100).round(1).apply(lambda x:f"{x:.1f}%")
            st.dataframe(sub_sum[["Service","GMV","Share%","Orders"]], hide_index=True, use_container_width=True, height=300)
        else:
            sub_sum = sub_data.groupby("subcategory").agg(gmv=("gmv","sum"),orders=("orders","sum")).reset_index().sort_values("gmv",ascending=False)
            sub_total = sub_sum["gmv"].sum()
            if len(prev_cat_data) and "subcategory" in prev_cat_data.columns:
                prev_sub_filt = prev_cat_data[prev_cat_data["category"]==sub_cat_filt] if sub_cat_filt!="ทั้งหมด" else prev_cat_data
                prev_sub_map  = prev_sub_filt.groupby("subcategory")["gmv"].sum().to_dict()
                sub_sum["Prev GMV"] = sub_sum["subcategory"].map(lambda c: fmt_gmv(prev_sub_map.get(c,0)))
                sub_sum["MoM%"]     = sub_sum.apply(lambda r: f"{(r['gmv']-prev_sub_map.get(r['subcategory'],0))/prev_sub_map.get(r['subcategory'],1)*100:+.0f}%" if prev_sub_map.get(r["subcategory"],0)>0 else "new", axis=1)
            sub_sum["GMV"]    = sub_sum["gmv"].apply(fmt_gmv)
            sub_sum["Share%"] = (sub_sum["gmv"]/sub_total*100).round(1).apply(lambda x:f"{x:.1f}%")
            tbl_sub = sub_sum[["subcategory","GMV","Share%"]+([f"Prev GMV","MoM%"] if "MoM%" in sub_sum.columns else [])+["orders"]].rename(columns={"subcategory":"Subcategory","orders":"Orders"})
            styled_sub = tbl_sub.style.set_properties(**{"font-size":"12px"})
            if "MoM%" in tbl_sub.columns:
                styled_sub = styled_sub.map(css_mom_cat, subset=["MoM%"])
            st.dataframe(styled_sub, hide_index=True, use_container_width=True, height=min(400,44+len(tbl_sub)*36))
    else:
        # Fallback to old category data
        cat_full = pd.DataFrame((mdata_cat or {}).get("category",[]))
        if len(cat_full):
            cat_full = cat_full.sort_values("gmv", ascending=False)
            cat_full["GMV"] = cat_full["gmv"].apply(fmt_gmv)
            st.dataframe(cat_full[["category","GMV","orders"]].rename(columns={"category":"Category","orders":"Orders"}),
                         hide_index=True, use_container_width=True, height=300)
        else:
            st.info("Re-upload ข้อมูลเพื่อดู Category detail ครับ")

    if trend.get("category") and cat_am_filt == "ทั้งหมด":
        trend_cat = pd.DataFrame(trend["category"])
        top_cats  = trend_cat.groupby("category")["gmv"].sum().nlargest(8).index
        ct = trend_cat[trend_cat["category"].isin(top_cats)]
        ct_pivot = ct.pivot(index="month", columns="category", values="gmv").fillna(0)/1e6
        ct_pivot.index = [idx_now.get(m,{}).get("label",m) for m in ct_pivot.index]
        st.markdown('<div class="section-title">GMV by Category รายเดือน (฿M)</div>', unsafe_allow_html=True)
        st.line_chart(ct_pivot, use_container_width=True, height=220)


# ══ TAB 3: New User ════════════════════════════════════════════════════════════
with tab_new:
    # Filters
    nu1, nu2 = st.columns(2)
    nu_am_filt   = nu1.selectbox("Filter by KAM",  ["ทั้งหมด"]+sorted(REAL_AMS), key="nu_am")
    nu_sh_opts   = ["ทั้งหมด"] + sorted(shops_df["organization_name"].unique()) if len(shops_df) else ["ทั้งหมด"]
    nu_shop_filt = nu2.selectbox("Filter by Shop", nu_sh_opts, key="nu_shop")

    # Get filtered shop ids for new user stats
    nu_shops = shops_df.copy()
    if nu_am_filt   != "ทั้งหมด": nu_shops = nu_shops[nu_shops["kam"]==nu_am_filt]
    if nu_shop_filt != "ทั้งหมด": nu_shops = nu_shops[nu_shops["organization_name"]==nu_shop_filt]

    nu_total = int(nu_shops["unique_customers"].sum()) if "unique_customers" in nu_shops.columns else stats["unique_customers"]
    nu_new   = int(nu_shops["new_customers"].sum())    if "new_customers"    in nu_shops.columns else stats["new_customers"]

    st.markdown('<div class="section-title">New vs Repeat Customers</div>', unsafe_allow_html=True)
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Total Customers",  f"{nu_total:,}")
    c2.metric("New Customers",    f"{nu_new:,}")
    c3.metric("Repeat Customers", f"{nu_total-nu_new:,}")
    c4.metric("New User %",       f"{nu_new/max(nu_total,1)*100:.1f}%")

    # Trend table — per-AM from Supabase if filtered
    if idx_now:
        st.markdown('<div class="section-title">New User Trend รายเดือน</div>', unsafe_allow_html=True)
        trend_rows = []
        for mk in sorted(idx_now.keys()):
            if nu_am_filt != "ทั้งหมด" or nu_shop_filt != "ทั้งหมด":
                _md = load_month_data(mk)
                if _md:
                    _sh = pd.DataFrame(_md.get("shops",[]))
                    if nu_am_filt   != "ทั้งหมด": _sh = _sh[_sh["kam"]==nu_am_filt]
                    if nu_shop_filt != "ทั้งหมด": _sh = _sh[_sh["organization_name"]==nu_shop_filt]
                    _tc = int(_sh["unique_customers"].sum()) if "unique_customers" in _sh.columns else 0
                    _nc = int(_sh["new_customers"].sum())    if "new_customers"    in _sh.columns else 0
                    _o  = int(_sh["total_orders"].sum())     if "total_orders"     in _sh.columns else 0
                    _g  = int(_sh["gmv"].sum())              if "gmv"              in _sh.columns else 0
                    _cov= idx_now[mk]["stats"].get("coverage_pct",100)/100
                    _inc= not idx_now[mk]["stats"].get("is_complete",True)
                    trend_rows.append({
                        "Month": idx_now[mk]["label"],
                        "Total Cust": f"{_tc:,}",
                        "New": f"{_nc:,}",
                        "Repeat": f"{_tc-_nc:,}",
                        "New%": f"{_nc/max(_tc,1)*100:.1f}%",
                        "Orders": f"{_o:,}",
                        "GMV": fmt_gmv(_g/_cov if _inc and _cov>0 else _g) + (" (RR)" if _inc else ""),
                    })
            else:
                s = idx_now[mk]["stats"]
                trend_rows.append({
                    "Month": idx_now[mk]["label"],
                    "Total Cust": f"{s['unique_customers']:,}",
                    "New": f"{s['new_customers']:,}",
                    "Repeat": f"{s['unique_customers']-s['new_customers']:,}",
                    "New%": f"{s['new_customers']/max(s['unique_customers'],1)*100:.1f}%",
                    "Orders": f"{s['orders']:,}",
                    "GMV": fmt_gmv(s['gmv_run_rate'] if not s.get('is_complete') else s['gmv']) + (" (RR)" if not s.get('is_complete') else ""),
                })
        st.dataframe(pd.DataFrame(trend_rows), hide_index=True, use_container_width=True)

    # Category breakdown
    nu_cat_src = pd.DataFrame((mdata or {}).get("svc_perf",[]))
    if nu_am_filt   != "ทั้งหมด" and len(nu_cat_src): nu_cat_src = nu_cat_src[nu_cat_src["kam"]==nu_am_filt]
    if len(nu_cat_src):
        st.markdown('<div class="section-title">New User % by Category</div>', unsafe_allow_html=True)
        cf2 = pd.DataFrame(mdata.get("category",[])).copy()
        if len(cf2):
            cf2["new_pct"] = (cf2["new_customers"]/cf2["unique_customers"].replace(0,np.nan)*100).round(1).fillna(0)
            cf2 = cf2.sort_values("new_pct", ascending=False)
            st.bar_chart(cf2.set_index("category")["new_pct"], use_container_width=True, height=200)


# ══ TAB 4: Store Health ════════════════════════════════════════════════════════
with tab_health:
    st.markdown(f'<div class="section-title">Priority Stores — {len(shops_df):,} ร้าน ({sel_info["label"]})</div>', unsafe_allow_html=True)

    dcols = {"organization_name":"Shop","kam":"AM","category":"Category",
             "total_orders":"Orders","gmv":"GMV","health_score":"Health",
             "price_score":"Price",
             "view_score":"View MoM","cvr_score":"CVR MoM","priority":"Priority","alerts":"Alerts"}

    tbl = shops_df[[c for c in dcols if c in shops_df.columns]].rename(columns=dcols).copy()

    # Format numbers
    tbl["GMV"]      = shops_df["gmv"].apply(lambda x: f"฿{int(x):,}")
    tbl["Health"]   = shops_df["health_score"].apply(lambda x: f"{x:.1f}")
    tbl["Price"]    = shops_df["price_score"].apply(lambda x: f"{int(x)}")
    tbl["View MoM"] = shops_df["view_score"].apply(lambda x: f"{int(x)}")
    tbl["CVR MoM"]  = shops_df["cvr_score"].apply(lambda x: f"{int(x)}")

    # Short alerts
    def short_alert(a):
        if not isinstance(a, str): return ""
        parts = [p.strip() for p in a.split("|")]
        short = []
        for p in parts:
            if "ราคาสูง"   in p: short.append(f"💰 +{p.split('+')[1].split('%')[0]}%" if '+' in p else "💰 ราคา")
            elif "View ลด"  in p: short.append(f"👁 {p.split('View ลด')[1].split('MoM')[0].strip()}")
            elif "CR% ลด"   in p: short.append(f"📉 {p.split('CR% ลด')[1].split('MoM')[0].strip()}")
            elif "SKU น้อย" in p: short.append(f"📦 {p.split('(')[1].split(')')[0]}" if '(' in p else "📦")
        return " · ".join(short)

    tbl["Alerts"] = shops_df["alerts"].apply(short_alert)

    if "view_mom_pct" in shops_df.columns:
        tbl["View Δ"] = shops_df["view_mom_pct"].apply(
            lambda x: f"{'▲' if x>0 else '▼' if x<0 else '–'}{abs(x):.0f}%" if x!=0 else "–")
    if "cvr_mom_pct" in shops_df.columns:
        tbl["CVR Δ"]  = shops_df["cvr_mom_pct"].apply(
            lambda x: f"{'▲' if x>0 else '▼' if x<0 else '–'}{abs(x):.0f}%" if x!=0 else "–")
    if "avg_cr" in shops_df.columns:
        tbl["CR%"]    = shops_df["avg_cr"].apply(lambda x: f"{x:.2f}%")

    score_cols = ["Health","Price","View MoM","CVR MoM"]

    def css_str(v):
        """Color score columns — values are now strings like '18.2'."""
        try: return f"color:{sc(float(v))};font-weight:500"
        except: return ""

    def cdelta(v):
        """Color delta columns."""
        if not isinstance(v, str): return ""
        return "color:#3a7d2c;font-weight:500" if "▲" in v else "color:#d94040;font-weight:500" if "▼" in v else "color:#aaa"

    styled = tbl.style.map(css_str, subset=score_cols).map(cprio, subset=["Priority"])
    if "View Δ" in tbl.columns:
        styled = styled.map(cdelta, subset=["View Δ"])
    if "CVR Δ" in tbl.columns:
        styled = styled.map(cdelta, subset=["CVR Δ"])
    styled = styled.set_properties(**{"font-size":"12px"})

    st.dataframe(styled, use_container_width=True, height=520)

    d = datetime.now().strftime("%Y%m%d")
    d1,d2 = st.columns(2)
    d1.download_button("⬇ shop_scores.csv", to_csv(shops_df[list(dcols.keys())]), f"shop_scores_{sel_month}.csv","text/csv",use_container_width=True)
    alerts_only = shops_df[shops_df["alert_count"]>0][["organization_name","kam","category","health_score","priority","alert_count","alerts"]]
    d2.download_button("⬇ alerts_only.csv", to_csv(alerts_only), f"alerts_{sel_month}.csv","text/csv",use_container_width=True)


# ══ TAB 5: Action List ════════════════════════════════════════════════════════
with tab_action:
    action_shops = pd.DataFrame(mdata["shops"])
    if sel_am != "ทั้งหมด":
        action_shops = action_shops[action_shops["kam"]==sel_am]
    action_shops = action_shops[action_shops["priority"].isin(["critical","warning"])]
    action_shops = action_shops[action_shops["alert_count"]>0].sort_values("health_score")

    # ── Filters row ──────────────────────────────────────────────────────────
    f_col1, f_col2, f_col3, f_col4 = st.columns(4)

    with f_col1:
        ams_with_issues = ["ทั้งหมด"] + sorted(action_shops["kam"].unique())
        cur_filter = st.selectbox("Filter by AM", ams_with_issues,
                                   key="action_am_dd", label_visibility="visible")

    with f_col2:
        shop_opts = ["ทั้งหมด"] + sorted(action_shops["organization_name"].unique())
        shop_filter = st.selectbox("Filter by Shop", shop_opts,
                                    key="action_shop_dd", label_visibility="visible")

    with f_col3:
        issue_opts = [
            "ทั้งหมด",
            "💰 Price",
            "📦 SKU",
            "👁 View",
            "📉 CVR",
        ]
        issue_filter = st.selectbox("Filter by Issue", issue_opts,
                                     key="action_issue_dd", label_visibility="visible")

    with f_col4:
        sort_by = st.selectbox("Sort by",
            ["Health score (ต่ำสุดก่อน)", "GMV (สูงสุดก่อน)", "GMV (ต่ำสุดก่อน)", "Orders (มากสุดก่อน)", "Shop View (มากสุดก่อน)", "Alert count (มากสุดก่อน)"],
            key="action_sort", label_visibility="visible")

    # Apply filters
    if cur_filter != "ทั้งหมด":
        action_shops = action_shops[action_shops["kam"]==cur_filter]
    if shop_filter != "ทั้งหมด":
        action_shops = action_shops[action_shops["organization_name"]==shop_filter]
    if issue_filter != "ทั้งหมด":
        if "Price" in issue_filter:
            action_shops = action_shops[action_shops["price_score"] < 50]
        elif issue_filter == "📦 SKU":
            action_shops = action_shops[action_shops["sku_score"] < 30]
        elif issue_filter == "👁 View":
            action_shops = action_shops[action_shops["view_score"] < 40]
        elif issue_filter == "📉 CVR":
            action_shops = action_shops[action_shops["cvr_score"] < 40]

    # Apply sort
    sort_map = {
        "Health score (ต่ำสุดก่อน)": ("health_score", True),
        "GMV (สูงสุดก่อน)":           ("gmv", False),
        "GMV (ต่ำสุดก่อน)":           ("gmv", True),
        "Orders (มากสุดก่อน)":         ("total_orders", False),
        "Shop View (มากสุดก่อน)":      ("avg_view", False),
        "Alert count (มากสุดก่อน)":   ("alert_count", False),
    }
    sc_col, sc_asc = sort_map.get(sort_by, ("health_score", True))
    # Force numeric before sort (JSON deserialization may give strings)
    if sc_col in action_shops.columns:
        action_shops[sc_col] = pd.to_numeric(action_shops[sc_col], errors="coerce").fillna(0)
    _act_cov = stats.get("coverage_pct",100)/100
    _act_inc = not stats.get("is_complete",True)
    if _act_inc and _act_cov > 0 and sc_col in ["gmv","avg_view","total_orders"]:
        action_shops = action_shops.assign(
            _sort_rr=action_shops[sc_col] / _act_cov
        ).sort_values("_sort_rr", ascending=sc_asc).drop(columns=["_sort_rr"])
    else:
        action_shops = action_shops.sort_values(sc_col, ascending=sc_asc)

    # ── Task summary for selected KAM ─────────────────────────────────────
    if my_kam_id := st.session_state.get("my_kam_identity","–"):
        if my_kam_id != "–":
            all_c = all_comments_cache
            pending_shops  = [sid for sid,cs in all_c.items() if cs and cs[-1]["status"]=="pending"]
            inprog_shops   = [sid for sid,cs in all_c.items() if cs and cs[-1]["status"]=="in_progress"]
            escalate_shops = [sid for sid,cs in all_c.items() if cs and cs[-1]["status"]=="escalate"]
            done_shops     = [sid for sid,cs in all_c.items() if cs and cs[-1]["status"]=="done"]
            if any([pending_shops, inprog_shops, escalate_shops, done_shops]):
                st.markdown(f"""
                <div style="background:#fff;border:1px solid #ebebeb;border-radius:8px;padding:10px 16px;margin-bottom:12px;display:flex;gap:16px;align-items:center;flex-wrap:wrap">
                  <div style="font-size:10px;font-weight:600;color:#bbb;text-transform:uppercase;letter-spacing:.07em">Task Summary — {my_kam_id}</div>
                  <span style="font-size:11px;background:#FEF2F2;color:#DC2626;padding:3px 10px;border-radius:4px;font-weight:600">⏳ Pending {len(pending_shops)}</span>
                  <span style="font-size:11px;background:#FFFBEB;color:#D97706;padding:3px 10px;border-radius:4px;font-weight:600">⚙️ In Progress {len(inprog_shops)}</span>
                  <span style="font-size:11px;background:#EFF6FF;color:#2563EB;padding:3px 10px;border-radius:4px;font-weight:600">🚨 Escalate {len(escalate_shops)}</span>
                  <span style="font-size:11px;background:#F0FDF4;color:#16A34A;padding:3px 10px;border-radius:4px;font-weight:600">✅ Done {len(done_shops)}</span>
                </div>""", unsafe_allow_html=True)

    st.markdown(f'<div class="section-title">Action List — {len(action_shops)} ร้านที่ต้องดูแล</div>', unsafe_allow_html=True)
    st.caption(f"เรียงตาม {sort_by}")

    # ── Load all comments for this month ────────────────────────────────────
    all_comments_cache = load_all_comments_cached(sel_month)

    # ── Load previous month for MoM comparison ──────────────────────────────
    all_mk   = sorted(idx_now.keys())
    prev_mk  = all_mk[all_mk.index(sel_month)-1] if sel_month in all_mk and all_mk.index(sel_month)>0 else None
    prev_mdata = load_month_data(prev_mk) if prev_mk else None

    # Build prev month shop GMV map: shop_id → gmv
    prev_shop_map = {}
    if prev_mdata:
        for s in prev_mdata.get("shops",[]):
            prev_shop_map[str(s.get("shop_id_str",""))] = {
                "gmv": s.get("gmv",0),
                "total_orders": s.get("total_orders",0),
                "avg_view": s.get("avg_view",0),
                "avg_cr": s.get("avg_cr",0),
            }

    # Build category SKU average map (to say "avg in category is X")
    cat_sku_avg = {}
    all_shops_df2 = pd.DataFrame(mdata["shops"])
    if len(all_shops_df2):
        cat_sku_avg = all_shops_df2.groupby("category")["sku_count"].mean().round(1).to_dict()

    prev_label = idx_now[prev_mk]["label"] if prev_mk else None

    for _, row in action_shops.head(80).iterrows():
        shop_id = str(row.get("shop_id_str", row.get("shop_id","")))
        prev    = prev_shop_map.get(shop_id, {})

        # ── MoM GMV delta ──────────────────────────────────────────────────
        prev_gmv = prev.get("gmv", 0)
        if prev_gmv > 0:
            mom_delta = row["gmv"] - prev_gmv
            mom_pct   = mom_delta / prev_gmv * 100
            if mom_pct < -10:
                mom_str = f"▼ {abs(mom_pct):.0f}% vs {prev_label} (฿{abs(mom_delta)/1e3:.0f}K)"
                mom_color = "#A32D2D"
            elif mom_pct > 10:
                mom_str = f"▲ {mom_pct:.0f}% vs {prev_label}"
                mom_color = "#3B6D11"
            else:
                mom_str = f"ทรงตัว vs {prev_label}"
                mom_color = "#888"
        else:
            mom_str = "ไม่มีข้อมูลเดือนก่อน" if prev_label else ""
            mom_color = "#aaa"

        # ── Build actions with specific numbers ────────────────────────────
        acts = []

        if row["sku_score"] < 30:
            cat_avg = cat_sku_avg.get(row["category"], 0)
            gap     = max(0, round(cat_avg - row["sku_count"]))
            acts.append(("📦 เพิ่ม SKU",
                f"มี {int(row['sku_count'])} SKUs — avg ของ {row['category']} อยู่ที่ {cat_avg:.0f} SKUs "
                f"(ขาดอีก ~{gap} SKUs) → เพิ่ม service หรือ package ใหม่"))

        if row["price_score"] < 50:
            selling   = row.get("selling_price_mean", 0)
            lowest    = row.get("lowest_price_12m",  0)
            baht_diff = selling - lowest if selling and lowest else 0
            # Build per-service detail
            overpriced_svcs = row.get("overpriced_svcs", [])
            if overpriced_svcs:
                svc_lines = []
                for s in overpriced_svcs[:3]:
                    nm   = s["service_name"][:40] + ("…" if len(s["service_name"])>40 else "")
                    diff = s["svc_sell"] - s["svc_low12"]
                    svc_lines.append(f"• {nm}: ฿{s['svc_sell']:,.0f} vs lowest ฿{s['svc_low12']:,.0f} (+{s['svc_pct']:.0f}%, ฿{diff:,.0f})")
                svc_detail = "\n" + "\n".join(svc_lines)
            else:
                svc_detail = f" — avg ฿{selling:,.0f} vs lowest ฿{lowest:,.0f} (+{row['price_above']:.0f}%, ฿{baht_diff:,.0f})"
            acts.append(("💰 ปรับราคา", f"Services ที่แพงกว่า lowest 12m:{svc_detail}"))

        # Operation pillar removed

        if row["view_score"] < 40:
            cur_view  = row.get("avg_view", 0)
            view_mom_pct = row.get("view_mom_pct", 0)
            if view_mom_pct != 0:
                mom_str = f" ({'+' if view_mom_pct>=0 else ''}{view_mom_pct:.0f}% vs เดือนก่อน)"
                emoji   = "📉" if view_mom_pct < 0 else "📈"
            else:
                mom_str = ""; emoji = "📉"
            acts.append((f"👁 เพิ่ม View",
                f"{emoji} Page view {cur_view:,.0f}/เดือน{mom_str} — "
                f"เพิ่มรูปภาพ, ปรับ description, ขอ featured listing หรือ banner"))

        if row["cvr_score"] < 40:
            cr = row.get("avg_cr", 0)
            cvr_mom_pct = row.get("cvr_mom_pct", 0)
            if cvr_mom_pct != 0:
                cr_str = f" ({'+' if cvr_mom_pct>=0 else ''}{cvr_mom_pct:.0f}% vs เดือนก่อน)"
                emoji  = "📉" if cvr_mom_pct < 0 else "📈"
            else:
                cr_str = ""; emoji = "📉"
            acts.append((f"📉 ปรับ CVR",
                f"{emoji} CR% {cr:.2f}%{cr_str} — "
                f"ตรวจ: รูปภาพ, description, ราคา, reviews และ response time"))

        # ── Render card ────────────────────────────────────────────────────
        priority_color = "#FCEBEB" if row["priority"]=="critical" else "#FAEEDA"
        priority_text  = "#A32D2D" if row["priority"]=="critical" else "#854F0B"

        with st.container():
            st.markdown(f"""
            <div style="background:#fff;border:1px solid #ebebeb;border-radius:8px;padding:14px 18px;margin-bottom:6px">
              <div style="display:flex;align-items:flex-start;gap:10px;margin-bottom:6px">
                <div style="background:{priority_color};color:{priority_text};font-size:10px;padding:2px 8px;border-radius:8px;font-weight:500;white-space:nowrap;margin-top:1px">{row['priority']}</div>
                <div style="flex:1">
                  <div style="font-weight:500;font-size:13px">{row['organization_name']}</div>
                  <div style="font-size:10px;color:#aaa">{row['category']} · {row['kam']} · GMV {fmt_gmv(row['gmv'])}
                    <span style="color:{mom_color};margin-left:6px">{mom_str}</span>
                  </div>
                </div>
                <div style="text-align:right">
                  <div style="font-size:18px;font-weight:600;color:{sc(row['health_score'])}">{row['health_score']}</div>
                </div>
              </div>
              <div style="display:flex;gap:4px;margin-bottom:6px;flex-wrap:wrap">
                {''.join([f'<div style="text-align:center;padding:2px 8px;border-radius:5px;background:#f5f3ef"><div style="font-size:8px;color:#aaa">{n}</div><div style="font-size:12px;font-weight:600;color:{sc(row[k])}">{int(row[k])}</div></div>' for k,n in zip(["sku_score","price_score","view_score","cvr_score"],["SKU","Price","View","CVR"])])}
              </div>
            </div>
            """, unsafe_allow_html=True)
            if acts:
                for icon_label, detail in acts:
                    lines = detail.split("\n")
                    if len(lines) == 1:
                        st.markdown(f"→ **{icon_label}** — {detail}")
                    else:
                        st.markdown(f"→ **{icon_label}** — {lines[0]}")
                        for line in lines[1:]:
                            st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;{line}")
            st.markdown("---")

    if len(action_shops) > 80:
        st.caption(f"แสดง 80 อันดับแรก จาก {len(action_shops)} ร้าน")
    d3 = datetime.now().strftime("%Y%m%d")
    st.download_button("⬇ action_list.csv", to_csv(action_shops[["organization_name","kam","category","health_score","priority","alert_count","alerts"]]), f"action_list_{sel_month}.csv","text/csv")


# ══ TAB 6: SKU Analysis ══════════════════════════════════════════════════════
with tab_sku:
    sku_mdata = load_month_data(sel_month)
    svc_perf_data = sku_mdata.get("svc_perf", []) if sku_mdata else []
    shops_sku     = pd.DataFrame(sku_mdata.get("shops", [])) if sku_mdata else pd.DataFrame()

    # Filters
    sk1, sk2, sk3 = st.columns(3)
    sku_am_filt  = sk1.selectbox("KAM",      ["ทั้งหมด"]+sorted(REAL_AMS), key="sku_am")
    all_cats_sku = sorted(set(s.get("category","") for s in svc_perf_data) - {""})
    sku_cat_filt = sk2.selectbox("Category", ["ทั้งหมด"]+all_cats_sku, key="sku_cat")
    sku_search   = sk3.text_input("ค้นหาร้าน / service", placeholder="…", key="sku_search")

    if not svc_perf_data:
        st.info("Re-upload ข้อมูลเพื่อใช้ SKU Analysis ครับ")
    else:
        sdf = pd.DataFrame(svc_perf_data)
        if sku_am_filt  != "ทั้งหมด": sdf = sdf[sdf["kam"]==sku_am_filt]
        if sku_cat_filt != "ทั้งหมด": sdf = sdf[sdf["category"]==sku_cat_filt]
        if sku_search:
            q = sku_search.lower()
            sdf = sdf[sdf["organization_name"].str.lower().str.contains(q) |
                      sdf["service_name"].str.lower().str.contains(q)]

        # ── Section 1: Top/Bottom Services ───────────────────────────────────
        st.markdown('<div class="section-title">Top / Bottom Services</div>', unsafe_allow_html=True)
        s1c1, s1c2 = st.columns(2)

        with s1c1:
            st.caption("🏆 Top 20 Services — GMV")
            top_svc = sdf.groupby("service_name").agg(gmv=("gmv","sum"), orders=("orders","sum"),
                shops=("shop_id_s","nunique"), basket=("basket","mean")).reset_index()
            top_svc = top_svc.nlargest(20,"gmv").copy()
            top_svc["gmv"]    = top_svc["gmv"].apply(fmt_gmv)
            top_svc["basket"] = top_svc["basket"].apply(lambda x: f"฿{x:,.0f}")
            top_svc.columns   = ["Service","GMV","Orders","Shops","Avg Basket"]
            st.dataframe(top_svc, hide_index=True, use_container_width=True, height=340)

        with s1c2:
            st.caption("📉 Bottom 20 Services — GMV (ขายน้อย แต่มี listing)")
            bot_svc = sdf[sdf["orders"]>0].groupby("service_name").agg(
                gmv=("gmv","sum"), orders=("orders","sum"), shops=("shop_id_s","nunique")).reset_index()
            bot_svc = bot_svc.nsmallest(20,"gmv").copy()
            bot_svc["gmv"] = bot_svc["gmv"].apply(fmt_gmv)
            bot_svc.columns = ["Service","GMV","Orders","Shops"]
            st.dataframe(bot_svc, hide_index=True, use_container_width=True, height=340)

        # ── Section 2: Price Analysis ─────────────────────────────────────────
        st.markdown('<div class="section-title">Price Analysis</div>', unsafe_allow_html=True)
        p1, p2, p3 = st.columns(3)
        total_svc = len(sdf)
        over30 = (sdf["pct"] > 30).sum()
        over15 = ((sdf["pct"] > 15) & (sdf["pct"] <= 30)).sum()
        fair   = (sdf["pct"].between(-5, 15)).sum()
        under  = (sdf["pct"] < -5).sum()
        p1.metric("แพงกว่า lowest >30%", f"{over30:,}", f"{over30/total_svc*100:.0f}% of services")
        p2.metric("แพงกว่า 15–30%",      f"{over15:,}", f"{over15/total_svc*100:.0f}% of services")
        p3.metric("ราคาดี (±15%)",       f"{fair:,}",   f"{fair/total_svc*100:.0f}% of services")

        st.caption("Services ที่ราคาสูงกว่า lowest 12m มากที่สุด (Top 30)")
        price_top = sdf[sdf["pct"]>0].nlargest(30,"pct")[
            ["service_name","organization_name","kam","category","sell","low12","pct","gmv","orders"]].copy()
        price_top["sell"]  = price_top["sell"].apply(lambda x: f"฿{x:,.0f}")
        price_top["low12"] = price_top["low12"].apply(lambda x: f"฿{x:,.0f}")
        price_top["pct"]   = price_top["pct"].apply(lambda x: f"+{x:.0f}%")
        price_top["gmv"]   = price_top["gmv"].apply(fmt_gmv)
        price_top.columns  = ["Service","Shop","AM","Category","Selling","Lowest 12m","Above%","GMV","Orders"]

        def css_above2(v):
            if not isinstance(v,str): return ""
            try:
                f = float(v.replace("+","").replace("%",""))
                return "color:#DC2626;font-weight:600" if f>=30 else "color:#D97706;font-weight:500" if f>=15 else ""
            except: return ""
        st.dataframe(price_top.style.map(css_above2, subset=["Above%"]).set_properties(**{"font-size":"11px"}),
                     hide_index=True, use_container_width=True, height=320)

        # ── Section 3: SKU Gap Analysis ───────────────────────────────────────
        st.markdown('<div class="section-title">SKU Gap — ร้านที่มี SKU น้อยกว่า Category Average</div>', unsafe_allow_html=True)
        if len(shops_sku):
            sks = shops_sku.copy()
            if sku_am_filt  != "ทั้งหมด": sks = sks[sks["kam"]==sku_am_filt]
            if sku_cat_filt != "ทั้งหมด": sks = sks[sks["category"]==sku_cat_filt]
            if sku_search:   sks = sks[sks["organization_name"].str.lower().str.contains(sku_search.lower(),na=False)]

            gap_df = sks[sks.get("sku_gap", pd.Series(dtype=float)).fillna(0) > 0 if "sku_gap" in sks.columns else sks["sku_score"]<50].copy() if "sku_gap" in sks.columns else sks[sks["sku_score"]<50].copy()
            if "sku_gap" in gap_df.columns:
                gap_df = gap_df.sort_values("sku_gap", ascending=False)
                show_cols = ["organization_name","kam","category","sku_count","cat_avg_sku","sku_gap","gmv"]
                avail = [c for c in show_cols if c in gap_df.columns]
                gap_tbl = gap_df[avail].copy()
                gap_tbl["gmv"] = gap_tbl["gmv"].apply(fmt_gmv)
                rename_map = {"organization_name":"Shop","kam":"AM","category":"Category",
                              "sku_count":"SKUs","cat_avg_sku":"Cat Avg","sku_gap":"Gap","gmv":"GMV"}
                gap_tbl = gap_tbl.rename(columns=rename_map)
                def css_gap(v):
                    try: return "color:#D97706;font-weight:500" if float(v)>5 else "color:#F59E0B" if float(v)>2 else ""
                    except: return ""
                st.dataframe(gap_tbl.style.map(css_gap, subset=["Gap"]).set_properties(**{"font-size":"11px"}),
                             hide_index=True, use_container_width=True, height=320)
            else:
                st.info("Re-upload เพื่อดู SKU gap ครับ")

        # ── Section 4: Concentration Risk ─────────────────────────────────────
        st.markdown('<div class="section-title">SKU Concentration Risk — ร้านที่พึ่งพา SKU เดียวสูง</div>', unsafe_allow_html=True)
        if len(shops_sku) and "top_sku_pct" in shops_sku.columns:
            sks2 = shops_sku.copy()
            if sku_am_filt  != "ทั้งหมด": sks2 = sks2[sks2["kam"]==sku_am_filt]
            if sku_cat_filt != "ทั้งหมด": sks2 = sks2[sks2["category"]==sku_cat_filt]
            risk = sks2[sks2["top_sku_pct"]>=70].sort_values("top_sku_pct", ascending=False)
            r_cols = ["organization_name","kam","category","gmv","sku_count","top_sku","top_sku_pct"]
            avail2 = [c for c in r_cols if c in risk.columns]
            risk_tbl = risk[avail2].copy()
            risk_tbl["gmv"] = risk_tbl["gmv"].apply(fmt_gmv)
            risk_tbl["top_sku_pct"] = risk_tbl["top_sku_pct"].apply(lambda x: f"{x:.0f}%")
            rename2 = {"organization_name":"Shop","kam":"AM","category":"Category","gmv":"GMV",
                       "sku_count":"SKUs","top_sku":"Top SKU","top_sku_pct":"Top SKU GMV%"}
            risk_tbl = risk_tbl.rename(columns=rename2)
            def css_conc(v):
                if not isinstance(v,str): return ""
                try:
                    f = float(v.replace("%",""))
                    return "color:#DC2626;font-weight:700" if f>=90 else "color:#D97706;font-weight:500" if f>=70 else ""
                except: return ""
            st.dataframe(risk_tbl.style.map(css_conc, subset=["Top SKU GMV%"]).set_properties(**{"font-size":"11px"}),
                         hide_index=True, use_container_width=True, height=320)
            n90 = (sks2["top_sku_pct"]>=90).sum()
            n70 = ((sks2["top_sku_pct"]>=70)&(sks2["top_sku_pct"]<90)).sum()
            st.caption(f"🔴 {n90} ร้านที่ top SKU มี GMV >90%  ·  🟡 {n70} ร้านที่ 70–90%")

            st.download_button("⬇ sku_concentration.csv", to_csv(risk[avail2]),
                               f"sku_concentration_{sel_month}.csv", "text/csv")
        else:
            st.info("Re-upload เพื่อดู SKU concentration ครับ")

# ══ TAB 7: Portfolio Review ══════════════════════════════════════════════════
with tab_portfolio:

    # ── Filters ──────────────────────────────────────────────────────────────
    pf1, pf2, pf3 = st.columns([2,2,1])
    pf_am   = pf1.selectbox("KAM", sorted(REAL_AMS), key="pf_am")
    pf_month_opts = [idx_now[m]["label"] for m in sorted(idx_now.keys())]
    pf_month_sel  = pf2.selectbox("เดือนที่ต้องการ", pf_month_opts,
                                   index=len(pf_month_opts)-1, key="pf_month")
    pf_mk  = next((m for m in idx_now if idx_now[m]["label"]==pf_month_sel), sel_month)
    pf_keys = sorted(idx_now.keys())
    pf_prev_mk = pf_keys[pf_keys.index(pf_mk)-1] if pf_keys.index(pf_mk) > 0 else None
    pf_prev_lbl = idx_now[pf_prev_mk]["label"] if pf_prev_mk else None
    pf_cov      = idx_now[pf_mk]["stats"].get("coverage_pct", 100) / 100
    pf_is_rr    = not idx_now[pf_mk]["stats"].get("is_complete", True)

    if not pf_prev_mk:
        st.info("ต้องมีข้อมูลอย่างน้อย 2 เดือนเพื่อดู Portfolio Review ครับ")
    else:
        # ── Load data ─────────────────────────────────────────────────────────
        pf_cur  = load_month_data(pf_mk)
        pf_prev = load_month_data(pf_prev_mk)

        if not pf_cur or not pf_prev:
            st.error("โหลดข้อมูลไม่ได้ กรุณา refresh")
        else:
            cur_shops  = pd.DataFrame(pf_cur["shops"])
            prev_shops = pd.DataFrame(pf_prev["shops"])

            # Filter to selected KAM
            cur_am  = cur_shops[cur_shops["kam"]==pf_am].copy()
            prev_am = prev_shops[prev_shops["kam"]==pf_am].copy()

            if len(cur_am) == 0:
                st.warning(f"ไม่พบข้อมูลร้านของ {pf_am} ใน {pf_month_sel}")
            else:
                # Coverage for run rate
                cur_cov  = idx_now[pf_mk]["stats"].get("coverage_pct",100)/100
                cur_is_rr= not idx_now[pf_mk]["stats"].get("is_complete",True)
                prev_cov = idx_now[pf_prev_mk]["stats"].get("coverage_pct",100)/100
                prev_is_rr = not idx_now[pf_prev_mk]["stats"].get("is_complete",True)

                # Merge cur and prev on organization_name (most reliable cross-month join)
                prev_cols = ["organization_name","gmv","total_orders","avg_view","avg_cr","price_above",
                             "health_score","sku_score","price_score","view_score","cvr_score",
                             "alerts","priority","sku_count","category","shop_id_s"]
                prev_cols = [c for c in prev_cols if c in prev_am.columns]
                prev_rename = {c: f"{c}_prev" for c in prev_cols if c != "organization_name"}
                prev_for_merge = prev_am[prev_cols].rename(columns=prev_rename).copy()

                merged = cur_am.merge(prev_for_merge, on="organization_name", how="left")

                # Compute MoM metrics per shop
                rows = []
                for _, r in merged.iterrows():

                    # GMV with run rate
                    cur_gmv  = float(r.get("gmv",0)) / cur_cov  if cur_is_rr  and cur_cov  > 0 else float(r.get("gmv",0))
                    _p_gmv   = float(r.get("gmv_prev",0) or 0)
                    prev_gmv = _p_gmv / prev_cov if prev_is_rr and prev_cov > 0 else _p_gmv
                    gmv_pct  = (cur_gmv-prev_gmv)/prev_gmv*100 if prev_gmv > 0 else None

                    # View MoM — use prev month view as fallback if cur month has no view data
                    _has_view = idx_now.get(pf_mk,{}).get("stats",{}).get("has_view_data", True)
                    cur_view  = float(r.get("avg_view",0) or 0) / cur_cov  if cur_is_rr  else float(r.get("avg_view",0) or 0)
                    prev_view = float(r.get("avg_view_prev",0) or 0) / prev_cov if prev_is_rr else float(r.get("avg_view_prev",0) or 0)
                    if not _has_view and prev_view > 0:
                        # No view data for current month — show prev as reference
                        cur_view = None
                    view_pct  = (cur_view-prev_view)/prev_view*100 if (cur_view and prev_view > 0) else None

                    # CVR MoM
                    _has_cr  = _has_view
                    cur_cr   = float(r.get("avg_cr",0) or 0) if not (not _has_cr) else None
                    prev_cr  = float(r.get("avg_cr_prev",0) or 0)
                    if not _has_cr:
                        cur_cr = None
                    cr_pct   = (cur_cr-prev_cr)/prev_cr*100 if (cur_cr and prev_cr > 0) else None
                    cr_pp    = (cur_cr - prev_cr) if (cur_cr is not None and prev_cr) else None

                    # Price: % SKUs cheaper (price_score improved = lower price_above)
                    cur_pa   = float(r.get("price_above",0) or 0)
                    prev_pa  = float(r.get("price_above_prev",0) or 0)
                    price_dir = "ถูกลง" if cur_pa < prev_pa else "แพงขึ้น" if cur_pa > prev_pa else "เท่าเดิม"
                    price_chg = prev_pa - cur_pa  # positive = cheaper

                    rows.append({
                        "shop_id":      str(r.get("shop_id_s","")),
                        "name":         r["organization_name"],
                        "category":     r.get("category",""),
                        "cur_gmv":      int(cur_gmv),
                        "prev_gmv":     int(prev_gmv),
                        "gmv_pct":      gmv_pct,
                        "cur_view":     cur_view,
                        "prev_view":    prev_view,
                        "view_pct":     view_pct,
                        "cur_cr":       cur_cr,
                        "prev_cr":      prev_cr,
                        "cr_pct":       cr_pct,
                        "cr_pp":        cr_pp,
                        "price_above":  cur_pa,
                        "price_chg":    price_chg,
                        "price_dir":    price_dir,
                        "sku_count":    int(float(r.get("sku_count",0) or 0)),
                        "health":       float(r.get("health_score",0) or 0),
                        "priority":     str(r.get("priority","") or ""),
                        "alerts":       str(r.get("alerts","") or ""),
                    })

                pf_df = pd.DataFrame(rows)
                pf_df_valid = pf_df[pf_df["prev_gmv"] > 0].copy()

                # Sort by absolute ฿ change (RR-adjusted)
                _pf_cov3  = pf_cov if pf_cov > 0 else 1.0
                pf_df_valid["gmv_rr"]      = pf_df_valid["cur_gmv"] / _pf_cov3 if pf_is_rr else pf_df_valid["cur_gmv"]
                pf_df_valid["gmv_abs_chg"] = pf_df_valid["gmv_rr"] - pf_df_valid["prev_gmv"]
                top10_growth = pf_df_valid.nlargest(10,  "gmv_abs_chg")
                top10_drop   = pf_df_valid.nsmallest(10, "gmv_abs_chg")

                # ── Load service-level data for SKU breakdown ─────────────────
                cur_svc_df  = pd.DataFrame((pf_cur  or {}).get("svc_perf", []))
                prev_svc_df = pd.DataFrame((pf_prev or {}).get("svc_perf", []))

                def get_sku_breakdown(shop_id, n=5):
                    """Top N services by |GMV change| for a shop."""
                    if len(cur_svc_df) == 0: return []
                    c = cur_svc_df[cur_svc_df["shop_id_s"]==shop_id].copy()
                    if len(c) == 0: return []
                    if len(prev_svc_df):
                        p = prev_svc_df[prev_svc_df["shop_id_s"]==shop_id].set_index("service_name")
                    else:
                        p = pd.DataFrame()
                    rows = []
                    for _, sc_row in c.iterrows():
                        svc   = sc_row["service_name"]
                        c_gmv = float(sc_row.get("gmv",0))
                        c_sell= float(sc_row.get("sell",0))
                        c_ord = int(sc_row.get("orders",0))
                        if len(p) and svc in p.index:
                            pr    = p.loc[svc]
                            p_gmv = float(pr.get("gmv",0))
                            p_sell= float(pr.get("sell",0))
                        else:
                            p_gmv=0; p_sell=0
                        c_sell_min = float(sc_row.get("sell_min", c_sell))
                        c_sell_max = float(sc_row.get("sell_max", c_sell))
                        if len(p) and svc in p.index:
                            p_sell_min = float(pr.get("sell_min", p_sell))
                            p_sell_max = float(pr.get("sell_max", p_sell))
                        else:
                            p_sell_min = 0; p_sell_max = 0
                        rows.append({
                            "svc":       svc,
                            "c_gmv":     c_gmv,      "p_gmv":     p_gmv,
                            "c_sell":    c_sell,      "p_sell":    p_sell,
                            "c_sell_min":c_sell_min,  "c_sell_max":c_sell_max,
                            "p_sell_min":p_sell_min,  "p_sell_max":p_sell_max,
                            "orders":    c_ord,
                            "is_new":    p_gmv == 0 and c_gmv > 0,
                            "price_up":  (not (p_gmv == 0 and c_gmv > 0)) and p_sell_min > 0 and c_sell_min > p_sell_min,
                        })
                    rows.sort(key=lambda x: abs(x["c_gmv"]-x["p_gmv"]), reverse=True)
                    return rows  # return all, caller slices

                def render_sku_breakdown(skus, is_growth=True):
                    if not skus: return ""
                    # Split into growing vs declining services
                    growing  = sorted([s for s in skus if s["c_gmv"]-s["p_gmv"] > 0],  key=lambda x: -(x["c_gmv"]-x["p_gmv"]))
                    dropping = sorted([s for s in skus if s["c_gmv"]-s["p_gmv"] < 0],  key=lambda x: x["c_gmv"]-x["p_gmv"])
                    new_svcs = [s for s in skus if s["is_new"] and s["c_gmv"] > 0]

                    def svc_row(s, show_sign=True):
                        name    = s["svc"][:38] + ("…" if len(s["svc"])>38 else "")
                        c_gmv   = s["c_gmv"]
                        p_gmv   = s["p_gmv"]
                        # Apply run rate to current month GMV
                        c_gmv_rr = c_gmv / _pf_cov3 if pf_is_rr and _pf_cov3 > 0 and c_gmv > 0 else c_gmv
                        gd      = c_gmv_rr - p_gmv  # RR-adjusted change
                        g_col   = "#16A34A" if gd >= 0 else "#DC2626"
                        g_sym   = "▲" if gd >= 0 else "▼"
                        if s["is_new"]:
                            g_str = f"฿{int(c_gmv_rr):,}"
                            pct_str = ""
                        else:
                            g_str = f"{g_sym}฿{abs(int(gd)):,}"
                            pct_str = (f'<span style="font-size:9px;color:{g_col};margin-left:4px">' +
                                       f'({g_sym}{abs(gd)/p_gmv*100:.0f}%)</span>') if p_gmv > 0 else ""
                        new_b = ' <span style="font-size:8px;background:#EFF6FF;color:#2563EB;padding:1px 4px;border-radius:2px">NEW</span>' if s["is_new"] else ""
                        return (f'<div style="display:flex;justify-content:space-between;align-items:center;'
                                f'padding:3px 0;border-bottom:1px solid #f5f5f5;font-size:10px">'
                                f'<span style="color:#555;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{name}{new_b}</span>'
                                f'<span style="color:{g_col};font-weight:600;white-space:nowrap;margin-left:8px">{g_str}</span>'
                                f'{pct_str}</div>')

                    # Total growth and drop for contribution %
                    total_growth_gmv = sum(s["c_gmv"]/_pf_cov3 - s["p_gmv"] for s in growing if s["p_gmv"]>0) +                                        sum(s["c_gmv"]/_pf_cov3 for s in new_svcs)
                    total_drop_gmv   = abs(sum(s["c_gmv"]/_pf_cov3 - s["p_gmv"] for s in dropping if s["p_gmv"]>0))

                    def contrib_badge(s, total_pool):
                        c_rr = s["c_gmv"]/_pf_cov3 if pf_is_rr and _pf_cov3>0 and s["c_gmv"]>0 else s["c_gmv"]
                        if s["is_new"]:
                            delta = c_rr
                        else:
                            delta = abs(c_rr - s["p_gmv"])
                        pct = delta/total_pool*100 if total_pool>0 else 0
                        return f'<span style="font-size:8px;background:#f0f0f0;color:#666;padding:1px 4px;border-radius:3px;margin-left:4px">{pct:.0f}% of change</span>' if pct >= 5 else ""

                    html = '<div style="margin-top:6px;padding:8px 10px;background:#F8FAFF;border-radius:6px;border:1px solid #E8F0FE">'
                    html += '<div style="font-size:9px;font-weight:600;color:#3B8BD4;text-transform:uppercase;letter-spacing:.06em;margin-bottom:4px">Service Breakdown</div>'

                    if growing[:3]:
                        html += '<div style="font-size:9px;color:#16A34A;font-weight:600;margin:4px 0 2px">📈 ขายดีขึ้น</div>'
                        for s in growing[:3]:
                            row_html = svc_row(s)
                            badge    = contrib_badge(s, total_growth_gmv)
                            # Insert badge before closing </div>
                            html += row_html.replace("</div>", badge+"</div>", 1) if badge else row_html
                    if new_svcs[:2]:
                        html += '<div style="font-size:9px;color:#2563EB;font-weight:600;margin:4px 0 2px">✨ Service ใหม่</div>'
                        for s in new_svcs[:2]:
                            row_html = svc_row(s)
                            badge    = contrib_badge(s, total_growth_gmv)
                            html += row_html.replace("</div>", badge+"</div>", 1) if badge else row_html
                    if dropping[:3]:
                        html += '<div style="font-size:9px;color:#DC2626;font-weight:600;margin:4px 0 2px">📉 ขายแย่ลง</div>'
                        for s in dropping[:3]:
                            row_html = svc_row(s)
                            badge    = contrib_badge(s, total_drop_gmv)
                            html += row_html.replace("</div>", badge+"</div>", 1) if badge else row_html

                    html += '</div>'
                    return html

                # ── Generate auto-comment ─────────────────────────────────────
                def gen_comment(row, is_growth=True):
                    tips = []
                    g    = row.get("gmv_pct")
                    vp   = row.get("view_pct")
                    crp  = row.get("cr_pct")
                    pa   = row.get("price_above",0)
                    skus = row.get("sku_count",0)
                    abs_chg = row.get("gmv_abs_chg",0)

                    if is_growth:
                        # Explain WHY it grew
                        drivers = []
                        if vp is not None and vp > 15:
                            drivers.append(f"View ▲{vp:.0f}%")
                        if crp is not None and crp > 15:
                            drivers.append(f"CVR ▲{crp:.0f}%")
                        if pa < 10:
                            drivers.append("ราคาแข่งได้")
                        if drivers:
                            tips.append("เติบโตจาก: " + " + ".join(drivers))
                        else:
                            tips.append("New service หรือ promo — ตรวจ service breakdown")
                        if pa > 30:
                            tips.append(f"⚠️ ราคายังสูงกว่า floor +{pa:.0f}%")
                        if skus <= 1:
                            tips.append("⚠️ Single SKU — เพิ่ม service เพื่อรักษา")
                    else:
                        # Explain WHY it dropped
                        if vp is not None and vp < -15:
                            tips.append(f"View ▼{abs(vp):.0f}% — เพิ่มรูป/content")
                        if crp is not None and crp < -15:
                            tips.append(f"CVR ▼{abs(crp):.0f}% — ตรวจ listing/ราคา")
                        if pa > 30:
                            tips.append(f"ราคาสูง +{pa:.0f}% vs floor")
                        if skus <= 1:
                            tips.append("Single SKU — เพิ่ม service")
                        if not tips:
                            tips.append("ตรวจ service breakdown")
                    return " · ".join(tips[:3])

                # ── Render card helper ────────────────────────────────────────
                def arrow_pct(v, good_if_positive=True):
                    if v is None: return '<span style="color:#ccc">–</span>'
                    color = ("#16A34A" if v>=0 else "#DC2626") if good_if_positive else ("#DC2626" if v>=0 else "#16A34A")
                    arrow = "▲" if v>=0 else "▼"
                    return f'<span style="color:{color};font-weight:600">{arrow}{abs(v):.0f}%</span>'

                def render_table(df, title, title_color, is_growth=True):
                    st.markdown(
                        f'<div style="font-size:11px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;'
                        f'color:{title_color};margin-bottom:10px">{title}</div>',
                        unsafe_allow_html=True
                    )
                    # Sort by RR GMV desc — cur_gmv already RR-adjusted in rows building
                    df = df.copy()
                    df["_sort_gmv"] = pd.to_numeric(df["cur_gmv"], errors="coerce").fillna(0)
                    df = df.sort_values("_sort_gmv", ascending=False)
                    for rank, (_, row) in enumerate(df.iterrows(), 1):
                        comment = gen_comment(row, is_growth=is_growth)
                        pri_bg  = "#FEF2F2" if row["priority"]=="critical" else "#FFFBEB" if row["priority"]=="warning" else "#F0FDF4"
                        pri_c   = "#DC2626"  if row["priority"]=="critical" else "#D97706"  if row["priority"]=="warning" else "#16A34A"
                        health_c = sc(row["health"])

                        _pf_has_v = idx_now.get(pf_mk,{}).get("stats",{}).get("has_view_data",True)
                        view_str = (f'{int(row["cur_view"]):,}' if row["cur_view"] else
                                    (f'({int(row.get("prev_view",0)):,} Apr)' if row.get("prev_view") else "–"))
                        cr_str   = (f'{row["cur_cr"]:.2f}%' if row.get("cur_cr") else
                                    (f'({row.get("prev_cr",0):.2f}% Apr)' if row.get("prev_cr") else "–"))

                        price_color = "#1a1a1a"
                        price_str   = ""  # removed price vs floor %

                        all_skus  = get_sku_breakdown(row.get("shop_id",""))
                        skus      = all_skus[:5]
                        sku_html  = render_sku_breakdown(skus, is_growth=is_growth)

                        # % SKUs more expensive than last month
                        existing_skus = [s for s in all_skus if not s["is_new"] and s["p_sell"] > 0]
                        n_total   = len(existing_skus)
                        n_up      = sum(1 for s in existing_skus if s["price_up"])
                        if n_total > 0:
                            pct_up    = n_up / n_total * 100
                            if pct_up >= 50:
                                price_sku_str = f'<div style="font-size:9px;color:#DC2626;margin-top:2px">{n_up}/{n_total} SKUs แพงขึ้น ({pct_up:.0f}%)</div>'
                            elif pct_up > 0:
                                price_sku_str = f'<div style="font-size:9px;color:#D97706;margin-top:2px">{n_up}/{n_total} SKUs แพงขึ้น ({pct_up:.0f}%)</div>'
                            else:
                                price_sku_str = f'<div style="font-size:9px;color:#16A34A;margin-top:2px">✓ ราคาไม่ขึ้นจากเดือนก่อน</div>'
                        else:
                            price_sku_str = ""



                        st.markdown(f"""
<div style="background:#fff;border:1px solid #e8e8e8;border-radius:8px;padding:12px 16px;margin-bottom:8px;page-break-inside:avoid">
  <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">
    <div style="width:22px;height:22px;border-radius:50%;background:#f0f0f0;display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;flex-shrink:0;color:#555">{rank}</div>
    <div style="flex:1;min-width:0">
      <div style="font-size:13px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{row["name"]}</div>
      <div style="font-size:10px;color:#aaa">{row["category"]} · GMV {fmt_gmv(row["cur_gmv"])}
        <span style="margin-left:6px">{arrow_pct(row["gmv_pct"])}</span>
        <span style="font-size:9px;color:#bbb;margin-left:4px">vs {pf_prev_lbl}</span>
      </div>
    </div>
    <span style="font-size:10px;font-weight:600;background:{pri_bg};color:{pri_c};padding:2px 8px;border-radius:4px;flex-shrink:0">{row["priority"]}</span>
    <div style="font-size:18px;font-weight:700;color:{health_c};flex-shrink:0">{row["health"]:.1f}</div>
  </div>
  <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin-bottom:8px">
    <div style="background:#fafafa;border-radius:6px;padding:7px 10px;text-align:center">
      <div style="font-size:9px;color:#bbb;font-weight:600;text-transform:uppercase;letter-spacing:.06em;margin-bottom:3px">GMV Change</div>
      <div style="font-size:14px">{arrow_pct(row["gmv_pct"])}</div>
      <div style="font-size:9px;color:#aaa">{fmt_gmv(row["cur_gmv"])} vs {fmt_gmv(row["prev_gmv"])}</div>
    </div>
    <div style="background:#fafafa;border-radius:6px;padding:7px 10px;text-align:center">
      <div style="font-size:9px;color:#bbb;font-weight:600;text-transform:uppercase;letter-spacing:.06em;margin-bottom:3px">View</div>
      <div style="font-size:14px">{view_str}</div>
      <div style="font-size:9px">{arrow_pct(row["view_pct"])}</div>
    </div>
    <div style="background:#fafafa;border-radius:6px;padding:7px 10px;text-align:center">
      <div style="font-size:9px;color:#bbb;font-weight:600;text-transform:uppercase;letter-spacing:.06em;margin-bottom:3px">Conversion</div>
      <div style="font-size:14px">{cr_str}</div>
      <div style="font-size:9px">{(lambda pp, pct:
          f'<span style="color:{"#16A34A" if pp>=0 else "#DC2626"};font-weight:500">{"▲" if pp>=0 else "▼"}{abs(pp):.2f} pp</span>'
          if pp is not None else '<span style="color:#ccc">–</span>'
      )(row.get("cr_pp"), row.get("cr_pct"))}</div>
    </div>
    <div style="background:#fafafa;border-radius:6px;padding:7px 10px;text-align:center">
      <div style="font-size:9px;color:#bbb;font-weight:600;text-transform:uppercase;letter-spacing:.06em;margin-bottom:3px">Price vs Last Month</div>
      {price_sku_str}
    </div>
  </div>
  <div style="background:#F8F9FA;border-left:3px solid #e0e0e0;border-radius:0 4px 4px 0;padding:6px 10px;font-size:11px;color:#555">
    💡 {comment}
  </div>
  {sku_html}
</div>""", unsafe_allow_html=True)

                # ── Header ────────────────────────────────────────────────────
                _pf_cur_g  = float(cur_am["gmv"].sum())
                _pf_prev_g = float(prev_am["gmv"].sum())
                _pf_rr_g   = _pf_cur_g / pf_cov if pf_is_rr and pf_cov > 0 else _pf_cur_g
                _pf_rr_str = f'<div style="font-size:10px;color:#2563EB;font-weight:500">RR {fmt_gmv(_pf_rr_g)}</div>' if pf_is_rr else ""
                _pf_chg    = (_pf_rr_g - _pf_prev_g) / _pf_prev_g * 100 if _pf_prev_g > 0 else None
                st.markdown(f"""
<div style="background:#fff;border:1px solid #e8e8e8;border-radius:10px;padding:16px 20px;margin-bottom:16px">
  <div style="display:flex;align-items:center;justify-content:space-between">
    <div>
      <div style="font-size:18px;font-weight:700;letter-spacing:-.02em">{pf_am} — Portfolio Review</div>
      <div style="font-size:12px;color:#aaa;margin-top:2px">{pf_month_sel} vs {pf_prev_lbl} · {len(cur_am)} shops</div>
    </div>
    <div style="text-align:right">
      <div style="font-size:11px;color:#aaa;font-weight:600;text-transform:uppercase;letter-spacing:.06em">Portfolio GMV</div>
      <div style="font-size:22px;font-weight:700">{fmt_gmv(_pf_cur_g)}</div>
      {_pf_rr_str}
      <div style="font-size:11px">{arrow_pct(_pf_chg)} vs {pf_prev_lbl}</div>
    </div>
  </div>
</div>""", unsafe_allow_html=True)

                # ── Two columns: growth + drop ─────────────────────────────────
                col_g, col_d = st.columns(2)
                with col_g:
                    render_table(top10_growth, "🟢 Top 10 Growth MoM", "#16A34A")
                with col_d:
                    render_table(top10_drop,   "🔴 Top 10 Drop MoM",   "#DC2626", is_growth=False)

                # ── Screenshot tip ────────────────────────────────────────────
                st.markdown("---")
                st.caption("💡 Screenshot hint: กด F11 เพื่อ fullscreen แล้ว Ctrl+P → Save as PDF สำหรับ present CEO ครับ")

                # Download CSV
                dl_df = pd.concat([top10_growth, top10_drop]).drop_duplicates(subset="shop_id")
                dl_df["comment"] = dl_df.apply(gen_comment, axis=1)
                st.download_button(
                    "⬇ portfolio_review.csv",
                    to_csv(dl_df[["name","category","cur_gmv","prev_gmv","gmv_pct",
                                   "cur_view","view_pct","cur_cr","cr_pct",
                                   "price_above","sku_count","health","priority","comment"]]),
                    f"portfolio_{pf_am}_{pf_mk}.csv", "text/csv"
                )


# ══ TAB 8: Upload ════════════════════════════════════════════════════════════
with tab_upload:
    st.markdown("### 📤 Upload ข้อมูลใหม่")

    col_up, col_info = st.columns([2, 1])
    with col_up:
        pw_tab = st.text_input("Password", type="password", key="pw_tab",
                               placeholder="ใส่ password เพื่อ upload")

        if pw_tab == st.secrets.get("ADMIN_PASSWORD","gowabi2024"):
            st.success("✓ Authenticated")
            st.markdown("---")

            tx_file_tab   = st.file_uploader("ไฟล์ที่ 1 — Transaction (csv/xlsx) ✱",
                                              type=["csv","xlsx"], key="tx_tab")
            view_file_tab = st.file_uploader("ไฟล์ที่ 2 — View/CR (csv) — ไม่บังคับ",
                                              type=["csv"], key="view_tab")

            if tx_file_tab:
                try:
                    tmp2 = pd.read_csv(io.BytesIO(tx_file_tab.read()),
                                       usecols=["service_created_at","kam"], low_memory=False)
                    tx_file_tab.seek(0)
                    tmp2["ts"] = pd.to_datetime(tmp2["service_created_at"], errors="coerce")
                    tmp2 = tmp2[tmp2["kam"].isin(REAL_AMS)]
                    found_m2 = sorted(tmp2["ts"].dt.to_period("M").dropna().unique())
                    st.info(f"พบข้อมูล **{len(found_m2)} เดือน**: " +
                            ", ".join([MONTH_LABELS[p.month-1]+f" {p.year}" for p in found_m2]))
                except Exception:
                    pass

            up_mode_tab = st.radio(
                "โหมด",
                ["🔄 Overwrite — แทนที่ข้อมูลเดือนที่มีอยู่",
                 "➕ Append — เพิ่มเฉพาะเดือนใหม่"],
                key="up_mode_tab"
            )
            is_ow_tab = up_mode_tab.startswith("🔄")

            if tx_file_tab and st.button("🚀 Process & Upload", type="primary",
                                          use_container_width=True, key="btn_tab"):
                try:
                    with st.spinner("Processing… อาจใช้เวลา 1–3 นาที"):
                        tx_file_tab.seek(0)
                        result2 = process_raw(
                            tx_file_tab.read(),
                            view_file_tab.read() if view_file_tab else None
                        )
                    idx_fresh2 = load_index()
                    saved2 = []
                    for mkey2, mdata2 in result2["months"].items():
                        if not is_ow_tab and mkey2 in idx_fresh2:
                            continue
                        save_month(mkey2, mdata2)
                        idx_fresh2[mkey2] = {
                            "label":       mdata2["stats"]["label"],
                            "upload_time": result2["upload_time"],
                            "stats":       mdata2["stats"],
                        }
                        saved2.append(mkey2)
                    if result2.get("trend"):
                        save_trend(result2["trend"])
                    save_index(idx_fresh2)
                    load_index_cached.clear()
                    load_month_data.clear()
                    load_trend_data.clear()
                    if saved2:
                        mlabels2 = [MONTH_LABELS[int(m.split("-")[1])-1]+" "+m.split("-")[0]
                                    for m in sorted(saved2)]
                        st.success(f"✓ บันทึกแล้ว: {', '.join(mlabels2)}")
                        st.balloons()
                        st.rerun()
                except RuntimeError as e:
                    st.error(str(e))
                except Exception as e:
                    st.error(f"Error: {type(e).__name__}: {e}")

        elif pw_tab:
            st.error("Password ไม่ถูกต้อง")
        else:
            st.info("ใส่ password เพื่อเริ่ม upload")

    with col_info:
        st.markdown("**วิธีใช้**")
        st.markdown("""
- **Overwrite** — ใช้เมื่อต้องการอัพเดทข้อมูลเดือนเดิม
- **Append** — ใช้เมื่อ upload เดือนใหม่เพิ่มเข้ามา

**ไฟล์ที่ต้องการ:**
- Transaction CSV/xlsx (raw data)
- View/CR CSV (optional — ถ้ามีจะได้ View MoM และ CVR MoM จริง)

**หลัง upload:**
ข้อมูลจะอัพเดทให้ทีมเห็นทันทีครับ
        """)

        # Manage data
        idx3 = load_index_cached()
        if idx3:
            st.markdown("---")
            st.markdown("**ข้อมูลที่เก็บไว้**")
            for mk3 in sorted(idx3.keys(), reverse=True):
                p3 = idx3[mk3]
                s3 = p3.get("stats",{})
                rr3 = "🔵 RR" if not s3.get("is_complete",True) else "✓"
                st.caption(f"{rr3} **{p3['label']}** · {s3.get('days',0)}/{s3.get('days_in_month',30)} วัน · อัพ {p3['upload_time']}")
