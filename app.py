"""
Gowabi AM Store Health Dashboard  — v4 Full Production
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
st.set_page_config(page_title="Gowabi AM Dashboard", page_icon="💆",
                   layout="wide", initial_sidebar_state="expanded")
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500&display=swap');
html,body,[class*="css"]{font-family:'DM Sans',sans-serif}
#MainMenu,footer,header{visibility:hidden}
.block-container{padding:1rem 1.5rem 2rem;max-width:1500px}
[data-testid="metric-container"]{background:#f8f7f4;border:0.5px solid rgba(0,0,0,0.08);border-radius:10px;padding:.85rem 1rem}
[data-testid="metric-container"] label{font-size:11px !important;color:#888 !important}
[data-testid="metric-container"] [data-testid="metric-value"]{font-size:20px !important;font-weight:500 !important}
[data-testid="stSidebar"]{background:#f2f0ec;border-right:0.5px solid rgba(0,0,0,0.08)}
.section-title{font-size:10px;font-weight:500;letter-spacing:.08em;text-transform:uppercase;color:#999;margin:.75rem 0 .4rem}
.rr-badge{display:inline-block;background:#E6F1FB;color:#185FA5;font-size:10px;padding:2px 8px;border-radius:10px;margin-left:6px}
</style>
""", unsafe_allow_html=True)

# ─── Constants ────────────────────────────────────────────────────────────────
REAL_AMS = {"Amm","Aum","Chertam","Fah KAM","Geem","Get KAM",
            "Mameaw","Nahm","Pui","Puinoon","Seeiw","Wan"}
EXCLUDED  = {"cancelled","refunded","expired","no_show"}
PILLAR_COLS  = ["sku_score","price_score","view_score","cvr_score"]
PILLAR_NAMES = ["SKU Quality","Price","View MoM","CVR MoM"]
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

        # Merge view data
        use_real = False
        if view_map:
            agg["avg_view"] = agg["shop_id_s"].map(lambda x: view_map.get(x,{}).get("avg_view",0))
            agg["avg_cr"]   = agg["shop_id_s"].map(lambda x: view_map.get(x,{}).get("avg_cr",0))
            agg["avg_view"] = agg["avg_view"].fillna(0)
            agg["avg_cr"]   = agg["avg_cr"].fillna(0)
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
            def view_mom_score(shop_id):
                cur  = cur_view_data.get(shop_id,  {}).get("view", 0)
                prev = prev_view_data.get(shop_id, {}).get("view", 0)
                if prev <= 0 and cur <= 0: return 50.0
                if prev <= 0: return 75.0   # new shop with views
                pct = (cur - prev) / prev * 100
                # Map: -50% → 0, 0% → 50, +50% → 100 (clipped)
                return float(np.clip(50 + pct, 0, 100))
            def cvr_mom_score(shop_id):
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
        am = agg.groupby("kam").agg(
            shops=("shop_id_s","count"), gmv=("gmv","sum"),
            critical_shops=("priority",lambda x:(x=="critical").sum()),
            warning_shops=("priority",lambda x:(x=="warning").sum()),
            avg_health=("health_score","mean"),
            avg_sku=("sku_score","mean"),   avg_price=("price_score","mean"),
            avg_view=("view_score","mean"),
            avg_cvr=("cvr_score","mean"),   avg_view_mom=("view_mom_pct","mean"),  avg_cvr_mom=("cvr_mom_pct","mean"),  total_alerts=("alert_count","sum"),
        ).reset_index().round(1)
        am["gmv"] = am["gmv"].astype(int)

        # Category stats
        cat = mdf.groupby("category").agg(
            gmv=("gmv","sum"), orders=("booking_id","nunique"),
            new_customers=("is_new","sum"), unique_customers=("user_id","nunique"),
        ).reset_index()
        cat["gmv"] = cat["gmv"].round(0).astype(int)

        # Monthly stats
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
        }

        result["months"][mkey] = {
            "shops":     agg.to_dict("records"),
            "am":        am.to_dict("records"),
            "category":  cat.to_dict("records"),
            "stats":     monthly_stat,
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
    svc_top = df.groupby("service_name")["gmv"].sum().nlargest(30).index
    sv = df[df["service_name"].isin(svc_top)].groupby(["service_name","month_period"])["gmv"].sum().reset_index()
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
    s = f"฿{v/1e6:.1f}M" if v>=1e6 else f"฿{v/1e3:.0f}K" if v>=1e3 else f"฿{int(v)}"
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
    st.markdown("### 💆 Gowabi AM")
    st.markdown("**Store Health Dashboard**")
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
        st.markdown("## 💆 Gowabi AM Dashboard")
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
st.markdown(f'## {sel_info["label"]} — Store Health Dashboard {rr_text}', unsafe_allow_html=True)
st.caption(f'อัพโหลด {sel_info["upload_time"]} · {"จริง ✓" if stats.get("use_real_view") else "proxy"} View/CVR')


# ─── Tabs ─────────────────────────────────────────────────────────────────────
tab_ov, tab_gmv, tab_cat, tab_new, tab_health, tab_action = st.tabs([
    "📊 Overview", "📈 GMV MoM", "🗂️ Category", "👥 New User", "🏪 Store Health", "⚡ Action List"
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

    st.markdown('<div class="section-title">AM Scorecard — คลิกเพื่อดูรายคน</div>', unsafe_allow_html=True)
    ov_am_sel = st.session_state.get("ov_am_sel", "all")

    # AM selector pills
    pill_cols = st.columns(min(len(am_full)+1, 7))
    if pill_cols[0].button("ทั้งหมด", key="ov_all",
                           type="primary" if ov_am_sel=="all" else "secondary",
                           use_container_width=True):
        st.session_state["ov_am_sel"] = "all"; st.rerun()
    for i, (_, r) in enumerate(am_full.sort_values("avg_health").iterrows()):
        if i+1 < len(pill_cols):
            if pill_cols[i+1].button(r["kam"], key=f"ov_{r['kam']}",
                                     type="primary" if ov_am_sel==r["kam"] else "secondary",
                                     use_container_width=True):
                st.session_state["ov_am_sel"] = r["kam"]; st.rerun()

    # Filter data by selected AM
    am_src_ov = am_full if ov_am_sel=="all" else am_full[am_full["kam"]==ov_am_sel]
    shops_src  = shops_full if ov_am_sel=="all" else shops_full[shops_full["kam"]==ov_am_sel]

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
                p_orders = psh["total_orders"].sum() if "total_orders" in psh.columns else 0
                p_view   = psh["avg_view"].mean()    if "avg_view"   in psh.columns else 0
                # Run rate adjust if prev month was incomplete
                p_gmv_rr = pa["gmv"] / prev_cov if prev_is_rr2 and prev_cov > 0 else pa["gmv"]
                p_ord_rr = p_orders  / prev_cov if prev_is_rr2 and prev_cov > 0 else p_orders
                p_view_rr= p_view    / prev_cov if prev_is_rr2 and prev_cov > 0 else p_view
                prev_am_map[pa["kam"]] = {
                    "gmv":    p_gmv_rr,
                    "orders": p_ord_rr,
                    "view":   p_view_rr,
                }

    def delta_html(curr, prev_val, fmt_fn=None, suffix=""):
        """Return colored % change HTML string."""
        if not prev_val or prev_val == 0: return ""
        pct = (curr - prev_val) / prev_val * 100
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
        total_orders = am_shops["total_orders"].sum() if "total_orders" in am_shops.columns else 0
        basket_size  = r["gmv"] / total_orders if total_orders > 0 else 0
        avg_view     = am_shops["avg_view"].mean() if "avg_view" in am_shops.columns else 0
        avg_cr       = am_shops["avg_cr"].mean()   if "avg_cr"   in am_shops.columns else 0
        new_cust     = am_shops["new_customers"].sum() if "new_customers" in am_shops.columns else 0

        # Run rate values for this AM (pro-rate by month coverage)
        cov = ov_stats.get("coverage_pct", 100) / 100
        gmv_rr     = r["gmv"]        / cov if ov_is_rr and cov > 0 else r["gmv"]
        orders_rr  = total_orders    / cov if ov_is_rr and cov > 0 else total_orders
        view_rr    = avg_view        / cov if ov_is_rr and cov > 0 else avg_view

        # Prev month values
        prev_am = prev_am_map.get(r["kam"], {})
        p_gmv   = prev_am.get("gmv",    0)
        p_ord   = prev_am.get("orders", 0)
        p_view  = prev_am.get("view",   0)

        health_color = sc(r["avg_health"])

        # Helper: metric cell HTML
        # Pre-compute all metric cells before embedding in HTML
        def mk_cell(label, main_val, rr_val=None, prev_val=None, color="inherit"):
            rr_part = (f'<div style="font-size:9px;color:#185FA5;margin-top:1px">RR {fmt_gmv(rr_val)}</div>'
                       if rr_val and ov_is_rr else "")
            d_part  = delta_html(rr_val or main_val, prev_val) if prev_val else ""
            return (
                '<div style="background:#f8f7f4;border-radius:8px;padding:8px 10px;text-align:center">'
                f'<div style="font-size:9px;color:#aaa;text-transform:uppercase;letter-spacing:.05em;margin-bottom:3px">{label}</div>'
                f'<div style="font-size:14px;font-weight:600;color:{color}">{main_val}</div>'
                f'{rr_part}{d_part}'
                '</div>'
            )

        c_gmv    = mk_cell("GMV",        fmt_gmv(r["gmv"]),         rr_val=gmv_rr,   prev_val=p_gmv  or None)
        c_ord    = mk_cell("Orders",     f"{int(total_orders):,}",  rr_val=orders_rr, prev_val=p_ord  or None)
        c_basket = mk_cell("Basket Size",f"฿{basket_size:,.0f}")
        c_new    = mk_cell("New User",   f"{int(new_cust):,}")
        c_view   = mk_cell("Shop View",  f"{avg_view:,.0f}",        rr_val=view_rr,  prev_val=p_view or None)
        c_cvr    = mk_cell("CVR",        f"{avg_cr:.2f}%",          color=sc(r["avg_cvr"]))

        cells_html = c_gmv + c_ord + c_basket + c_new + c_view + c_cvr

        card_html = (
            '<div style="background:#fff;border:1px solid #e8e5e0;border-radius:12px;padding:12px 16px;margin-bottom:8px">'
            '<div style="display:flex;align-items:center;gap:12px;margin-bottom:10px">'
            f'<div style="font-size:14px;font-weight:600;min-width:80px">{r["kam"]}</div>'
            f'<div style="font-size:22px;font-weight:600;color:{health_color}">{r["avg_health"]:.1f}</div>'
            f'<div style="font-size:11px;color:#aaa">{int(r["shops"])} shops</div>'
            '<div style="margin-left:auto;font-size:11px">'
            f'<span style="color:#E24B4A">● {int(r["critical_shops"])} critical</span>&nbsp;&nbsp;'
            f'<span style="color:#EF9F27">● {int(r["warning_shops"])} warning</span>'
            '</div></div>'
            '<div style="display:grid;grid-template-columns:repeat(6,1fr);gap:8px">'
            + cells_html +
            '</div></div>'
        )
        st.markdown(card_html, unsafe_allow_html=True)

    # Pillar scores
    st.markdown('<div class="section-title">5 Pillar Scores</div>', unsafe_allow_html=True)
    pcols = st.columns(5)
    pillar_am_keys = ["avg_sku","avg_price","avg_view","avg_cvr"]
    for col,(pname,pk) in zip(pcols, zip(PILLAR_NAMES, pillar_am_keys)):
        avg   = am_src_ov[pk].mean() if len(am_src_ov) else 0
        label = "ต้องแก้" if avg<40 else "ปรับปรุง" if avg<60 else "ดี"
        bg,tc = ("#FCEBEB","#A32D2D") if avg<40 else ("#FAEEDA","#854F0B") if avg<60 else ("#EAF3DE","#3B6D11")
        col.markdown(f"""<div style="background:#f8f7f4;border-radius:10px;padding:.75rem 1rem;border:0.5px solid rgba(0,0,0,0.07)">
          <div style="font-size:9px;color:#999;font-weight:500;text-transform:uppercase;letter-spacing:.05em;margin-bottom:3px">{pname}</div>
          <div style="font-size:20px;font-weight:500;color:{sc(avg)}">{avg:.0f}</div>
          <div style="height:4px;background:#e5e3de;border-radius:2px;margin:4px 0">
            <div style="width:{avg:.0f}%;height:100%;background:{sc(avg)};border-radius:2px"></div>
          </div>
          <span style="font-size:9px;background:{bg};color:{tc};padding:1px 6px;border-radius:8px">{label}</span>
        </div>""", unsafe_allow_html=True)


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
            all_shop_names = sorted(trend_shop["organization_name"].unique()) if len(trend_shop) else []
            gmv_shop_filt  = st.multiselect("Filter by Shop (เว้นว่าง = ทั้งหมด)",
                                             all_shop_names, default=[], key="gmv_shop",
                                             placeholder="เลือกร้านที่ต้องการ…")

        # Build monthly summary across all months
        all_months  = sorted(idx_now.keys())
        all_labels  = [idx_now[m]["label"] for m in all_months]
        all_gmv     = [idx_now[m]["stats"]["gmv"] for m in all_months]
        all_rr_gmv  = [idx_now[m]["stats"]["gmv_run_rate"] for m in all_months]

        col1,col2 = st.columns(2)
        with col1:
            st.markdown('<div class="section-title">GMV รายเดือน (฿M)</div>', unsafe_allow_html=True)
            st.bar_chart(pd.DataFrame({"GMV":[v/1e6 for v in all_gmv]}, index=all_labels),
                         use_container_width=True, height=180)
        with col2:
            st.markdown('<div class="section-title">MoM Table</div>', unsafe_allow_html=True)
            mom_rows = []
            for i,(m,label) in enumerate(zip(all_months, all_labels)):
                info = idx_now[m]["stats"]
                gmv  = info["gmv"]; rr = info["gmv_run_rate"]
                prev = idx_now[all_months[i-1]]["stats"]["gmv"] if i>0 else None
                mom  = f"{(gmv-prev)/prev*100:+.1f}%" if prev else "–"
                mom_rows.append({"Month":label,"GMV":fmt_gmv(gmv),
                                 "Run Rate":f"฿{rr/1e6:.1f}M" if not info.get("is_complete") else "–","MoM":mom})
            st.dataframe(pd.DataFrame(mom_rows), hide_index=True, use_container_width=True)

        # GMV by AM trend (filtered)
        st.markdown('<div class="section-title">GMV by AM รายเดือน (฿M)</div>', unsafe_allow_html=True)
        if len(trend_kam):
            km = trend_kam.copy()
            if gmv_am_filt != "ทั้งหมด": km = km[km["kam"]==gmv_am_filt]
            km_pivot = km.pivot(index="month", columns="kam", values="gmv").fillna(0) / 1e6
            st.line_chart(km_pivot, use_container_width=True, height=200)

        col3,col4 = st.columns(2)
        with col3:
            st.markdown('<div class="section-title">All Shops — GMV by Month (฿K)</div>', unsafe_allow_html=True)
            if len(trend_shop):
                ts = trend_shop.copy()
                if gmv_am_filt != "ทั้งหมด": ts = ts[ts["kam"]==gmv_am_filt]
                if gmv_shop_filt: ts = ts[ts["organization_name"].isin(gmv_shop_filt)]
                top20 = ts.groupby("organization_name")["gmv"].sum().sort_values(ascending=False).index
                ts = ts[ts["organization_name"].isin(top20)]
                # Swap axes: rows=shops, columns=months (฿K)
                tp = ts.pivot(index="organization_name", columns="month", values="gmv").fillna(0) / 1e3
                # Add total column
                tp["Total"] = tp.sum(axis=1)
                tp = tp.sort_values("Total", ascending=False).drop(columns="Total")
                # Rename month columns to labels
                tp.columns = [idx_now.get(c,{}).get("label",c) if c in idx_now else c for c in tp.columns]
                tp.index.name = "Shop"
                st.dataframe(tp.round(0), use_container_width=True, height=320)

        with col4:
            st.markdown('<div class="section-title">Top 20 Services — GMV Total (฿K)</div>', unsafe_allow_html=True)
            if len(trend_svc):
                sv = trend_svc.copy()
                sv["gmv"] = pd.to_numeric(sv["gmv"], errors="coerce").fillna(0)
                sv = sv.groupby("service_name")["gmv"].sum().nlargest(20).reset_index()
                sv["gmv"] = (sv["gmv"]/1e3).round(0)
                sv.columns = ["Service","GMV (฿K)"]
                st.dataframe(sv, hide_index=True, use_container_width=True, height=320)


# ══ TAB 2: Category ════════════════════════════════════════════════════════════
with tab_cat:
    # ── Filters ────────────────────────────────────────────────────────────
    cf1, cf2 = st.columns(2)
    with cf1:
        cat_am_filt = st.selectbox("Filter by KAM", ["ทั้งหมด"]+sorted(REAL_AMS), key="cat_am")
    with cf2:
        # Month filter from available periods
        all_months_cat = sorted(idx_now.keys())
        month_opts_cat = ["ทุกเดือน"] + [idx_now[m]["label"] for m in all_months_cat]
        cat_month_filt = st.selectbox("Filter by Month", month_opts_cat, key="cat_month",
                                      index=len(month_opts_cat)-1)  # default = latest

    # Resolve selected month key
    sel_month_cat = None
    if cat_month_filt != "ทุกเดือน":
        for mk in all_months_cat:
            if idx_now[mk]["label"] == cat_month_filt:
                sel_month_cat = mk; break

    # Load category data for selected month
    if sel_month_cat:
        mdata_cat  = load_month_data(sel_month_cat) or mdata
    else:
        mdata_cat  = mdata

    cat_full = pd.DataFrame(mdata_cat["category"])

    # Apply KAM filter on shops to get category breakdown by AM
    if cat_am_filt != "ทั้งหมด" and sel_month_cat:
        shops_for_cat = pd.DataFrame(mdata_cat.get("shops",[]))
        if len(shops_for_cat):
            am_shop_ids = set(shops_for_cat[shops_for_cat["kam"]==cat_am_filt]["shop_id_str"].astype(str))
            # Note: category breakdown from mdata is not per-AM — show a note
            st.info(f"หมายเหตุ: Category data แสดง {cat_month_filt} ทั้งหมด · KAM filter มีผลกับ Store Health เท่านั้น")

    st.markdown(f'<div class="section-title">Category Overview — {cat_month_filt}</div>', unsafe_allow_html=True)
    if len(cat_full):
        cat_full["new_pct"] = (cat_full["new_customers"]/cat_full["unique_customers"].replace(0,np.nan)*100).round(1).fillna(0)
        cat_full = cat_full.sort_values("gmv", ascending=False)
        cat_full["gmv_fmt"] = cat_full["gmv"].apply(fmt_gmv)
        st.dataframe(
            cat_full[["category","gmv_fmt","orders","unique_customers","new_customers","new_pct"]].rename(columns={
                "category":"Category","gmv_fmt":"GMV","orders":"Orders",
                "unique_customers":"Customers","new_customers":"New","new_pct":"New%"
            }),
            hide_index=True, use_container_width=True, height=300
        )

    if trend.get("category"):
        trend_cat   = pd.DataFrame(trend["category"])
        trend_cat_f = trend_cat.copy()

        # Apply month filter to trend
        if cat_month_filt != "ทุกเดือน" and sel_month_cat:
            trend_cat_f = trend_cat_f[trend_cat_f["month"]==sel_month_cat]

        st.markdown('<div class="section-title">GMV by Category รายเดือน (฿M)</div>', unsafe_allow_html=True)
        tc_all = trend_cat.copy()  # always show full trend for chart
        top_cats = tc_all.groupby("category")["gmv"].sum().nlargest(8).index
        ct = tc_all[tc_all["category"].isin(top_cats)]
        ct_pivot = ct.pivot(index="month", columns="category", values="gmv").fillna(0) / 1e6
        # Rename month index to labels
        ct_pivot.index = [idx_now.get(m,{}).get("label",m) for m in ct_pivot.index]
        st.line_chart(ct_pivot, use_container_width=True, height=220)

        st.markdown('<div class="section-title">New User % by Category รายเดือน</div>', unsafe_allow_html=True)
        trend_cat["new_pct"] = (trend_cat["new"]/trend_cat["customers"].replace(0,np.nan)*100).round(1).fillna(0)
        np_data = trend_cat[trend_cat["category"].isin(top_cats)]
        np_pivot = np_data.pivot(index="month", columns="category", values="new_pct").fillna(0)
        np_pivot.index = [idx_now.get(m,{}).get("label",m) for m in np_pivot.index]
        st.line_chart(np_pivot, use_container_width=True, height=180)

        # Category detail table per month
        st.markdown('<div class="section-title">Category Detail by Month</div>', unsafe_allow_html=True)
        cat_detail = trend_cat.copy()
        cat_detail["gmv"] = pd.to_numeric(cat_detail["gmv"], errors="coerce").fillna(0)
        cat_detail["month_label"] = cat_detail["month"].map(lambda m: idx_now.get(m,{}).get("label",m))
        cat_pivot = cat_detail.pivot_table(index="category", columns="month_label",
                                           values="gmv", aggfunc="sum").fillna(0) / 1e3
        cat_pivot["Total"] = cat_pivot.sum(axis=1)
        cat_pivot = cat_pivot.sort_values("Total", ascending=False)
        st.dataframe(cat_pivot.round(0), use_container_width=True, height=280)


# ══ TAB 3: New User ════════════════════════════════════════════════════════════
with tab_new:
    st.markdown('<div class="section-title">New vs Repeat Customers</div>', unsafe_allow_html=True)

    # Current month
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Total Customers", f"{stats['unique_customers']:,}")
    c2.metric("New Customers",   f"{stats['new_customers']:,}")
    c3.metric("Repeat Customers", f"{stats['unique_customers']-stats['new_customers']:,}")
    c4.metric("New User %", f"{stats['new_customers']/max(stats['unique_customers'],1)*100:.1f}%")

    # Trend table
    if idx_now:
        st.markdown('<div class="section-title">New User Trend รายเดือน</div>', unsafe_allow_html=True)
        rows = []
        for mk in sorted(idx_now.keys()):
            s = idx_now[mk]["stats"]
            rows.append({
                "Month":     idx_now[mk]["label"],
                "Total Cust":f"{s['unique_customers']:,}",
                "New":       f"{s['new_customers']:,}",
                "Repeat":    f"{s['unique_customers']-s['new_customers']:,}",
                "New%":      f"{s['new_customers']/max(s['unique_customers'],1)*100:.1f}%",
                "Orders":    f"{s['orders']:,}",
                "GMV":       fmt_gmv(s['gmv_run_rate'] if not s.get('is_complete') else s['gmv'], not s.get('is_complete')),
            })
        st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)

    # Category new user breakdown
    if len(cat_full):
        st.markdown('<div class="section-title">New User % by Category</div>', unsafe_allow_html=True)
        cf2 = pd.DataFrame(mdata["category"]).copy()
        cf2["new_pct"] = (cf2["new_customers"]/cf2["unique_customers"].replace(0,np.nan)*100).round(1).fillna(0)
        cf2 = cf2.sort_values("new_pct", ascending=False)
        st.bar_chart(cf2.set_index("category")["new_pct"], use_container_width=True, height=200)


# ══ TAB 4: Store Health ════════════════════════════════════════════════════════
with tab_health:
    st.markdown(f'<div class="section-title">Priority Stores — {len(shops_df):,} ร้าน ({sel_info["label"]})</div>', unsafe_allow_html=True)

    dcols = {"organization_name":"Shop","kam":"AM","category":"Category",
             "total_orders":"Orders","gmv":"GMV","health_score":"Health",
             "sku_score":"SKU","price_score":"Price",
             "view_score":"View MoM","cvr_score":"CVR MoM","priority":"Priority","alerts":"Alerts"}

    tbl = shops_df[list(dcols.keys())].rename(columns=dcols).copy()

    # Format numbers — clean & readable
    tbl["GMV"]      = tbl["GMV"].apply(fmt_gmv)
    tbl["Health"]   = tbl["Health"].apply(lambda x: f"{x:.1f}")
    tbl["SKU"]      = tbl["SKU"].apply(lambda x: f"{int(x)}")
    tbl["Price"]    = tbl["Price"].apply(lambda x: f"{int(x)}")
    tbl["View MoM"] = tbl["View MoM"].apply(lambda x: f"{int(x)}")
    tbl["CVR MoM"]  = tbl["CVR MoM"].apply(lambda x: f"{int(x)}")

    # Add MoM % columns if available
    if "view_mom_pct" in shops_df.columns:
        tbl["View Δ"] = shops_df["view_mom_pct"].apply(
            lambda x: f"{'▲' if x>0 else '▼' if x<0 else '–'}{abs(x):.0f}%" if x != 0 else "–")
    if "cvr_mom_pct" in shops_df.columns:
        tbl["CVR Δ"] = shops_df["cvr_mom_pct"].apply(
            lambda x: f"{'▲' if x>0 else '▼' if x<0 else '–'}{abs(x):.0f}%" if x != 0 else "–")
    if "avg_cr" in shops_df.columns:
        tbl["CR%"] = shops_df["avg_cr"].apply(lambda x: f"{x:.2f}%")

    score_cols  = ["Health","SKU","Price","View MoM","CVR MoM"]

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
    f_col1, f_col2, f_col3 = st.columns([3, 2, 2])

    with f_col1:
        st.markdown('<div class="section-title" style="margin-bottom:4px">Filter by AM</div>', unsafe_allow_html=True)
        ams_with_issues = sorted(action_shops["kam"].unique())
        cur_filter = st.session_state.get("action_am_filter","all")
        am_btns = st.columns(min(len(ams_with_issues)+1, 6))
        if am_btns[0].button("ทั้งหมด", key="af_all",
                             type="primary" if cur_filter=="all" else "secondary",
                             use_container_width=True):
            st.session_state["action_am_filter"] = "all"; st.rerun()
        for i, am_name in enumerate(ams_with_issues[:5]):
            n = (action_shops["kam"]==am_name).sum()
            if am_btns[i+1].button(f"{am_name}\n({n})", key=f"af_{am_name}",
                                   type="primary" if cur_filter==am_name else "secondary",
                                   use_container_width=True):
                st.session_state["action_am_filter"] = am_name; st.rerun()
        if len(ams_with_issues) > 5:
            extra = st.selectbox("AM อื่นๆ", ["–"]+ams_with_issues[5:], key="af_extra", label_visibility="collapsed")
            if extra != "–":
                st.session_state["action_am_filter"] = extra; st.rerun()

    with f_col2:
        st.markdown('<div class="section-title" style="margin-bottom:4px">Filter by GMV</div>', unsafe_allow_html=True)
        max_gmv = int(action_shops["gmv"].max()) if len(action_shops) else 1000000
        gmv_range = st.select_slider(
            "GMV range",
            options=["ทั้งหมด", "< ฿10K", "฿10K–50K", "฿50K–200K", "฿200K–1M", "> ฿1M"],
            value="ทั้งหมด",
            key="gmv_range_filter",
            label_visibility="collapsed"
        )

    with f_col3:
        st.markdown('<div class="section-title" style="margin-bottom:4px">Sort by</div>', unsafe_allow_html=True)
        sort_by = st.selectbox(
            "Sort",
            ["Health score (ต่ำสุดก่อน)", "GMV (สูงสุดก่อน)", "GMV (ต่ำสุดก่อน)", "Alert count (มากสุดก่อน)"],
            key="action_sort",
            label_visibility="collapsed"
        )

    # Apply AM filter
    if cur_filter != "all":
        action_shops = action_shops[action_shops["kam"]==cur_filter]

    # Apply GMV filter
    gmv_map = {
        "< ฿10K":      (0,       10_000),
        "฿10K–50K":   (10_000,  50_000),
        "฿50K–200K":  (50_000,  200_000),
        "฿200K–1M":   (200_000, 1_000_000),
        "> ฿1M":       (1_000_000, 999_999_999),
    }
    if gmv_range != "ทั้งหมด" and gmv_range in gmv_map:
        lo, hi = gmv_map[gmv_range]
        action_shops = action_shops[(action_shops["gmv"] >= lo) & (action_shops["gmv"] < hi)]

    # Apply sort
    sort_map = {
        "Health score (ต่ำสุดก่อน)": ("health_score", True),
        "GMV (สูงสุดก่อน)":           ("gmv", False),
        "GMV (ต่ำสุดก่อน)":           ("gmv", True),
        "Alert count (มากสุดก่อน)":   ("alert_count", False),
    }
    sc_col, sc_asc = sort_map.get(sort_by, ("health_score", True))
    action_shops = action_shops.sort_values(sc_col, ascending=sc_asc)

    st.markdown(f'<div class="section-title">Action List — {len(action_shops)} ร้านที่ต้องดูแล</div>', unsafe_allow_html=True)
    if gmv_range != "ทั้งหมด":
        st.caption(f"กรอง: GMV {gmv_range} · เรียงตาม {sort_by}")
    else:
        st.caption(f"เรียงตาม {sort_by}")

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
            acts.append(("🔴 เพิ่ม SKU",
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
            acts.append(("🔴 ปรับราคา", f"Services ที่แพงกว่า lowest 12m:{svc_detail}"))

        # Operation pillar removed

        if row["view_score"] < 40:
            cur_view  = row.get("avg_view", 0)
            view_mom_pct = row.get("view_mom_pct", 0)
            if view_mom_pct != 0:
                mom_str = f" ({'+' if view_mom_pct>=0 else ''}{view_mom_pct:.0f}% vs เดือนก่อน)"
                emoji   = "📉" if view_mom_pct < 0 else "📈"
            else:
                mom_str = ""; emoji = "📉"
            acts.append((f"🟡 เพิ่ม View",
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
            acts.append((f"🟡 ปรับ CVR",
                f"{emoji} CR% {cr:.2f}%{cr_str} — "
                f"ตรวจ: รูปภาพ, description, ราคา, reviews และ response time"))

        # ── Render card ────────────────────────────────────────────────────
        priority_color = "#FCEBEB" if row["priority"]=="critical" else "#FAEEDA"
        priority_text  = "#A32D2D" if row["priority"]=="critical" else "#854F0B"

        with st.container():
            st.markdown(f"""
            <div style="background:#fff;border:1px solid #e8e5e0;border-radius:10px;padding:10px 14px;margin-bottom:4px">
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
