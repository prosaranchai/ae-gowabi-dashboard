"""
Gowabi AM Store Health Dashboard
=================================
Multi-user online dashboard with daily xlsx upload.

Stack: Streamlit Cloud (UI) + Supabase (shared database)

Modes:
  - /          → Team view  (everyone sees latest data)
  - /?admin=1  → Admin view (upload new xlsx + refresh data)
"""

import streamlit as st
import pandas as pd
import numpy as np
import io
import json
from datetime import datetime
from supabase import create_client

# ─── Page config ─────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Gowabi AM Dashboard",
    page_icon="💆",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500&family=DM+Mono&display=swap');
html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding: 1.25rem 2rem 2rem; max-width: 1400px; }
[data-testid="metric-container"] {
    background: #f8f7f4; border: 0.5px solid rgba(0,0,0,0.08);
    border-radius: 10px; padding: 1rem 1.25rem;
}
[data-testid="metric-container"] label { font-size: 11px !important; color: #888 !important; }
[data-testid="metric-container"] [data-testid="metric-value"] { font-size: 22px !important; font-weight: 500 !important; }
[data-testid="stSidebar"] { background: #f2f0ec; border-right: 0.5px solid rgba(0,0,0,0.08); }
.stDataFrame { border-radius: 8px; overflow: hidden; }
.section-title {
    font-size: 11px; font-weight: 500; letter-spacing: .08em;
    text-transform: uppercase; color: #999; margin: 1.25rem 0 .6rem;
}
.badge-critical { background:#FCEBEB; color:#A32D2D; padding:2px 8px; border-radius:20px; font-size:11px; }
.badge-warning  { background:#FAEEDA; color:#854F0B; padding:2px 8px; border-radius:20px; font-size:11px; }
.badge-healthy  { background:#EAF3DE; color:#3B6D11; padding:2px 8px; border-radius:20px; font-size:11px; }
[data-testid="stFileUploader"] { border: 1.5px dashed rgba(0,0,0,0.15) !important; border-radius: 10px !important; }
</style>
""", unsafe_allow_html=True)


# ─── Supabase client ──────────────────────────────────────────────────────────

@st.cache_resource
def get_supabase():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)


# ─── Constants ────────────────────────────────────────────────────────────────

REAL_AMS = {
    "Amm","Aum","Chertam","Fah KAM","Geem","Get KAM",
    "Mameaw","Nahm","Pui","Puinoon","Seeiw","Wan",
}
EXCLUDED_STATUSES = {"cancelled","refunded","expired","no_show"}
PILLAR_COLS  = ["sku_score","price_score","op_score","view_score","cvr_score"]
PILLAR_NAMES = ["SKU Quality","Price","Operation","View","Conversion"]


# ─── Processing (same logic as process_dashboard.py) ─────────────────────────

def process_xlsx(file_bytes: bytes) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    df = pd.read_excel(io.BytesIO(file_bytes), engine="openpyxl")
    df = df[df["kam"].apply(lambda x: isinstance(x, str))]
    df = df[df["kam"].isin(REAL_AMS)]
    df = df[~df["order_status"].isin(EXCLUDED_STATUSES)]

    agg = df.groupby(["shop_id","organization_name","kam"]).agg(
        total_orders        = ("booking_id",       "count"),
        gmv                 = ("gmv",               "sum"),
        sku_count           = ("sku_id",            "nunique"),
        selling_price_mean  = ("selling_price",     "mean"),
        original_price_mean = ("original_price",    "mean"),
        lowest_price_12m    = ("lowest_price_12m",  "mean"),
        unique_customers    = ("user_id",           "nunique"),
        new_customers       = ("is_first_booking",  lambda x: (x=="TRUE").sum()),
        category            = ("category",          "first"),
    ).reset_index()

    agg["price_above_lowest"] = ((agg["selling_price_mean"]-agg["lowest_price_12m"])/agg["lowest_price_12m"].replace(0,np.nan)*100).round(1).fillna(0)
    agg["repeat_rate"]        = ((agg["unique_customers"]-agg["new_customers"])/agg["unique_customers"].replace(0,np.nan)*100).round(1).fillna(0)
    agg["orders_per_cust"]    = (agg["total_orders"]/agg["unique_customers"].replace(0,np.nan)).round(2).fillna(1)

    agg["sku_score"]   = agg.groupby("category")["sku_count"].rank(pct=True).mul(100).round(0)
    agg["price_score"] = (100-(agg["price_above_lowest"].clip(0,30)/30*100)).round(0).clip(0,100)
    agg["op_score"]    = agg["repeat_rate"].rank(pct=True).mul(100).round(0)
    agg["view_score"]  = agg.groupby("category")["total_orders"].rank(pct=True).mul(100).round(0)
    agg["cvr_score"]   = agg["orders_per_cust"].rank(pct=True).mul(100).round(0)
    agg["health_score"] = agg[PILLAR_COLS].mean(axis=1).round(1)
    agg["priority"]     = pd.cut(agg["health_score"],bins=[0,40,60,100],labels=["critical","warning","healthy"]).astype(str)
    agg["gmv"]          = agg["gmv"].round(0).astype(int)

    def make_alerts(r):
        a = []
        if r["sku_score"]   < 30: a.append(f"SKU น้อย ({int(r['sku_count'])} — ต่ำกว่า 70% ของ {r['category']})")
        if r["price_score"] < 50: a.append(f"ราคาสูงกว่า lowest 12m +{r['price_above_lowest']:.0f}%")
        if r["op_score"]    < 30: a.append(f"Repeat rate ต่ำ ({r['repeat_rate']:.0f}%)")
        if r["view_score"]  < 25: a.append(f"Volume ต่ำใน {r['category']} ({int(r['total_orders'])} orders)")
        if r["cvr_score"]   < 30: a.append(f"Orders/customer ต่ำ ({r['orders_per_cust']:.1f}x)")
        return " | ".join(a)

    agg["alerts"]      = agg.apply(make_alerts, axis=1)
    agg["alert_count"] = agg["alerts"].apply(lambda x: len(x.split(" | ")) if x else 0)

    am = agg.groupby("kam").agg(
        shops=("shop_id","count"), gmv=("gmv","sum"),
        critical_shops=("priority",lambda x:(x=="critical").sum()),
        warning_shops=("priority",lambda x:(x=="warning").sum()),
        avg_health=("health_score","mean"),
        avg_sku=("sku_score","mean"), avg_price=("price_score","mean"),
        avg_op=("op_score","mean"),   avg_view=("view_score","mean"),
        avg_cvr=("cvr_score","mean"),
        total_alerts=("alert_count","sum"),
    ).reset_index().round(1)
    am["gmv"] = am["gmv"].astype(int)

    updated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    return agg, am, updated_at


# ─── Supabase: save & load ────────────────────────────────────────────────────

BUCKET   = "dashboard-data"
KEY_SHOPS = "latest_shops.json"
KEY_AM    = "latest_am.json"
KEY_META  = "latest_meta.json"


def save_to_supabase(shops_df: pd.DataFrame, am_df: pd.DataFrame, updated_at: str, filename: str):
    sb = get_supabase()

    shops_json = shops_df.to_json(orient="records", force_ascii=False)
    am_json    = am_df.to_json(orient="records", force_ascii=False)
    meta_json  = json.dumps({"updated_at": updated_at, "filename": filename, "shop_count": len(shops_df)})

    for key, data in [(KEY_SHOPS, shops_json), (KEY_AM, am_json), (KEY_META, meta_json)]:
        sb.storage.from_(BUCKET).upload(
            key, data.encode("utf-8"),
            file_options={"content-type": "application/json", "upsert": "true"}
        )


@st.cache_data(ttl=60)   # re-fetch from Supabase every 60 seconds
def load_from_supabase():
    try:
        sb = get_supabase()
        shops_raw = sb.storage.from_(BUCKET).download(KEY_SHOPS)
        am_raw    = sb.storage.from_(BUCKET).download(KEY_AM)
        meta_raw  = sb.storage.from_(BUCKET).download(KEY_META)
        shops_df  = pd.read_json(io.BytesIO(shops_raw))
        am_df     = pd.read_json(io.BytesIO(am_raw))
        meta      = json.loads(meta_raw)
        return shops_df, am_df, meta
    except Exception:
        return None, None, None


# ─── Helpers ──────────────────────────────────────────────────────────────────

def score_color(v):
    if v < 40: return "#E24B4A"
    if v < 60: return "#EF9F27"
    return "#639922"

def fmt_gmv(v):
    if v >= 1_000_000: return f"฿{v/1e6:.1f}M"
    if v >= 1_000:     return f"฿{v/1e3:.0f}K"
    return f"฿{v}"

def to_csv(df):
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")

def color_score(val):
    try: return f"color: {score_color(float(val))}; font-weight: 500"
    except: return ""

def color_priority(val):
    return {"critical":"background:#FCEBEB;color:#A32D2D",
            "warning": "background:#FAEEDA;color:#854F0B",
            "healthy": "background:#EAF3DE;color:#3B6D11"}.get(str(val),"")


# ─── Check admin mode ─────────────────────────────────────────────────────────

params = st.query_params
is_admin = params.get("admin", "") == "1"


# ─── Sidebar ─────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### 💆 Gowabi AM")
    st.markdown("**Store Health Dashboard**")
    st.markdown("---")

    if is_admin:
        st.markdown("#### 🔐 Admin Mode")
        admin_key = st.text_input("Admin password", type="password")
        correct_key = st.secrets.get("ADMIN_PASSWORD", "gowabi2024")

        if admin_key == correct_key:
            st.success("✓ Authenticated")
            uploaded = st.file_uploader("Upload xlsx", type=["xlsx"])

            if uploaded and st.button("🔄 Process & Update Dashboard", type="primary"):
                with st.spinner("Processing…"):
                    shops_df, am_df, updated_at = process_xlsx(uploaded.read())
                    save_to_supabase(shops_df, am_df, updated_at, uploaded.name)
                    load_from_supabase.clear()
                st.success(f"✓ อัพเดทแล้ว — {len(shops_df):,} shops")
                st.balloons()
        elif admin_key:
            st.error("Password ไม่ถูกต้อง")

        st.markdown("---")
        st.markdown("**ลิงค์สำหรับทีม AM:**")
        st.code(st.secrets.get("APP_URL", "your-app.streamlit.app"), language=None)

    # ── Filters (shown always) ──────────────────────────────────────────────
    shops_df, am_df, meta = load_from_supabase()

    if shops_df is not None:
        am_options = ["ทั้งหมด"] + sorted(shops_df["kam"].unique().tolist())
        selected_am = st.selectbox("AM", am_options)

        pillar_filter = st.selectbox(
            "กรอง Pillar ที่ต่ำ",
            ["ทุก pillar"] + PILLAR_NAMES,
        )
        priority_filter = st.multiselect(
            "Priority",
            ["critical","warning","healthy"],
            default=["critical","warning"],
        )
        search = st.text_input("ค้นหาชื่อร้าน", placeholder="พิมพ์ชื่อร้าน…")

        st.markdown("---")
        if meta:
            st.markdown(f'<div style="font-size:11px;color:#999">อัพเดทล่าสุด:<br>{meta.get("updated_at","")}<br>{meta.get("filename","")}</div>', unsafe_allow_html=True)

        if not is_admin:
            st.markdown(f'<div style="font-size:11px;color:#bbb;margin-top:8px">Admin? เพิ่ม <code>?admin=1</code> ใน URL</div>', unsafe_allow_html=True)


# ─── Main content ─────────────────────────────────────────────────────────────

shops_df, am_df, meta = load_from_supabase()

if shops_df is None:
    st.markdown("""
    <div style="text-align:center;padding:4rem 0">
      <div style="font-size:48px;margin-bottom:1rem">💆</div>
      <h2 style="font-weight:500">ยังไม่มีข้อมูล</h2>
      <p style="color:#888;margin-top:.5rem">Admin กรุณา upload xlsx ผ่าน <code>?admin=1</code></p>
    </div>""", unsafe_allow_html=True)
    st.stop()


# ── Apply filters ─────────────────────────────────────────────────────────────

filtered = shops_df.copy()

if selected_am != "ทั้งหมด":
    filtered = filtered[filtered["kam"] == selected_am]

if pillar_filter != "ทุก pillar":
    col_map = dict(zip(PILLAR_NAMES, PILLAR_COLS))
    filtered = filtered[filtered[col_map[pillar_filter]] < 40]

if priority_filter:
    filtered = filtered[filtered["priority"].isin(priority_filter)]

if search:
    filtered = filtered[filtered["organization_name"].str.contains(search, case=False, na=False)]

filtered = filtered.sort_values("health_score")

am_view = am_df.copy()
if selected_am != "ทั้งหมด":
    am_view = am_view[am_view["kam"] == selected_am]


# ── Header ────────────────────────────────────────────────────────────────────

col_t, col_b = st.columns([3, 1])
with col_t:
    st.markdown("## Store Health Dashboard")
    updated = meta.get("updated_at","–") if meta else "–"
    fname   = meta.get("filename","") if meta else ""
    st.markdown(f'<div style="font-size:12px;color:#888;margin-top:-8px">YTD · อัพเดท {updated} · {fname}</div>', unsafe_allow_html=True)

with col_b:
    d = datetime.now().strftime("%Y%m%d")
    st.download_button("⬇ shop_scores.csv", data=to_csv(filtered), file_name=f"shop_scores_{d}.csv", use_container_width=True)


# ── Metric cards ──────────────────────────────────────────────────────────────

st.markdown('<div class="section-title">Overview</div>', unsafe_allow_html=True)
c1,c2,c3,c4,c5,c6 = st.columns(6)
c1.metric("Shops",     f"{len(filtered):,}")
c2.metric("GMV YTD",   fmt_gmv(filtered["gmv"].sum()))
c3.metric("Critical 🔴", f"{(filtered['priority']=='critical').sum():,}")
c4.metric("Warning 🟡",  f"{(filtered['priority']=='warning').sum():,}")
c5.metric("Avg Health",  f"{filtered['health_score'].mean():.1f}" if len(filtered) else "–")
c6.metric("Alerts",      f"{filtered['alert_count'].sum():,}")


# ── AM Scorecard ──────────────────────────────────────────────────────────────

st.markdown('<div class="section-title">AM Scorecard</div>', unsafe_allow_html=True)
am_cols = st.columns(min(len(am_view), 6))
for col, (_, row) in zip(am_cols, am_view.sort_values("avg_health").iterrows()):
    col.markdown(f"""
    <div style="background:#f8f7f4;border-radius:10px;padding:.85rem 1rem;border:0.5px solid rgba(0,0,0,0.07)">
      <div style="font-size:13px;font-weight:500;margin-bottom:.3rem">{row['kam']}</div>
      <div style="font-size:20px;font-weight:500;color:{score_color(row['avg_health'])}">{row['avg_health']:.1f}</div>
      <div style="font-size:11px;color:#999;margin-top:2px">{int(row['shops'])} shops · {fmt_gmv(row['gmv'])}</div>
      <div style="font-size:11px;margin-top:4px">
        <span style="color:#E24B4A">● {int(row['critical_shops'])}</span>&nbsp;
        <span style="color:#EF9F27">● {int(row['warning_shops'])}</span>
      </div>
    </div>""", unsafe_allow_html=True)


# ── 5 Pillars ─────────────────────────────────────────────────────────────────

st.markdown('<div class="section-title">5 Pillar Scores</div>', unsafe_allow_html=True)
pcols = st.columns(5)
for col, (pname, pcol) in zip(pcols, zip(PILLAR_NAMES, PILLAR_COLS)):
    avg   = filtered[pcol].mean() if len(filtered) else 0
    color = score_color(avg)
    label = "ต้องแก้ด่วน" if avg < 40 else "ปรับปรุง" if avg < 60 else "ดี"
    col.markdown(f"""
    <div style="background:#f8f7f4;border-radius:10px;padding:.85rem 1rem;border:0.5px solid rgba(0,0,0,0.07)">
      <div style="font-size:10px;color:#999;font-weight:500;text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px">{pname}</div>
      <div style="font-size:22px;font-weight:500;color:{color}">{avg:.0f}</div>
      <div style="height:4px;background:#e5e3de;border-radius:2px;margin:6px 0">
        <div style="width:{avg:.0f}%;height:100%;background:{color};border-radius:2px"></div>
      </div>
      <div style="font-size:10px;color:{color}">{label}</div>
    </div>""", unsafe_allow_html=True)


# ── Store table ───────────────────────────────────────────────────────────────

st.markdown(f'<div class="section-title">Priority Stores — {len(filtered):,} ร้าน</div>', unsafe_allow_html=True)

display_cols = {
    "organization_name":"Shop","kam":"AM","category":"Category",
    "total_orders":"Orders","gmv":"GMV","health_score":"Health",
    "sku_score":"SKU","price_score":"Price","op_score":"Op",
    "view_score":"View","cvr_score":"CVR","priority":"Priority","alerts":"Alerts",
}
tbl = filtered[list(display_cols.keys())].rename(columns=display_cols).copy()
tbl["GMV"] = tbl["GMV"].apply(fmt_gmv)

score_cols = ["Health","SKU","Price","Op","View","CVR"]
styled = (
    tbl.style
    .applymap(color_score,     subset=score_cols)
    .applymap(color_priority,  subset=["Priority"])
    .set_properties(**{"font-size":"12px"})
    .set_table_styles([{"selector":"th","props":[("font-size","11px"),("background","#f2f0ec")]}])
)
st.dataframe(styled, use_container_width=True, height=500)


# ── Export row ────────────────────────────────────────────────────────────────

st.markdown('<div class="section-title">Export</div>', unsafe_allow_html=True)
dl1,dl2,dl3 = st.columns(3)
alerts_only = filtered[filtered["alert_count"]>0][["organization_name","kam","category","health_score","priority","alert_count","alerts"]]
dl1.download_button("⬇ shop_scores.csv",  to_csv(filtered[list(display_cols.keys())]),  f"shop_scores_{d}.csv",  "text/csv", use_container_width=True)
dl2.download_button("⬇ alerts_only.csv",  to_csv(alerts_only),                          f"alerts_{d}.csv",       "text/csv", use_container_width=True)
dl3.download_button("⬇ am_summary.csv",   to_csv(am_view),                              f"am_summary_{d}.csv",   "text/csv", use_container_width=True)
