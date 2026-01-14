import os
import time
import hashlib

import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from cassandra.cluster import Cluster


# ==============================
# Auth (admin password)
# ==============================
def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def get_admin_password() -> str:
    # .streamlit/secrets.toml: ADMIN_PASSWORD = "admin"
    if "ADMIN_PASSWORD" in st.secrets:
        return str(st.secrets["ADMIN_PASSWORD"])
    return os.getenv("STREAMLIT_ADMIN_PASSWORD", "")


def login_gate() -> bool:
    if "auth_ok" not in st.session_state:
        st.session_state.auth_ok = False
    if st.session_state.auth_ok:
        return True

    st.markdown("## 🔐 Admin Login")
    st.caption("Restricted access. Please authenticate to access fraud monitoring.")
    pw = st.text_input("Admin password", type="password", key="pw_input")

    if st.button("Sign in", key="btn_signin", type="primary"):
        real_pw = get_admin_password()
        if not real_pw:
            st.error("Admin password not configured. Set STREAMLIT_ADMIN_PASSWORD or .streamlit/secrets.toml.")
            return False
        if _sha256(pw) == _sha256(real_pw):
            st.session_state.auth_ok = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    return False


def logout_button_sidebar():
    # đặt ở cuối sidebar
    st.sidebar.markdown("---")
    st.sidebar.markdown("## Session")
    if st.sidebar.button("Logout", key="btn_logout", use_container_width=True):
        st.session_state.auth_ok = False
        st.rerun()


# ==============================
# Cassandra
# ==============================
@st.cache_resource()
def connect_to_cassandra():
    cluster = Cluster(["127.0.0.1"])
    return cluster.connect("bigdata")


@st.cache_data(ttl=2)
def fetch_rows(limit: int) -> pd.DataFrame:
    session = connect_to_cassandra()
    q = f"SELECT * FROM transaction_data LIMIT {int(limit)};"
    rows = session.execute(q)
    return pd.DataFrame(list(rows))


@st.cache_data(ttl=2)
def fetch_one_by_trans_num(trans_num: str) -> pd.DataFrame:
    """
    Fetch 1 row by PRIMARY KEY trans_num (fast, no ALLOW FILTERING).
    """
    session = connect_to_cassandra()
    q = "SELECT * FROM transaction_data WHERE trans_num = %s LIMIT 1;"
    rows = session.execute(q, (trans_num,))
    return pd.DataFrame(list(rows))


def ensure_types(df: pd.DataFrame) -> pd.DataFrame:
    num_cols = ["amt", "p_fraud", "is_fraud", "is_fraud_prediction", "category_label", "merchant_label"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if "inserted_at" in df.columns:
        df["inserted_at"] = pd.to_datetime(df["inserted_at"], errors="coerce", utc=True)

    if "trans_num" in df.columns:
        df["trans_num"] = df["trans_num"].astype(str)

    return df


# ==============================
# Auto refresh (NO while True)
# ==============================
def auto_refresh(seconds: int):
    """
    Rerun page after N seconds by updating a query param.
    Works without st.autorefresh / experimental_rerun.
    """
    seconds = max(1, int(seconds))
    now = int(time.time())
    qp = dict(st.query_params)

    last = int(qp.get("t", "0") or "0")
    if now - last >= seconds:
        st.query_params["t"] = str(now)
        st.rerun()


# ==============================
# UI style (FIX title clipped)
# ==============================
def apply_style():
    st.set_page_config(page_title="Fraud Monitoring (Admin)", page_icon="🛡️", layout="wide")
    st.markdown(
        """
        <style>
        /* Fix title bị cắt chữ khi zoom/font */
        .block-container { 
            padding-top: 2.2rem !important; 
            padding-bottom: 2rem; 
        }

        .title { 
            font-size: 2.2rem; 
            font-weight: 800; 
            letter-spacing: -0.02em; 
            line-height: 1.25 !important;
            padding-top: 0.25rem !important;
            margin: 0 0 0.35rem 0 !important; 
        }
        .subtitle { 
            color: rgba(49,51,63,0.7); 
            margin: 0 0 0.6rem 0 !important; 
            line-height: 1.35 !important;
        }

        .card { border: 1px solid rgba(49,51,63,.15); border-radius: 16px; padding: 14px 16px; background: rgba(255,255,255,.02); }
        .badge { display:inline-block; padding: 4px 10px; border-radius:999px; font-size: 0.85rem; border: 1px solid rgba(49,51,63,.2); }
        .ok { background: rgba(0,200,0,0.08); }
        .warn { background: rgba(255,165,0,0.10); }
        .danger { background: rgba(255,0,0,0.10); }
        </style>
        """,
        unsafe_allow_html=True,
    )


def fmt_pct(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "N/A"
    return f"{x*100:.2f}%"


# ==============================
# Main
# ==============================
def main():
    apply_style()

    if not login_gate():
        return

    # ---- Sidebar (gọn) ----
    st.sidebar.markdown("## Controls")
    limit = st.sidebar.slider("Rows to fetch (LIMIT)", 200, 8000, 2000, 100, key="limit")
    refresh = st.sidebar.slider("Refresh (seconds)", 1, 30, 5, 1, key="refresh")

    threshold = st.sidebar.slider(
        "Fraud threshold (p_fraud ≥ threshold)",
        0.01,
        0.99,
        0.13,   # default
        0.01,
        key="threshold",
    )

    st.sidebar.markdown("## Filters")
    pred_filter = st.sidebar.selectbox("Predicted fraud", ["All", 0, 1], index=0, key="pred_filter")

    # ---- Header ----
    st.markdown(
        """
        <div style="padding-top: 6px;">
          <div class="title">🛡️ Real-Time Fraud Monitoring Dashboard</div>
          <div class="subtitle">Kafka → Spark ML → Cassandra → Streamlit.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.divider()

    # Load data
    df = fetch_rows(limit)
    df = ensure_types(df)

    if df.empty:
        st.warning("No data from Cassandra yet. Ensure producer + fraud_detection consumer are running.")

        # ✅ Logout vẫn ở cuối sidebar (ngay cả khi df rỗng)
        logout_button_sidebar()

        auto_refresh(refresh)
        return

    # filter options
    cats = ["All"]
    merchs = ["All"]
    if "category_label" in df.columns:
        cats = ["All"] + sorted([int(x) for x in df["category_label"].dropna().unique().tolist()])
    if "merchant_label" in df.columns:
        merchs = ["All"] + sorted([int(x) for x in df["merchant_label"].dropna().unique().tolist()])[:800]

    category_filter = st.sidebar.selectbox("category_label", cats, index=0, key="category_filter")
    merchant_filter = st.sidebar.selectbox("merchant_label", merchs, index=0, key="merchant_filter")

    # ✅ CHỈ SỬA: ĐẶT LOGOUT SAU CÙNG (sau tất cả filter)
    logout_button_sidebar()

    # Derive is_fraud_prediction using p_fraud if needed
    if "p_fraud" in df.columns:
        df["pred_by_threshold"] = (df["p_fraud"].fillna(0.0) >= threshold).astype(int)
        if "is_fraud_prediction" not in df.columns:
            df["is_fraud_prediction"] = df["pred_by_threshold"]
    else:
        if "is_fraud_prediction" not in df.columns:
            df["is_fraud_prediction"] = 0

    # Apply filters
    view = df.copy()
    if pred_filter != "All":
        view = view[view["is_fraud_prediction"] == int(pred_filter)]
    if category_filter != "All" and "category_label" in view.columns:
        view = view[view["category_label"] == int(category_filter)]
    if merchant_filter != "All" and "merchant_label" in view.columns:
        view = view[view["merchant_label"] == int(merchant_filter)]

    if "inserted_at" in view.columns and view["inserted_at"].notna().any():
        view = view.sort_values("inserted_at", ascending=False)

    # KPIs
    total = int(view.shape[0])
    pred_cnt = int((view["is_fraud_prediction"] == 1).sum()) if "is_fraud_prediction" in view.columns else 0
    pred_rate = float(pred_cnt / total) if total > 0 else np.nan
    true_rate = float(np.nanmean(view["is_fraud"])) if "is_fraud" in view.columns else np.nan
    avg_amt = float(np.nanmean(view["amt"])) if "amt" in view.columns else np.nan
    max_pf = float(np.nanmax(view["p_fraud"])) if "p_fraud" in view.columns and view["p_fraud"].notna().any() else np.nan

    status = "OK"
    badge_class = "ok"
    if pred_cnt > 0:
        status = "ALERT"
        badge_class = "danger"
    elif not np.isnan(max_pf) and max_pf >= threshold * 0.8:
        status = "WATCH"
        badge_class = "warn"

    k1, k2, k3, k4, k5, k6 = st.columns([1.2, 1.2, 1.2, 1.2, 1.2, 1.0])
    k1.metric("Transactions (window)", f"{total}")
    k2.metric("Pred Fraud Count", f"{pred_cnt}")
    k3.metric("Pred Fraud Rate", fmt_pct(pred_rate))
    k4.metric("True Fraud Rate", fmt_pct(true_rate))
    k5.metric("Avg Amount", f"{avg_amt:.2f}" if not np.isnan(avg_amt) else "N/A")
    with k6:
        st.markdown(
            f'<div class="card"><span class="badge {badge_class}"><b>{status}</b></span><br>'
            f'<span style="color:rgba(49,51,63,0.7)">Threshold:</span> <b>{threshold:.2f}</b><br>'
            f'<span style="color:rgba(49,51,63,0.7)">Max p_fraud:</span> <b>{max_pf:.3f}</b></div>',
            unsafe_allow_html=True,
        )

    st.divider()

    # Alerts + Evidence
    left, right = st.columns([1.2, 1.0], vertical_alignment="top")

    with left:
        st.markdown("## 🚨 Fraud Alerts (Top suspicious)")

        if "p_fraud" in view.columns and view["p_fraud"].notna().any():
            alerts = view.sort_values("p_fraud", ascending=False).head(25)
        else:
            alerts = view[view["is_fraud_prediction"] == 1].head(25)

        if alerts.empty:
            st.info("No fraud alerts in this window. To see fraud: increase fraud ratio in producer or lower threshold.")
        else:
            cols = [c for c in ["trans_num", "inserted_at", "amt", "p_fraud", "is_fraud_prediction", "is_fraud", "category_label", "merchant_label"] if c in alerts.columns]
            st.dataframe(alerts[cols], use_container_width=True, height=260)

        # ---- Inspect (fast search) ----
        st.markdown("## 🔎 Inspect a transaction (fast search)")

        if "search_tx" not in st.session_state:
            st.session_state["search_tx"] = ""
        if "prefix" not in st.session_state:
            st.session_state["prefix"] = ""
        if "suggest_pick" not in st.session_state:
            st.session_state["suggest_pick"] = None

        prefix = st.text_input(
            "Prefix (min 4 chars) → autocomplete from current window",
            key="prefix",
            placeholder="e.g. 4e2e / 3f1e / a6b5 ...",
        )

        candidates = []
        if prefix and len(prefix.strip()) >= 4 and "trans_num" in view.columns:
            pref = prefix.strip()
            s = view["trans_num"].astype(str)
            candidates = s[s.str.startswith(pref)].drop_duplicates().head(50).tolist()

        if candidates:
            picked = st.selectbox("Suggestions", candidates, key="suggest_pick")
            if st.button("Use selected trans_num", key="btn_use_selected"):
                st.session_state["search_tx"] = picked
                st.rerun()

        typed = st.text_input(
            "Search by trans_num (exact)",
            key="search_tx",
            placeholder="Paste full trans_num here…",
        )

        do_search = st.button("Search", type="primary", key="btn_search_exact")

        if do_search and typed.strip():
            tx = typed.strip()

            hit = view[view["trans_num"].astype(str) == tx].head(1)

            if hit.empty:
                hit = fetch_one_by_trans_num(tx)
                hit = ensure_types(hit)

            if hit.empty:
                st.warning("Không tìm thấy trans_num này (có thể LIMIT nhỏ / đang filter / record chưa insert).")
            else:
                r = hit.iloc[0].to_dict()
                pf = r.get("p_fraud", None)
                pred = r.get("is_fraud_prediction", None)
                amt = r.get("amt", None)

                st.markdown("### Transaction Detail")
                st.code(str(r.get("trans_num", "")), language="text")

                st.write(
                    {
                        "trans_num": r.get("trans_num"),
                        "inserted_at": str(r.get("inserted_at")),
                        "amount (amt)": amt,
                        "p_fraud": pf,
                        "prediction": pred,
                        "true_label (is_fraud)": r.get("is_fraud"),
                        "category_label": r.get("category_label"),
                        "merchant_label": r.get("merchant_label"),
                        "lat/long": (r.get("lat"), r.get("long")),
                        "merch_lat/merch_long": (r.get("merch_lat"), r.get("merch_long")),
                        "city_pop": r.get("city_pop"),
                    }
                )

                reason = []
                try:
                    if pf is not None and not (isinstance(pf, float) and np.isnan(pf)):
                        pfv = float(pf)
                        if pfv >= threshold:
                            reason.append(f"p_fraud={pfv:.3f} ≥ threshold={threshold:.2f} → flagged as fraud.")
                        else:
                            reason.append(f"p_fraud={pfv:.3f} < threshold={threshold:.2f} → not flagged by threshold.")
                except Exception:
                    pass

                try:
                    if amt is not None and not (isinstance(amt, float) and np.isnan(amt)):
                        if "amt" in view.columns and view["amt"].notna().any():
                            if float(amt) > np.nanpercentile(view["amt"].dropna(), 95):
                                reason.append("Amount is in the top ~5% of the current window (high-value transaction).")
                except Exception:
                    pass

                if reason:
                    st.success("• " + "\n• ".join(reason))
                else:
                    st.info("No explanation available (missing p_fraud/amt).")

    with right:
        st.markdown("## 📊 Evidence")

        if "p_fraud" in view.columns and view["p_fraud"].notna().any():
            fig = px.histogram(view, x="p_fraud", nbins=30, title="p_fraud distribution")
            fig.add_vline(
                x=threshold, line_width=3, line_dash="dash",
                annotation_text="threshold", annotation_position="top right"
            )
            st.plotly_chart(fig, use_container_width=True, key="pf_hist")
        else:
            st.info("p_fraud not found → enable p_fraud in Cassandra + consumer.")

        if "category_label" in view.columns and "is_fraud_prediction" in view.columns:
            fraud_only = view[view["is_fraud_prediction"] == 1]
            if fraud_only.empty:
                st.info("No predicted fraud rows in current window.")
            else:
                agg = (
                    fraud_only.groupby("category_label")
                    .size()
                    .reset_index(name="pred_fraud_count")
                    .sort_values("pred_fraud_count", ascending=False)
                    .head(20)
                )
                fig2 = px.bar(agg, x="category_label", y="pred_fraud_count", title="Predicted fraud count by category (Top 20)")
                st.plotly_chart(fig2, use_container_width=True, key="cat_bar")

        if "inserted_at" in view.columns and view["inserted_at"].notna().any():
            ts = view.dropna(subset=["inserted_at"]).copy()
            ts["minute"] = ts["inserted_at"].dt.floor("min")
            ts_agg = ts.groupby("minute").agg(
                total=("trans_num", "count"),
                pred_fraud=("is_fraud_prediction", "sum"),
            ).reset_index()

            fig3 = go.Figure()
            fig3.add_trace(go.Scatter(x=ts_agg["minute"], y=ts_agg["total"], mode="lines", name="Total/min"))
            fig3.add_trace(go.Scatter(x=ts_agg["minute"], y=ts_agg["pred_fraud"], mode="lines", name="Pred fraud/min"))
            fig3.update_layout(title="Streaming intensity over time", xaxis_title="time", yaxis_title="count")
            st.plotly_chart(fig3, use_container_width=True, key="ts")

    st.divider()

    st.markdown("## 🧾 Transaction Table (Clear fraud marking)")
    show_cols = [c for c in [
        "trans_num", "inserted_at", "amt", "p_fraud", "is_fraud_prediction", "is_fraud",
        "category_label", "merchant_label", "lat", "long", "merch_lat", "merch_long", "city_pop"
    ] if c in view.columns]

    table = view.copy()
    if "p_fraud" in table.columns and table["p_fraud"].notna().any():
        table = table.sort_values("p_fraud", ascending=False)

    if "p_fraud" in table.columns:
        def highlight_pf(val):
            try:
                v = float(val)
            except Exception:
                return ""
            if v >= threshold:
                return "background-color: rgba(255,0,0,0.18)"
            if v >= threshold * 0.7:
                return "background-color: rgba(255,165,0,0.18)"
            return ""

        styled = table[show_cols].head(200).style.applymap(highlight_pf, subset=["p_fraud"])
        st.dataframe(styled, use_container_width=True, height=360)
    else:
        st.dataframe(table[show_cols].head(200), use_container_width=True, height=360)

    st.caption("Legend: red = p_fraud ≥ threshold (flagged), orange = near-threshold (watch).")

    auto_refresh(refresh)


if __name__ == "__main__":
    main()
