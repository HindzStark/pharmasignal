"""PharmaSignal dashboard: MSD brands vs competitors on FDA adverse-event data, served from Athena.

Run: streamlit run dashboard/app.py   (uses your default AWS profile; region ap-south-1)
Every query runs in the stack's Athena workgroup, which enforces a per-query scan limit; results
are cached for 10 minutes so browsing the dashboard doesn't re-scan S3.
"""
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline import aws  # noqa: E402

st.set_page_config(page_title="PharmaSignal", layout="wide")
st.title("PharmaSignal: drug safety intelligence")
st.caption("Source: FDA Adverse Event Reporting System (FAERS) via openFDA, curated by AWS Glue and "
           "queried with Amazon Athena. Spontaneous reports show association, not causation, and are not "
           "for clinical use.")


@st.cache_data(ttl=600, show_spinner="Querying Athena...")
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    df = aws.athena_query(sql, list(params) or None)
    for col in df.columns:  # Athena returns strings; restore numbers
        converted = pd.to_numeric(df[col], errors="coerce")
        if converted.notna().sum() == df[col].notna().sum():
            df[col] = converted
    return df


try:
    areas = query("select distinct therapy_area from dim_drug order by 1")["therapy_area"].tolist()
except Exception as exc:  # stack missing, no credentials, or tables not built yet
    st.error(f"Could not query Athena: {exc}\n\nDeploy with `scripts/deploy.ps1`, then run "
             "`python -m pipeline.run_pipeline --skip-extract`.")
    st.stop()

area = st.sidebar.selectbox("Therapy area", areas)
min_reports = st.sidebar.slider("Min. reports for a drug-reaction pair", 3, 50, 3)

# --- KPI benchmark ---------------------------------------------------------
st.subheader(f"{area}: safety benchmark")
summary = query(
    "select brand_name, company, total_reports, serious_rate, death_rate, hospitalization_rate, hcp_reported_rate "
    "from mart_drug_safety_summary where therapy_area = ? order by total_reports desc", (area,))
cols = st.columns(len(summary) or 1)
for col, row in zip(cols, summary.itertuples()):
    col.metric(f"{row.brand_name} ({row.company})", f"{int(row.total_reports):,} reports",
               f"{row.serious_rate:.0%} serious", delta_color="off")
st.bar_chart(summary.set_index("brand_name")[["serious_rate", "death_rate", "hospitalization_rate"]], stack=False)

# --- Signals ---------------------------------------------------------------
st.subheader("Disproportionality signals vs therapy-area peers")
st.caption("A signal is flagged when reports ≥ 3, PRR ≥ 2 and χ² ≥ 4 (Evans criteria).")
brand = st.selectbox("Brand", summary["brand_name"].tolist())
signals = query(
    "select reaction_pt as reaction, reports_with_reaction as reports, prr, chi_square, is_signal "
    "from mart_safety_signals where therapy_area = ? and brand_name = ? and reports_with_reaction >= ? "
    "order by is_signal desc, prr desc nulls last limit 50", (area, brand, min_reports))
signals["is_signal"] = signals["is_signal"].astype(str).str.lower().eq("true")
st.dataframe(signals, width="stretch", hide_index=True)

# --- Trend -----------------------------------------------------------------
st.subheader("Report volume by quarter")
trend = query(
    "select f.received_quarter, d.brand_name, count(distinct f.report_id) as reports "
    "from fct_drug_reactions f join dim_drug d on f.generic_name = d.generic_name "
    "where f.therapy_area = ? group by 1, 2 order by 1", (area,))
if not trend.empty:
    st.line_chart(trend.pivot(index="received_quarter", columns="brand_name", values="reports"))
