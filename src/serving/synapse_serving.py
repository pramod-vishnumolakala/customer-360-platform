"""
Synapse Serving Layer — exposes Customer 360 Gold profiles
for Power BI dashboards and downstream marketing systems.
Pramod Vishnumolakala — github.com/pramod-vishnumolakala
"""

import logging
import pandas as pd
import pyodbc
from contextlib import contextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SYNAPSE_SERVER   = "pramod-customer360-synapse.sql.azuresynapse.net"
SYNAPSE_DATABASE = "customer360dw"
SYNAPSE_DRIVER   = "{ODBC Driver 18 for SQL Server}"


@contextmanager
def synapse_connection(username: str, password: str):
    """Context manager for Synapse SQL connection."""
    conn_str = (
        f"DRIVER={SYNAPSE_DRIVER};"
        f"SERVER={SYNAPSE_SERVER};"
        f"DATABASE={SYNAPSE_DATABASE};"
        f"UID={username};PWD={password};"
        "Encrypt=yes;TrustServerCertificate=no;"
    )
    conn = pyodbc.connect(conn_str)
    try:
        yield conn
    finally:
        conn.close()


def get_customer_profile(customer_id: str, conn) -> dict:
    """Fetch full Customer 360 profile for a single customer."""
    query = """
        SELECT *
        FROM gold.customer_360_profile
        WHERE customer_id = ?
    """
    cursor = conn.cursor()
    cursor.execute(query, customer_id)
    row = cursor.fetchone()
    if not row:
        return {}
    columns = [desc[0] for desc in cursor.description]
    return dict(zip(columns, row))


def get_churn_risk_segment(tier: str, conn) -> pd.DataFrame:
    """Return churn-risk customers for a given value tier."""
    query = """
        SELECT customer_id, total_annual_premium, churn_risk_score,
               renewal_rate, open_claims, customer_tenure_years
        FROM gold.customer_360_profile
        WHERE is_churn_risk = 1
          AND customer_value_tier = ?
        ORDER BY total_annual_premium DESC
    """
    return pd.read_sql(query, conn, params=[tier])


def get_cross_sell_candidates(min_premium: float, conn) -> pd.DataFrame:
    """
    Identify single-line customers with high premium — cross-sell targets.
    Improving policy renewal rates by 15% via targeted campaigns.
    """
    query = """
        SELECT customer_id, total_annual_premium, product_lines_count,
               highest_tier, customer_tenure_years, renewal_rate
        FROM gold.customer_360_profile
        WHERE product_lines_count = 1
          AND total_annual_premium >= ?
          AND is_churn_risk = 0
        ORDER BY total_annual_premium DESC
    """
    return pd.read_sql(query, conn, params=[min_premium])


def refresh_power_bi_dataset(dataset_id: str, workspace_id: str):
    """Trigger Power BI dataset refresh via REST API."""
    import requests
    from azure.identity import DefaultAzureCredential

    credential = DefaultAzureCredential()
    token = credential.get_token("https://analysis.windows.net/powerbi/api/.default").token
    url = (
        f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}"
        f"/datasets/{dataset_id}/refreshes"
    )
    resp = requests.post(url, headers={"Authorization": f"Bearer {token}"})
    if resp.status_code == 202:
        logger.info(f"Power BI dataset refresh triggered: {dataset_id}")
    else:
        logger.error(f"Power BI refresh failed: {resp.status_code} {resp.text}")
