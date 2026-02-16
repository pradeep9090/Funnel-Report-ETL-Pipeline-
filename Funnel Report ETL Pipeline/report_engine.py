#!/usr/bin/env python3
"""
report_engine.py — Data and funnel logic for the Funnel Report ETL Pipeline.

This file handles:
  1. Loading config from environment (Drill host/port/path, SMTP, output dir)
  2. Running SQL on Apache Drill and fetching funnel data (stages, OTP, discovery, FI status)
  3. Building the funnel summary table (aggregate stages, compute success/dropoff, percentages)

Run the pipeline via run_reports.py; this module is imported there.
"""
import os
import requests
import pandas as pd
from datetime import datetime, timedelta


# =============================================================================
# 1. SETTINGS — Load from environment (.env or os.environ)
# =============================================================================

def load_config():
    """Load Drill, output dir, and SMTP settings from env. Uses python-dotenv if available."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    return {
        "drill_host": os.environ.get("DRILL_HOST", "localhost"),
        "drill_port": int(os.environ.get("DRILL_PORT", "8047")),
        "drill_base_path": os.environ.get("DRILL_DATA_BASE", "/data/user-funnel"),
        "output_dir": os.environ.get("OUTPUT_DIR", "./output"),
        "smtp": {
            "from": os.environ.get("SMTP_FROM", ""),
            "host": os.environ.get("SMTP_HOST", "smtp.example.com"),
            "port": int(os.environ.get("SMTP_PORT", "587")),
            "user": os.environ.get("SMTP_USER", ""),
            "password": os.environ.get("SMTP_PASSWORD", ""),
        },
    }


# =============================================================================
# 2. DRILL QUERIES — Run SQL on Apache Drill, return DataFrames
# =============================================================================

def run_sql(sql, host, port):
    """Execute one SQL statement via Drill REST API; return rows as a DataFrame (or empty on error)."""
    url = f"http://{host}:{port}/query.json"
    try:
        r = requests.post(
            url,
            headers={"Content-Type": "application/json"},
            json={"queryType": "SQL", "query": sql, "options": {"drill.exec.http.rest.errors.verbose": "true"}},
        )
        r.raise_for_status()
        out = r.json()
        return pd.DataFrame(out["rows"]) if "rows" in out else pd.DataFrame()
    except requests.exceptions.RequestException as e:
        print(f"Drill request failed: {e}")
        return pd.DataFrame()


def _date_range(start_str, end_str):
    """Return list of dates as dd_mm_yyyy between start and end (inclusive)."""
    start = datetime.strptime(start_str.strip(), "%d_%m_%Y")
    end = datetime.strptime(end_str.strip(), "%d_%m_%Y")
    out = []
    cur = start
    while cur <= end:
        out.append(cur.strftime("%d_%m_%Y"))
        cur += timedelta(days=1)
    return out


def _month_prefixes(start_str, end_str):
    """Return list of month specs (*mm_yyyy) that cover the given date range (for stage CSV paths)."""
    start = datetime.strptime(start_str.strip(), "%d_%m_%Y")
    end = datetime.strptime(end_str.strip(), "%d_%m_%Y")
    out = []
    cur = start.replace(day=1)
    while cur <= end:
        out.append(f"*{cur.strftime('%m_%Y')}")
        cur = (cur + timedelta(days=32)).replace(day=1)
    return out


def fetch_stage_metrics(base_path, entity_id, date_spec, host, port):
    """
    Load funnel stage counts from Drill. date_spec: single day (dd_mm_yyyy), month (*mm_yyyy),
    or range (dd_mm_yyyy -> dd_mm_yyyy). Returns DataFrame with stage columns per row (e.g. per date).
    """
    if "->" in date_spec:
        parts = [p.strip() for p in date_spec.split("->")]
        prefixes = _month_prefixes(parts[0], parts[1])
        frames = []
        for p in prefixes:
            sql = f"SELECT * FROM dfs.`{base_path}/{p}/uf-stages-user-funnel-{p}.csv` WHERE Entity_ID = '{entity_id}'"
            frames.append(run_sql(sql, host, port))
        if not frames:
            return pd.DataFrame()
        combined = pd.concat(frames)
        combined["Date"] = pd.to_datetime(combined["Date"], format="%d-%m-%Y")
        start = datetime.strptime(parts[0], "%d_%m_%Y")
        end = datetime.strptime(parts[1], "%d_%m_%Y")
        return combined[(combined["Date"] >= start) & (combined["Date"] <= end)].reset_index(drop=True)
    if date_spec.startswith("*"):
        sql = f"SELECT * FROM dfs.`{base_path}/{date_spec}/uf-stages-user-funnel-{date_spec}.csv` WHERE Entity_ID = '{entity_id}'"
    else:
        sql = f"SELECT * FROM dfs.`{base_path}/{date_spec}/uf-stages-user-funnel-{date_spec}.csv` WHERE Entity_ID = '{entity_id}'"
    return run_sql(sql, host, port)


def fetch_otp_totals(base_path, entity_id, date_spec, host, port):
    """OTP breakdown: correct / incorrect / not entered. Same date_spec formats as fetch_stage_metrics. One row returned."""
    if "->" in date_spec:
        parts = [p.strip() for p in date_spec.split("->")]
        dates = _date_range(parts[0], parts[1])
        frames = []
        for d in dates:
            sql = f"""
            SELECT SUM(CAST(Correct_OTP_Entered AS DOUBLE)) AS Total_Correct_OTP_Entered,
                   SUM(CAST(Incorrect_OTP_Entered AS DOUBLE)) AS Total_Incorrect_OTP_Entered,
                   SUM(CAST(OTP_Not_Entered AS DOUBLE)) AS Total_OTP_Not_Entered
            FROM dfs.`{base_path}/{d}/otp-summary-user-funnel-{d}.csv` WHERE entity_id = '{entity_id}'
            """
            frames.append(run_sql(sql, host, port))
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames).sum(numeric_only=True).to_frame().T.reset_index(drop=True)
    sql = f"""
    SELECT SUM(CAST(Correct_OTP_Entered AS DOUBLE)) AS Total_Correct_OTP_Entered,
           SUM(CAST(Incorrect_OTP_Entered AS DOUBLE)) AS Total_Incorrect_OTP_Entered,
           SUM(CAST(OTP_Not_Entered AS DOUBLE)) AS Total_OTP_Not_Entered
    FROM dfs.`{base_path}/{date_spec}/otp-summary-user-funnel-{date_spec}.csv` WHERE entity_id = '{entity_id}'
    """
    return run_sql(sql, host, port)


def fetch_discovery_totals(base_path, entity_id, date_spec, host, port):
    """Discovery breakdown (Account_Discovered, Account_not_Found, etc.). One row returned."""
    if "->" in date_spec:
        parts = [p.strip() for p in date_spec.split("->")]
        dates = _date_range(parts[0], parts[1])
        frames = []
        for d in dates:
            sql = f"""
            SELECT SUM(CAST(NULLIF(Account_Discovered,'') AS DOUBLE)) AS Account_Discovered,
                   SUM(CAST(NULLIF(Account_not_Found,'') AS DOUBLE)) AS Account_not_Found,
                   SUM(CAST(NULLIF(FIP_Not_Selected,'') AS DOUBLE)) AS FIP_Not_Selected,
                   SUM(CAST(NULLIF(Failure,'') AS DOUBLE)) AS Failure,
                   SUM(CAST(NULLIF(NO_STATUS,'') AS DOUBLE)) AS NO_STATUS
            FROM dfs.`{base_path}/{d}/discovery-summary-user-funnel-{d}.csv` WHERE entity_id = '{entity_id}'
            """
            frames.append(run_sql(sql, host, port))
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames).sum(numeric_only=True).to_frame().T.reset_index(drop=True)
    sql = f"""
    SELECT SUM(CAST(NULLIF(Account_Discovered,'') AS DOUBLE)) AS Account_Discovered,
           SUM(CAST(NULLIF(Account_not_Found,'') AS DOUBLE)) AS Account_not_Found,
           SUM(CAST(NULLIF(FIP_Not_Selected,'') AS DOUBLE)) AS FIP_Not_Selected,
           SUM(CAST(NULLIF(Failure,'') AS DOUBLE)) AS Failure,
           SUM(CAST(NULLIF(NO_STATUS,'') AS DOUBLE)) AS NO_STATUS
    FROM dfs.`{base_path}/{date_spec}/discovery-summary-user-funnel-{date_spec}.csv` WHERE entity_id = '{entity_id}'
    """
    return run_sql(sql, host, port)


def fetch_fi_status_counts(base_path, entity_id, date_spec, host, port):
    """FI fetch status counts (Not Attempted, Failed, Success). Returns DataFrame with fetch_status and Count columns."""
    if "->" in date_spec:
        parts = [p.strip() for p in date_spec.split("->")]
        dates = _date_range(parts[0], parts[1])
        frames = []
        for d in dates:
            sql = f"""
            SELECT fetch_status, COUNT(fetch_status) AS Count
            FROM dfs.`{base_path}/{d}/user-funnel-{d}.csv`
            WHERE entity_id = '{entity_id}' AND fetch_status IN ('Not Attempted','Failed','Success')
              AND fetch_status IS NOT NULL AND fetch_status <> ''
            GROUP BY fetch_status
            """
            frames.append(run_sql(sql, host, port))
        if not frames:
            return pd.DataFrame()
        combined = pd.concat(frames)
        if combined.empty:
            return combined
        return combined.groupby("fetch_status")["Count"].sum().reset_index()
    sql = f"""
    SELECT fetch_status, COUNT(fetch_status) AS Count
    FROM dfs.`{base_path}/{date_spec}/user-funnel-{date_spec}.csv`
    WHERE entity_id = '{entity_id}' AND fetch_status IN ('Not Attempted','Failed','Success')
      AND fetch_status IS NOT NULL AND fetch_status <> ''
    GROUP BY fetch_status
    """
    return run_sql(sql, host, port)


# =============================================================================
# 3. FUNNEL TABLE — Aggregate stage data and build report rows
# =============================================================================

# Column names in the stage CSV that we sum for the funnel.
STAGE_COLUMNS = [
    "AA_client_Initialization",
    "OTP_Based_Sign_in_Sign_up",
    "View_Consent_Details",
    "Discovery",
    "Linking",
    "Rejected_Consent_Requests",
    "Approved_Consent_Requests",
    "FIP_Rejected_Consent_Artefacts",
    "FIP_Accepted_Consent_Artefacts",
    "Data_Fetch_Success",
    "Data_Fetch_Not_Attempted",
]


def aggregate_stages(stage_df):
    """Sum the stage columns across all rows (e.g. across dates); return a single Series (one value per column)."""
    subset = stage_df[STAGE_COLUMNS]
    return subset.astype(float).astype(int).sum()


def _pct(value, total):
    """Percentage of value over total, rounded to 1 decimal. Used for funnel % of initial users."""
    return round((value / total) * 100, 1) if total > 0 else 0


def build_report_table(stage_totals, otp_totals, discovery_totals, fi_status_df):
    """
    Build the funnel report as a DataFrame: summary rows at top, then stage table with
    success counts, dropoff counts, and percentages. stage_totals is a Series (output of aggregate_stages);
    otp_totals and discovery_totals are single-row DataFrames; fi_status_df has fetch_status and Count.
    """
    total_users = (
        int(stage_totals["AA_client_Initialization"])
        + int(stage_totals["OTP_Based_Sign_in_Sign_up"])
        + int(stage_totals["View_Consent_Details"])
        + int(stage_totals["Discovery"])
        + int(stage_totals["Linking"])
        + int(stage_totals["Rejected_Consent_Requests"])
        + int(stage_totals["Approved_Consent_Requests"])
    )
    pct = lambda x: _pct(x, total_users)

    d1 = int(stage_totals["AA_client_Initialization"])
    d2 = int(stage_totals["OTP_Based_Sign_in_Sign_up"])
    view_drop = int(stage_totals["View_Consent_Details"])
    auth_drop = d2 + view_drop

    disc = {}
    d3 = 0
    if not discovery_totals.empty:
        for col in ["Account_Discovered", "Account_not_Found", "FIP_Not_Selected", "Failure", "NO_STATUS"]:
            v = discovery_totals[col].iloc[0]
            disc[col] = int(float(v)) if pd.notna(v) else 0
        d3 = sum(disc.values())

    d4 = int(stage_totals["Linking"])
    rej = int(stage_totals["Rejected_Consent_Requests"])
    appr = int(stage_totals["Approved_Consent_Requests"])
    fip_rej = int(stage_totals["FIP_Rejected_Consent_Artefacts"])
    fip_ok = int(stage_totals["FIP_Accepted_Consent_Artefacts"])
    fetch_ok = int(stage_totals["Data_Fetch_Success"])
    not_attempted = int(stage_totals["Data_Fetch_Not_Attempted"])

    n_consent = total_users
    n_after_init = n_consent - d1
    n_after_auth = n_after_init - auth_drop
    n_after_disc = n_after_auth - d3
    n_after_link = n_after_disc - d4

    fi_req_ok = 0
    if not fi_status_df.empty and "fetch_status" in fi_status_df.columns:
        s = fi_status_df.loc[fi_status_df["fetch_status"] == "Success", "Count"].sum()
        f = fi_status_df.loc[fi_status_df["fetch_status"] == "Failed", "Count"].sum()
        fi_req_ok = int(s) + int(f)
    fi_fetch_drop = fi_req_ok - fetch_ok

    otp_wrong = int(otp_totals["Total_Incorrect_OTP_Entered"].iloc[0]) if not otp_totals.empty else 0
    otp_miss = int(otp_totals["Total_OTP_Not_Entered"].iloc[0]) if not otp_totals.empty else 0
    otp_ok_drop = d2 - (otp_wrong + otp_miss) + view_drop

    no_rec = disc.get("Account_not_Found", 0)
    fip_fail = disc.get("NO_STATUS", 0)
    some_fail = disc.get("Failure", 0)
    found_not_linked = disc.get("Account_Discovered", 0) + disc.get("FIP_Not_Selected", 0)

    table = [
        ["Summary", "% of initial users", "", "Note", "", "", ""],
        ["Percentage of initial users who approved the consent", pct(appr), "", "Please note that this funnel describes the journey of a user and not a consent request.", "", "", ""],
        ["Percentage of initial users who shared their data", pct(fetch_ok), "", "", "", "", ""],
        ["", "", "", "", "", "", ""],
        ["", "", "Successful Users", "", "", "Dropped off Users", ""],
        ["Stage", "Positive Action", "Count", "% of initial users", "Dropoff Cause", "Count", "% of initial users"],
        ["Consent Initiated", "AA successfully received a consent handle", n_consent, pct(n_consent), "AA did not receive a consent handle", 0, pct(0)],
        ["FIU initiated AA Client", "AA client was successfully initiated", n_after_init, pct(n_after_init), "AA client was not successfully initiated", d1, pct(d1)],
        ["Registration/Login", "User was authenticated", n_after_auth, pct(n_after_auth), "User was not authenticated", auth_drop, pct(auth_drop)],
        ["", "", "", "", "↳Incorrect OTP entered", otp_wrong, pct(otp_wrong)],
        ["", "", "", "", "↳OTP not received back", otp_miss, pct(otp_miss)],
        ["", "", "", "", "↳Correct OTP entered but user dropped off", otp_ok_drop, pct(otp_ok_drop)],
        ["Account Discovery", "User was able to find accounts", n_after_disc, pct(n_after_disc), "User was not able to find accounts", d3, pct(d3)],
        ["", "", "", "", "↳FIP returned 'No Records Found'", no_rec, pct(no_rec)],
        ["", "", "", "", "↳FIP failed to send records", fip_fail, pct(fip_fail)],
        ["", "", "", "", "↳Some FIP returned 'No Records Found' and some failed to send records", some_fail, pct(some_fail)],
        ["", "", "", "", "↳FIP returned accounts, but user did not link any accounts", found_not_linked, pct(found_not_linked)],
        ["Account Linking", "User was able to link accounts", n_after_link, pct(n_after_link), "User was not able to link accounts", d4, pct(d4)],
        ["Consent Request Review", "User approved the consent request", appr, pct(appr), "User did not approve the consent request", rej, pct(rej)],
        ["", "", "", "", "↳User rejected the consent", rej, pct(rej)],
        ["", "", "", "", "↳User did not take any action", "", ""],
        ["Consent Artefact Delivery", "FIP accepted the consent artefact", fip_ok, pct(fip_ok), "FIP rejected the consent artefact", fip_rej, pct(fip_rej)],
        ["FI Request", "FIU successfully requested the data", fi_req_ok, pct(fi_req_ok), "FIU did not request the data", not_attempted, pct(not_attempted)],
        ["FI Fetch", "FIU successfully received the data", fetch_ok, pct(fetch_ok), "FIU did not received the data", fi_fetch_drop, pct(fi_fetch_drop)],
    ]
    return pd.DataFrame(table)


# =============================================================================
# 4. DEMO MODE — Mock data for running pipeline without Drill (e.g. portfolio demo)
# =============================================================================

def get_mock_funnel_data():
    """
    Return synthetic stage_df, otp_df, discovery_df, fi_status_df so the pipeline
    can be run without Apache Drill (e.g. for demos or CI). Same schema as real fetch_* outputs.
    """
    # One row of stage totals (as if we had one day and summed)
    stage_row = {
        "AA_client_Initialization": 800,
        "OTP_Based_Sign_in_Sign_up": 450,
        "View_Consent_Details": 1050,
        "Discovery": 600,
        "Linking": 1600,
        "Rejected_Consent_Requests": 1950,
        "Approved_Consent_Requests": 1250,
        "FIP_Rejected_Consent_Artefacts": 150,
        "FIP_Accepted_Consent_Artefacts": 1100,
        "Data_Fetch_Success": 820,
        "Data_Fetch_Not_Attempted": 50,
    }
    stage_df = pd.DataFrame([stage_row])

    otp_df = pd.DataFrame([{
        "Total_Correct_OTP_Entered": 0,
        "Total_Incorrect_OTP_Entered": 450,
        "Total_OTP_Not_Entered": 1200,
    }])

    discovery_df = pd.DataFrame([{
        "Account_Discovered": 350,
        "Account_not_Found": 600,
        "FIP_Not_Selected": 400,
        "Failure": 150,
        "NO_STATUS": 200,
    }])

    fi_status_df = pd.DataFrame([
        {"fetch_status": "Success", "Count": 820},
        {"fetch_status": "Failed", "Count": 230},
        {"fetch_status": "Not Attempted", "Count": 50},
    ])

    return stage_df, otp_df, discovery_df, fi_status_df
