#!/usr/bin/env python3
#To see the pipeline and a sample report without Drill, use demo mode:
#python run_reports.py --demo
#The output will be in output/demo_funnel_report-<date>.xlsx
#The email will not be sent in demo mode.
#The report will be generated using mock data.
#The report will be generated using the data from the last 30 days.
#The report will be generated using the data from the last 30 days.
"""
run_reports.py — Entry point for the Funnel Report ETL Pipeline.

This file handles:
  1. Loading entity → To/CC mapping from recipients.json
  2. Writing the funnel table to a formatted Excel file
  3. Sending the report email with attachment via SMTP
  4. Running the full pipeline (fetch data via report_engine, write Excel, send email)

Usage: python run_reports.py
  Uses yesterday (t-1) by default. Configure via .env (see .env.example).
"""
import json
import os
import logging
import smtplib
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

import pandas as pd

# Import data and funnel logic from the other module
from report_engine import (
    load_config,
    fetch_stage_metrics,
    fetch_otp_totals,
    fetch_discovery_totals,
    fetch_fi_status_counts,
    aggregate_stages,
    build_report_table,
    get_mock_funnel_data,
)


# =============================================================================
# 1. RECIPIENTS — Load entity → To / CC from JSON
# =============================================================================

def load_recipients(path="recipients.json"):
    """Read recipients.json; return to_map (entity → list of To emails), cc_map (entity → CC list), and default_cc."""
    with open(path, "r") as f:
        data = json.load(f)
    to_map = data.get("to", {})
    cc_map = data.get("cc", {})
    default_cc = cc_map.get("default", ["cc@your-company.com"])
    return to_map, cc_map, default_cc


# =============================================================================
# 2. EXCEL — Write funnel table to file with styling
# =============================================================================

def write_funnel_excel(table_df, filepath):
    """Write the funnel report DataFrame to an Excel file with colors and column layout (gray headers, green success, brown dropoff)."""
    blank = pd.DataFrame({i: [""] for i in range(table_df.shape[1])})
    out = pd.concat([blank, table_df], ignore_index=True)

    with pd.ExcelWriter(filepath, engine="xlsxwriter") as wb:
        out.to_excel(wb, sheet_name="Funnel Dashboard", index=False, header=False)
        sheet = wb.sheets["Funnel Dashboard"]

        sheet.set_column(0, 0, 45)
        sheet.set_column(1, 1, 45)
        sheet.set_column(2, 2, 14)
        sheet.set_column(3, 3, 15)
        sheet.set_column(4, 4, 55)
        sheet.set_column(5, 5, 14)
        sheet.set_column(6, 6, 16)

        gray = wb.book.add_format({"bg_color": "#d9d9d9", "border": 1, "align": "left", "valign": "vcenter"})
        green = wb.book.add_format({"bg_color": "#aaecc6", "border": 1, "align": "left", "valign": "vcenter"})
        dark = wb.book.add_format({"bg_color": "#f5c8a7", "border": 1, "align": "left", "valign": "vcenter"})
        light = wb.book.add_format({"bg_color": "#fae4d3", "border": 1, "align": "left", "valign": "vcenter"})
        border = wb.book.add_format({"border": 1, "align": "left", "valign": "vcenter"})
        note_g = wb.book.add_format({"align": "left", "valign": "vcenter", "text_wrap": True, "bg_color": "#d9d9d9", "border": 1})
        note_w = wb.book.add_format({"align": "left", "valign": "vcenter", "text_wrap": True, "border": 1})

        sheet.merge_range(1, 3, 1, 4, out.iloc[1, 3], note_g)
        sheet.merge_range(2, 3, 2, 4, out.iloc[2, 3], note_w)
        sheet.merge_range(5, 2, 5, 3, out.iloc[5, 2], gray)
        sheet.merge_range(5, 5, 5, 6, out.iloc[5, 5], gray)

        success_rows = {7, 8, 9, 13, 18, 19, 22, 23, 24}
        drop_main = {7, 8, 9, 13, 18, 19, 22, 23, 24}
        drop_sub = {10, 11, 12, 14, 15, 16, 17, 20, 21}

        for r in range(6, 25):
            for c in range(7):
                fmt = border
                if r == 6 or c == 0:
                    fmt = gray
                if c in (1, 2, 3) and r in success_rows:
                    fmt = green
                if c == 4:
                    fmt = dark if r in drop_main else (light if r in drop_sub else border)
                if c in (5, 6) and r in drop_main:
                    fmt = dark
                val = out.iloc[r, c]
                if pd.isna(val):
                    sheet.write_blank(r, c, None, fmt)
                else:
                    sheet.write(r, c, val, fmt)

        sheet.write(1, 0, out.iloc[1, 0], gray)
        sheet.write(1, 1, out.iloc[1, 1], gray)
        for r in range(2, 4):
            for c in range(2):
                val = out.iloc[r, c]
                (sheet.write_blank if pd.isna(val) else sheet.write)(r, c, val if not pd.isna(val) else None, border)

        stage_fmt = wb.book.add_format({"align": "left", "valign": "vcenter", "text_wrap": True, "border": 1, "bg_color": "#d9d9d9"})
        sheet.merge_range("A10:A13", out.iloc[9, 0], stage_fmt)
        sheet.merge_range("A14:A18", out.iloc[13, 0], stage_fmt)
        sheet.merge_range("A20:A22", out.iloc[19, 0], stage_fmt)


# =============================================================================
# 3. EMAIL — Send report via SMTP
# =============================================================================

def send_report_mail(to_addrs, subject, body_html, attachments=None, cc_addrs=None, smtp_config=None):
    """Send an email with HTML body and optional file attachments. smtp_config: from, host, port, user, password. Returns True on success."""
    attachments = attachments or []
    cc_addrs = cc_addrs or []
    cfg = smtp_config or {}
    if not cfg.get("user") or not cfg.get("password"):
        logging.warning("SMTP not configured; skipping send.")
        return False
    try:
        msg = MIMEMultipart("alternative")
        msg["From"] = cfg.get("from") or cfg.get("user")
        msg["To"] = ", ".join(to_addrs)
        if cc_addrs:
            msg["Cc"] = ", ".join(cc_addrs)
        msg["Subject"] = subject
        plain = body_html.replace("<br>", "\n").replace("<b>", "").replace("</b>", "")
        msg.attach(MIMEText(plain, "plain"))
        msg.attach(MIMEText(body_html, "html"))
        for path in attachments:
            if os.path.isfile(path):
                with open(path, "rb") as f:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(f.read())
                    encoders.encode_base64(part)
                    part.add_header("Content-Disposition", f'attachment; filename="{os.path.basename(path)}"')
                    msg.attach(part)
        with smtplib.SMTP(cfg["host"], cfg["port"]) as srv:
            srv.starttls()
            srv.login(cfg["user"], cfg["password"])
            srv.send_message(msg)
        return True
    except Exception as e:
        logging.error("Mail send failed: %s", e)
        return False


# =============================================================================
# 4. PIPELINE — Run reports for all entities (fetch → Excel → email)
# =============================================================================

def run(demo=False, date_spec=None):
    """
    Run the ETL pipeline: for each entity, Extract (Drill or mock), Transform (funnel table),
    Load (Excel + optional email). Uses yesterday if date_spec is None.
    """
    cfg = load_config()
    to_map, cc_map, default_cc = load_recipients()
    out_dir = cfg["output_dir"]
    smtp = cfg["smtp"]

    if date_spec is None:
        date_spec = (datetime.now() - timedelta(days=1)).strftime("%d_%m_%Y")

    os.makedirs(out_dir, exist_ok=True)

    print("=" * 60)
    print("FUNNEL REPORT GENERATOR")
    print("=" * 60)
    print("Date:", date_spec)
    if demo:
        print("Mode: DEMO (mock data, no Drill required)")
    print("=" * 60)

    if demo:
        # Generate one sample report from mock data (no Drill)
        stages, otp, discovery, fi_status = get_mock_funnel_data()
        totals = aggregate_stages(stages)
        table = build_report_table(totals, otp, discovery, fi_status)
        out_path = os.path.join(out_dir, f"demo_funnel_report-{date_spec}.xlsx")
        write_funnel_excel(table, out_path)
        print("\nDemo report written:", out_path)
        print("(Email skipped in demo mode.)")
        print("\nDone.")
        return

    host = cfg["drill_host"]
    port = cfg["drill_port"]
    base = cfg["drill_base_path"]

    any_written = False
    for entity_id, to_list in to_map.items():
        print("\nEntity:", entity_id)
        safe_id = entity_id.replace("@", "-")
        out_path = os.path.join(out_dir, f"{safe_id}-{date_spec.replace(' -> ', '-')}.xlsx")
        try:
            # Extract: fetch from Drill
            stages = fetch_stage_metrics(base, entity_id, date_spec, host, port)
            if stages.empty:
                print("  No data; skipping.")
                continue
            otp = fetch_otp_totals(base, entity_id, date_spec, host, port)
            discovery = fetch_discovery_totals(base, entity_id, date_spec, host, port)
            fi_status = fetch_fi_status_counts(base, entity_id, date_spec, host, port)

            # Transform: build funnel table
            totals = aggregate_stages(stages)
            table = build_report_table(totals, otp, discovery, fi_status)

            # Load: write Excel and optionally send email
            write_funnel_excel(table, out_path)
            print("  Written:", out_path)
            any_written = True

            subj = f"{entity_id}_user_funnel_{date_spec}"
            body = f"Dear team,<br>Please find the user funnel for {entity_id} {date_spec}.<br><br>Thanks & Regards,<br>Your Team"
            cc_list = cc_map.get(entity_id, default_cc)
            if send_report_mail(to_list, subj, body, [out_path], cc_list, smtp):
                print("  Email sent.")
            else:
                print("  Email skipped (SMTP not configured).")
        except Exception as e:
            print("  Error:", e)

    if not any_written:
        print("\nNo reports generated (no data from Drill). Run with --demo to generate a sample report without Drill:")
        print("  python run_reports.py --demo")
    print("\nDone.")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Funnel Report ETL Pipeline — ETL pipeline for funnel analytics.")
    p.add_argument("--demo", action="store_true", help="Run with mock data; no Drill or .env required. Output: output/demo_funnel_report-<date>.xlsx")
    p.add_argument("--date", type=str, default=None, help="Date for report (dd_mm_yyyy). Default: yesterday.")
    args = p.parse_args()
    run(demo=args.demo, date_spec=args.date)
