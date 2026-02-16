# Configuration

## Running the pipeline

- **With real data**: Set `.env` from `.env.example`, then run `python run_reports.py`. Uses yesterday’s date by default.
- **Demo (no Drill)**: Run `python run_reports.py --demo` to generate a sample report from mock data. No `.env` or Drill required.
- **Custom date**: `python run_reports.py --date 15_02_2026` (single day, format `dd_mm_yyyy`).

## Environment variables

Copy `.env.example` to `.env` and set values.

| Variable | Description | Example |
|----------|-------------|---------|
| `DRILL_HOST` | Apache Drill server host | `localhost` or your Drill hostname |
| `DRILL_PORT` | Drill REST API port | `8047` |
| `DRILL_DATA_BASE` | Base path for CSV data in Drill dfs storage | `/data/user-funnel` |
| `SMTP_FROM` | From address for emails | `reports@your-company.com` |
| `SMTP_HOST` | SMTP server | `smtp.example.com` |
| `SMTP_PORT` | SMTP port (e.g. TLS) | `587` |
| `SMTP_USER` | SMTP login | same as From or your SMTP user |
| `SMTP_PASSWORD` | SMTP password or app password | *(set in .env only)* |
| `OUTPUT_DIR` | Directory for generated Excel files | `./output` |

- If `SMTP_USER` or `SMTP_PASSWORD` is missing, the script still generates reports but skips sending email.
- `OUTPUT_DIR` is created if it does not exist.
- `DRILL_DATA_BASE` must match where your user-funnel CSVs are mounted in Drill (see [Data sources](DATA_SOURCES.md)).

## Entity → email mapping

Recipients are defined in **`recipients.json`** at the project root:

- **`to`** – entity ID → list of **To** email addresses.
- **`cc`** – entity ID → list of **CC** addresses; use **`default`** for entities not listed.

Edit `recipients.json` to add or change entities and addresses.

## Running for a custom date

The script uses **yesterday (t-1)** by default. To use another date or range, set `date_spec` in the `run()` function in `run_reports.py` (e.g. `date_spec = "15_02_2026"` or `"01_02_2026 -> 15_02_2026"`), or add a CLI argument (e.g. `argparse`).

Date formats supported:

- Single day: `dd_mm_yyyy`
- Full month: `*mm_yyyy`
- Range: `dd_mm_yyyy -> dd_mm_yyyy`
