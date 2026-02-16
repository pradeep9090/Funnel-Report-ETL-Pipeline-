# Architecture

## ETL Flow

1. **Extract** — For each entity and date range, the pipeline queries Apache Drill (REST API) for four datasets:
   - Funnel stage counts (per day or month)
   - OTP summary (correct / incorrect / not entered)
   - Discovery summary (account discovered, not found, FIP not selected, failure, no status)
   - FI fetch status counts (Success, Failed, Not Attempted)

   All four are returned as pandas DataFrames. Date handling supports single day, full month, or a range (multiple Drill queries are issued and results concatenated).

2. **Transform** — Stage rows are aggregated (sum of stage columns). Those totals are combined with OTP, discovery, and FI status DataFrames to compute:
   - Success counts at each funnel stage
   - Dropoff counts and subcauses (e.g. incorrect OTP, FIP no records, user rejected consent)
   - Percent of initial users at each step

   Output is a single DataFrame (the “funnel table”) ready for Excel.

3. **Load** — The funnel table is written to an Excel file with formatting (headers, colors, column widths, merged cells). Optionally, the same file is attached to an email and sent via SMTP to entity-specific recipients from `recipients.json`.

## Modules

- **report_engine.py**: Configuration (env), Drill client (run_sql, fetch_*), funnel logic (aggregate_stages, build_report_table), and mock data for demo mode.
- **run_reports.py**: Recipient mapping (JSON), Excel writer, email sender, and the main loop that calls report_engine and writes/sends output. CLI (argparse) for `--demo` and `--date`.

## Data Flow (per entity)

```
Drill (or mock) → stage_df, otp_df, discovery_df, fi_status_df
       → aggregate_stages(stage_df) → stage_totals (Series)
       → build_report_table(stage_totals, otp_df, discovery_df, fi_status_df) → table (DataFrame)
       → write_funnel_excel(table, path) → .xlsx
       → optional: send_report_mail(..., [path], ...)
```

## Demo Mode

With `--demo`, the pipeline skips Drill entirely. `get_mock_funnel_data()` in report_engine returns synthetic DataFrames with the same schema as the real fetch_* functions. Transform and Load run as usual, producing one sample Excel file. This allows the project to be run and reviewed without an Apache Drill instance or .env configuration.
