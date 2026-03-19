# Data sources

The report is built from CSV data queried via **Apache Drill**. The base path is set by **`DRILL_DATA_BASE`** in `.env` (e.g. `/data/user-funnel`). All file paths below are relative to that base.

## Path patterns

`{date}` is either:

- `dd_mm_yyyy` for a single day, or  
- `*mm_yyyy` for a full month.

| File pattern | Purpose |
|--------------|----------|
| `{date}/uf-stages-user-funnel-{date}.csv` | Funnel stage counts per entity and date. Columns include `Entity_ID`, `Date`, and stage columns below. |
| `{date}/otp-summary-user-funnel-{date}.csv` | OTP breakdown: `entity_id`, `Correct_OTP_Entered`, `Incorrect_OTP_Entered`, `OTP_Not_Entered`. |
| `{date}/discovery-summary-user-funnel-{date}.csv` | Discovery: `entity_id`, `Account_Discovered`, `Account_not_Found`, `FIP_Not_Selected`, `Failure`, `NO_STATUS`. |
| `{date}/user-funnel-{date}.csv` | Per-row funnel data including `entity_id`, `fetch_status` (Not Attempted / Failed / Success). |

Set `DRILL_DATA_BASE` to match where these files are available in your Drill dfs storage (e.g. your NFS or object-store path).

## Stage columns (uf-stages)

Used in the dashboard:

- `AA_client_Initialization`
- `OTP_Based_Sign_in_Sign_up`
- `View_Consent_Details`
- `Discovery`
- `Linking`
- `Rejected_Consent_Requests`
- `Approved_Consent_Requests`
- `FIP_Rejected_Consent_Artefacts`
- `FIP_Accepted_Consent_Artefacts`
- `Data_Fetch_Success`
- `Data_Fetch_Not_Attempted`

For date ranges, the script aggregates across all days in the range.
