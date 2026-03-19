# Sample output: Funnel dashboard

This is a **snapshot** of how the final Excel report looks. The real file is a single sheet **"Funnel Dashboard"** with formatted cells (grey header, green for success columns, brown for dropoff columns).

---

## Summary (top section)

| Summary | % of initial users | | Note |
|--------|---------------------|---|------|
| Percentage of initial users who approved the consent | **12.5** | | *Please note that this funnel describes the journey of a user and not a consent request.* |
| Percentage of initial users who shared their data | **8.2** | | |

---

## Funnel stages (main table)

| Stage | Positive Action | Count | % of initial users | Dropoff Cause | Count | % of initial users |
|-------|-----------------|-------|--------------------|---------------|-------|--------------------|
| **Consent Initiated** | AA successfully received a consent handle | 10,000 | 100.0 | AA did not receive a consent handle | 0 | 0.0 |
| **FIU initiated AA Client** | AA client was successfully initiated | 9,200 | 92.0 | AA client was not successfully initiated | 800 | 8.0 |
| **Registration/Login** | User was authenticated | 6,500 | 65.0 | User was not authenticated | 2,700 | 27.0 |
| | | | | ↳ Incorrect OTP entered | 450 | 4.5 |
| | | | | ↳ OTP not received back | 1,200 | 12.0 |
| | | | | ↳ Correct OTP entered but user dropped off | 1,050 | 10.5 |
| **Account Discovery** | User was able to find accounts | 4,800 | 48.0 | User was not able to find accounts | 1,700 | 17.0 |
| | | | | ↳ FIP returned 'No Records Found' | 600 | 6.0 |
| | | | | ↳ FIP failed to send records | 200 | 2.0 |
| | | | | ↳ Some FIP returned 'No Records Found' and some failed | 150 | 1.5 |
| | | | | ↳ FIP returned accounts, but user did not link any | 750 | 7.5 |
| **Account Linking** | User was able to link accounts | 3,200 | 32.0 | User was not able to link accounts | 1,600 | 16.0 |
| **Consent Request Review** | User approved the consent request | 1,250 | 12.5 | User did not approve the consent request | 1,950 | 19.5 |
| | | | | ↳ User rejected the consent | 1,950 | 19.5 |
| | | | | ↳ User did not take any action | | |
| **Consent Artefact Delivery** | FIP accepted the consent artefact | 1,100 | 11.0 | FIP rejected the consent artefact | 150 | 1.5 |
| **FI Request** | FIU successfully requested the data | 1,050 | 10.5 | FIU did not request the data | 50 | 0.5 |
| **FI Fetch** | FIU successfully received the data | 820 | 8.2 | FIU did not receive the data | 230 | 2.3 |

---

## Excel formatting (reference)

- **Grey** – Headers (Stage, Positive Action, Count, Dropoff Cause, etc.).
- **Green** – Success side: Positive Action, Count, % of initial users for successful stage rows.
- **Dark brown** – Main dropoff rows (cause + count + %).
- **Light brown** – Subcause rows (↳ lines).
- Column A: stage names and merged cells for multi-row stages (e.g. Registration/Login, Account Discovery).
- Columns B–D: successful users; E–G: dropped-off users and subcauses.

---

## Generated file name

Reports are saved as:

`{OUTPUT_DIR}/{fiuid}-{date}.xlsx`

Example: `FIU_001-16_02_2026.xlsx` for FIU `FIU_001` and date `16_02_2026`.
