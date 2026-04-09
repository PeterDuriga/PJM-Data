"""
PJM RT LMP Daily Updater
========================
Fetches hourly real-time LMP data for two nodes from the PJM Data Miner API
and appends new rows to pjm.csv.

Nodes:
  - Dominion (DOM): pnode_id 34885323
  - Wheeling Hub (WH): pnode_id 51287

Output columns: row_index, ept, wh_lmp, dom_lmp, diff
  ept     : datetime_beginning_ept (date only if midnight, else datetime)
  wh_lmp  : total_lmp_rt for WH node
  dom_lmp : total_lmp_rt for DOM node
  diff    : dom_lmp - wh_lmp
"""

import http.client
import urllib.parse
import csv
import io
import time
import datetime
import ssl
import certifi
import sys
import os

# =============================================================================
# Config — edit these as needed
# =============================================================================
API_KEY    = os.getenv("API_KEY")
if not API_KEY:
    raise ValueError("API_KEY environment variable not set")
BASE_URL   = "api.pjm.com"
ENDPOINT   = "rt_hrl_lmps"
DOM_NODE   = "34885323"   # Dominion
WH_NODE    = "51287"      # Wheeling Hub
OUTPUT_CSV = "pjm.csv"    # path to the CSV to append to
ROW_COUNT  = 50000        # max rows per API request (API maximum)

HEADERS = {"Ocp-Apim-Subscription-Key": API_KEY}
SSL_CTX = ssl.create_default_context(cafile=certifi.where())

# =============================================================================
# API helpers
# =============================================================================
def fmt_api(dt: datetime.datetime) -> str:
    """Format datetime for the API date range parameter."""
    return dt.strftime("%Y-%m-%d %H:%M")


def rate_limit(request_times: list, max_per_min: int = 6):
    """Block until we are under the API rate limit."""
    now = time.time()
    request_times[:] = [t for t in request_times if now - t < 60]
    if len(request_times) >= max_per_min:
        sleep_for = 60 - (now - request_times[0]) + 0.5
        print(f"  Rate limit reached — sleeping {sleep_for:.1f}s ...")
        time.sleep(sleep_for)
    request_times.append(time.time())


def fetch_page(start_dt, end_dt, pnode_id, start_row, request_times):
    """Fetch one paginated CSV page from the API. Returns raw bytes or None."""
    date_range = f"{fmt_api(start_dt)} to {fmt_api(end_dt)}"
    params = {
        "datetime_beginning_ept": date_range,
        "pnode_id":               pnode_id,
        "rowCount":               str(ROW_COUNT),
        "startRow":               str(start_row),
        "format":                 "csv",
        "download":               "true",
    }
    path = f"/api/v1/{ENDPOINT}?{urllib.parse.urlencode(params)}"
    rate_limit(request_times)
    for attempt in range(3):
        try:
            conn = http.client.HTTPSConnection(BASE_URL, context=SSL_CTX, timeout=60)
            conn.request("GET", path, headers=HEADERS)
            resp = conn.getresponse()
            if resp.status == 200:
                return resp.read()
            print(f"  HTTP {resp.status} (attempt {attempt + 1}/3)")
        except Exception as exc:
            print(f"  Request error (attempt {attempt + 1}/3): {exc}")
        time.sleep(2 ** attempt)
    return None


def fetch_node(start_dt, end_dt, pnode_id):
    """
    Fetch all pages for a single pnode over the given date range.
    Returns a dict keyed by datetime_beginning_ept -> total_lmp_rt (float).
    """
    request_times = []
    lmp_by_dt = {}
    start_row = 1

    while True:
        data = fetch_page(start_dt, end_dt, pnode_id, start_row, request_times)
        if data is None:
            print(f"  ERROR: gave up fetching pnode {pnode_id} at startRow={start_row}")
            break

        text   = io.StringIO(data.decode("utf-8-sig"))
        reader = csv.DictReader(text)
        rows   = list(reader)

        for row in rows:
            dt_key = row.get("datetime_beginning_ept", "").strip()
            try:
                lmp_by_dt[dt_key] = float(row["total_lmp_rt"])
            except (KeyError, ValueError):
                pass

        if len(rows) < ROW_COUNT:
            break   # last page
        start_row += ROW_COUNT

    return lmp_by_dt


# =============================================================================
# CSV helpers
# =============================================================================
def fmt_ept(dt: datetime.datetime) -> str:
    """
    Format EPT datetime to match R's default POSIXct output:
    omit the time component when the hour is midnight (00:00:00).
    """
    if dt.hour == 0 and dt.minute == 0 and dt.second == 0:
        return dt.strftime("%Y-%m-%d")
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def parse_ept(s: str) -> datetime.datetime:
    """Parse an EPT string from either the CSV or the API."""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
                "%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %I:%M %p", "%m/%d/%Y"):
        try:
            return datetime.datetime.strptime(s.strip(), fmt)
        except ValueError:
            pass
    raise ValueError(f"Cannot parse EPT string: {s!r}")


def read_csv_tail(path):
    """
    Scan the CSV and return (last_datetime, last_row_number).
    Only reads the first and last rows for efficiency — iterates full file
    to be safe since we need the last row index for appending.
    """
    last_dt  = None
    last_idx = 0
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            if len(row) < 2:
                continue
            try:
                last_idx = int(row[0].strip().strip('"'))
            except ValueError:
                pass
            try:
                last_dt = parse_ept(row[1])
            except ValueError:
                pass
    return last_dt, last_idx


# =============================================================================
# Main
# =============================================================================
def main():
    # ── Validate CSV exists ────────────────────────────────────────────────
    if not os.path.exists(OUTPUT_CSV):
        print(f"ERROR: '{OUTPUT_CSV}' not found. Place it in the same directory as this script.")
        sys.exit(1)

    # ── Determine fetch window ─────────────────────────────────────────────
    print(f"Reading last entry from {OUTPUT_CSV} ...")
    last_dt, last_idx = read_csv_tail(OUTPUT_CSV)

    if last_dt is None:
        print("ERROR: Could not parse any datetimes from the CSV.")
        sys.exit(1)

    # Start one hour after the last row; end at the last full hour of yesterday.
    # RT LMP data typically has a ~1-day publication lag.
    start_dt = last_dt + datetime.timedelta(hours=1)
    end_dt   = (datetime.datetime.today() - datetime.timedelta(days=1)).replace(
        hour=23, minute=0, second=0, microsecond=0
    )

    if start_dt > end_dt:
        print(f"CSV is already up to date (last entry: {last_dt:%Y-%m-%d %H:%M}).")
        return

    print(f"Fetching {start_dt:%Y-%m-%d %H:%M} → {end_dt:%Y-%m-%d %H:%M}")

    # ── Fetch both nodes ───────────────────────────────────────────────────
    print(f"  DOM node ({DOM_NODE}) ...")
    dom = fetch_node(start_dt, end_dt, DOM_NODE)
    print(f"    → {len(dom)} hours retrieved")

    print(f"  WH node  ({WH_NODE}) ...")
    wh  = fetch_node(start_dt, end_dt, WH_NODE)
    print(f"    → {len(wh)} hours retrieved")

    # ── Merge on common timestamps ─────────────────────────────────────────
    common_dts = sorted(set(dom) & set(wh), key=lambda s: parse_ept(s))
    if not common_dts:
        print("No overlapping data found for the two nodes — nothing to append.")
        return

    # ── Append to CSV ──────────────────────────────────────────────────────
    next_idx = last_idx + 1
    appended = 0

    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        for dt_str in common_dts:
            dt      = parse_ept(dt_str)
            ept     = fmt_ept(dt)
            dom_lmp = dom[dt_str]
            wh_lmp  = wh[dt_str]
            diff    = dom_lmp - wh_lmp
            # Match R's write.csv format: quoted row index, unquoted values
            f.write(f'"{next_idx}",{ept},{wh_lmp},{dom_lmp},{diff}\n')
            next_idx += 1
            appended += 1

    print(f"\nDone. Appended {appended} rows → CSV now has {next_idx - 1} data rows.")


if __name__ == "__main__":
    main()
