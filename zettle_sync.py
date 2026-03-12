#!/usr/bin/env python3
"""
Zettle → Notion Transaction Sync

Pulls transactions from two Zettle accounts, enriches data with
VAT/fee calculations and artist tags, and pushes to Notion databases.

Usage:
    python zettle_sync.py --start-date 2026-03-01 --end-date 2026-03-31
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ZETTLE_TOKEN_URL = "https://oauth.zettle.com/token"
ZETTLE_PURCHASES_URL = "https://purchase.izettle.com/purchases/v2"

NOTION_API_URL = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"

LOG_FILE = os.getenv("LOG_FILE", "zettle_sync.log")
LAST_SYNC_FILE = os.getenv("LAST_SYNC_FILE", ".last_sync")

# VAT rates (Netherlands)
VAT_STANDARD = 0.21
VAT_REDUCED = 0.09

# Product groups that use reduced VAT (9%)
REDUCED_VAT_PRODUCTS = {"food", "drinks", "boeken", "books"}

# Product group extraction: first word of the product name
# e.g. "T-shirt M" → "T-shirt", "Vinyl LP" → "Vinyl"

ACCOUNTS = []

for i in (1, 2):
    client_id = os.getenv(f"ZETTLE_ACCOUNT{i}_CLIENT_ID")
    client_secret = os.getenv(f"ZETTLE_ACCOUNT{i}_CLIENT_SECRET")
    artist_name = os.getenv(f"ZETTLE_ACCOUNT{i}_ARTIST_NAME", f"Account {i}")
    if client_id and client_secret:
        ACCOUNTS.append(
            {
                "client_id": client_id,
                "client_secret": client_secret,
                "artist_name": artist_name,
            }
        )

NOTION_API_KEY = os.getenv("NOTION_API_KEY", "")
NOTION_TRANSACTIONS_DB_ID = os.getenv("NOTION_TRANSACTIONS_DB_ID", "")
NOTION_SUMMARY_DB_ID = os.getenv("NOTION_SUMMARY_DB_ID", "")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger("zettle_sync")
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler(LOG_FILE)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(message)s"))

logger.addHandler(file_handler)
logger.addHandler(console_handler)


# ---------------------------------------------------------------------------
# Zettle API
# ---------------------------------------------------------------------------


def zettle_get_token(client_id: str, client_secret: str) -> str:
    """Obtain an OAuth2 access token from Zettle."""
    resp = requests.post(
        ZETTLE_TOKEN_URL,
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "client_id": client_id,
            "assertion": client_secret,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def zettle_fetch_purchases(
    token: str, start_date: str, end_date: str
) -> list[dict]:
    """Fetch all purchases within the date range (paginated)."""
    headers = {"Authorization": f"Bearer {token}"}
    all_purchases = []
    params = {
        "startDate": f"{start_date}T00:00:00.000Z",
        "endDate": f"{end_date}T23:59:59.999Z",
        "limit": 100,
    }

    while True:
        resp = requests.get(
            ZETTLE_PURCHASES_URL, headers=headers, params=params, timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
        purchases = data.get("purchases", [])
        all_purchases.extend(purchases)

        last_id = data.get("lastPurchaseHash")
        if not last_id or len(purchases) < 100:
            break
        params["lastPurchaseHash"] = last_id

    return all_purchases


# ---------------------------------------------------------------------------
# Data Enrichment
# ---------------------------------------------------------------------------


def extract_product_group(product_name: str) -> str:
    """Extract product group from product name (first word)."""
    if not product_name:
        return "Unknown"
    return product_name.strip().split()[0]


def get_vat_rate(product_group: str) -> float:
    """Return VAT rate based on product group."""
    if product_group.lower() in REDUCED_VAT_PRODUCTS:
        return VAT_REDUCED
    return VAT_STANDARD


def enrich_transaction(purchase: dict, artist_name: str) -> list[dict]:
    """
    Enrich a Zettle purchase into one or more transaction records.
    Each product line in a purchase becomes its own transaction row.
    """
    records = []
    purchase_id = purchase.get("purchaseUUID") or purchase.get("purchaseUUID1")
    timestamp = purchase.get("timestamp", "")
    zettle_fee_total = abs(purchase.get("fee", {}).get("amount", 0)) / 100

    products = purchase.get("products", [])
    gross_total = sum(
        (p.get("unitPrice", 0) * p.get("quantity", 1)) for p in products
    ) / 100

    for product in products:
        product_name = product.get("name", "Unknown")
        quantity = product.get("quantity", 1)
        unit_price = product.get("unitPrice", 0) / 100
        line_gross = unit_price * quantity

        product_group = extract_product_group(product_name)
        vat_rate = get_vat_rate(product_group)

        # Gross is VAT-inclusive → Net = Gross / (1 + VAT)
        line_net = round(line_gross / (1 + vat_rate), 2)
        line_vat = round(line_gross - line_net, 2)

        # Distribute Zettle fee proportionally across product lines
        fee_share = (
            round(zettle_fee_total * (line_gross / gross_total), 2)
            if gross_total
            else 0
        )
        line_after_fees = round(line_net - fee_share, 2)

        row_id = f"{purchase_id}_{product.get('productUuid', product_name)}"

        records.append(
            {
                "transaction_id": row_id,
                "purchase_id": purchase_id,
                "date": timestamp,
                "artist": artist_name,
                "product_name": product_name,
                "product_group": product_group,
                "quantity": quantity,
                "amount_gross": line_gross,
                "vat_rate": vat_rate,
                "vat_amount": line_vat,
                "amount_net": line_net,
                "zettle_fee": fee_share,
                "amount_after_fees": line_after_fees,
            }
        )

    return records


# ---------------------------------------------------------------------------
# Notion API
# ---------------------------------------------------------------------------


def notion_headers() -> dict:
    return {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def notion_query_existing_ids(database_id: str) -> set[str]:
    """Fetch all existing transaction_ids from Notion for duplicate detection."""
    existing = set()
    url = f"{NOTION_API_URL}/databases/{database_id}/query"
    has_more = True
    start_cursor = None

    while has_more:
        body = {"page_size": 100}
        if start_cursor:
            body["start_cursor"] = start_cursor

        resp = requests.post(
            url, headers=notion_headers(), json=body, timeout=30
        )
        resp.raise_for_status()
        data = resp.json()

        for page in data.get("results", []):
            props = page.get("properties", {})
            tid_prop = props.get("Transaction ID", {})
            rich_text = tid_prop.get("rich_text", [])
            if rich_text:
                existing.add(rich_text[0].get("plain_text", ""))

        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

    return existing


def notion_create_transaction(database_id: str, record: dict) -> str:
    """Create a single transaction page in Notion. Returns the page ID."""
    url = f"{NOTION_API_URL}/pages"

    date_str = record["date"]
    if date_str:
        # Parse ISO timestamp to date-only for Notion
        try:
            date_str = datetime.fromisoformat(
                date_str.replace("Z", "+00:00")
            ).strftime("%Y-%m-%d")
        except (ValueError, AttributeError):
            pass

    properties = {
        "Transaction ID": {
            "rich_text": [{"text": {"content": record["transaction_id"]}}]
        },
        "Date": {"date": {"start": date_str}} if date_str else {"date": None},
        "Artist": {"select": {"name": record["artist"]}},
        "Product": {
            "title": [{"text": {"content": record["product_name"]}}]
        },
        "Product Group": {"select": {"name": record["product_group"]}},
        "Quantity": {"number": record["quantity"]},
        "Amount Gross": {"number": record["amount_gross"]},
        "VAT Rate": {"number": record["vat_rate"]},
        "VAT Amount": {"number": record["vat_amount"]},
        "Amount Net": {"number": record["amount_net"]},
        "Zettle Fee": {"number": record["zettle_fee"]},
        "Amount After Fees": {"number": record["amount_after_fees"]},
    }

    body = {"parent": {"database_id": database_id}, "properties": properties}
    resp = requests.post(url, headers=notion_headers(), json=body, timeout=30)
    resp.raise_for_status()
    return resp.json()["id"]


def notion_update_summary(database_id: str, records: list[dict]) -> None:
    """
    Update (or create) summary rows in the Notion Summary database.
    Groups by artist + product_group and upserts totals.
    """
    if not database_id:
        logger.info("No summary database configured, skipping summary update.")
        return

    # Aggregate
    summary: dict[tuple[str, str], dict] = {}
    for r in records:
        key = (r["artist"], r["product_group"])
        if key not in summary:
            summary[key] = {
                "artist": r["artist"],
                "product_group": r["product_group"],
                "total_quantity": 0,
                "total_gross": 0.0,
                "total_net": 0.0,
                "total_fees": 0.0,
                "total_after_fees": 0.0,
            }
        s = summary[key]
        s["total_quantity"] += r["quantity"]
        s["total_gross"] += r["amount_gross"]
        s["total_net"] += r["amount_net"]
        s["total_fees"] += r["zettle_fee"]
        s["total_after_fees"] += r["amount_after_fees"]

    # Query existing summary pages to find matches
    existing_pages: dict[tuple[str, str], str] = {}
    url = f"{NOTION_API_URL}/databases/{database_id}/query"
    resp = requests.post(
        url, headers=notion_headers(), json={"page_size": 100}, timeout=30
    )
    resp.raise_for_status()
    for page in resp.json().get("results", []):
        props = page.get("properties", {})
        artist_prop = props.get("Artist", {}).get("select", {})
        group_title = props.get("Product Group", {}).get("title", [])
        if artist_prop and group_title:
            a = artist_prop.get("name", "")
            g = group_title[0].get("plain_text", "")
            existing_pages[(a, g)] = page["id"]

    for key, s in summary.items():
        properties = {
            "Product Group": {
                "title": [{"text": {"content": s["product_group"]}}]
            },
            "Artist": {"select": {"name": s["artist"]}},
            "Total Quantity": {"number": s["total_quantity"]},
            "Total Gross": {"number": round(s["total_gross"], 2)},
            "Total Net": {"number": round(s["total_net"], 2)},
            "Total Fees": {"number": round(s["total_fees"], 2)},
            "Total After Fees": {"number": round(s["total_after_fees"], 2)},
        }

        if key in existing_pages:
            page_id = existing_pages[key]
            requests.patch(
                f"{NOTION_API_URL}/pages/{page_id}",
                headers=notion_headers(),
                json={"properties": properties},
                timeout=30,
            ).raise_for_status()
            logger.debug("Updated summary: %s / %s", *key)
        else:
            requests.post(
                f"{NOTION_API_URL}/pages",
                headers=notion_headers(),
                json={
                    "parent": {"database_id": database_id},
                    "properties": properties,
                },
                timeout=30,
            ).raise_for_status()
            logger.debug("Created summary: %s / %s", *key)

    logger.info("Summary database updated with %d groups.", len(summary))


# ---------------------------------------------------------------------------
# Main Sync Logic
# ---------------------------------------------------------------------------


def sync(start_date: str, end_date: str) -> None:
    if not ACCOUNTS:
        logger.error(
            "No Zettle accounts configured. Check your .env file."
        )
        sys.exit(1)

    if not NOTION_API_KEY or not NOTION_TRANSACTIONS_DB_ID:
        logger.error(
            "Notion API key or Transactions DB ID missing. Check your .env file."
        )
        sys.exit(1)

    # Fetch existing transaction IDs for duplicate detection
    logger.info("Fetching existing transaction IDs from Notion...")
    try:
        existing_ids = notion_query_existing_ids(NOTION_TRANSACTIONS_DB_ID)
    except requests.RequestException as e:
        logger.error("Failed to query Notion for existing IDs: %s", e)
        sys.exit(1)
    logger.info("Found %d existing transactions in Notion.", len(existing_ids))

    all_records: list[dict] = []
    counts: dict[str, int] = {}

    for account in ACCOUNTS:
        artist = account["artist_name"]
        logger.info("Processing account: %s", artist)

        # Authenticate
        try:
            token = zettle_get_token(
                account["client_id"], account["client_secret"]
            )
        except requests.RequestException as e:
            logger.error("Auth failed for %s: %s", artist, e)
            continue

        # Fetch purchases
        try:
            purchases = zettle_fetch_purchases(token, start_date, end_date)
        except requests.RequestException as e:
            logger.error("Fetch failed for %s: %s", artist, e)
            continue

        logger.info("Fetched %d purchases for %s.", len(purchases), artist)

        # Enrich and collect
        artist_records = []
        for purchase in purchases:
            enriched = enrich_transaction(purchase, artist)
            artist_records.extend(enriched)

        all_records.extend(artist_records)
        counts[artist] = 0

        # Push to Notion (skip duplicates)
        for record in artist_records:
            if record["transaction_id"] in existing_ids:
                logger.debug("Skipping duplicate: %s", record["transaction_id"])
                continue

            try:
                notion_create_transaction(
                    NOTION_TRANSACTIONS_DB_ID, record
                )
                existing_ids.add(record["transaction_id"])
                counts[artist] += 1
            except requests.RequestException as e:
                logger.error(
                    "Failed to push transaction %s: %s",
                    record["transaction_id"],
                    e,
                )

    # Update summary
    if all_records:
        try:
            notion_update_summary(NOTION_SUMMARY_DB_ID, all_records)
        except requests.RequestException as e:
            logger.error("Failed to update summary: %s", e)

    # Print summary
    logger.info("--- Sync Complete ---")
    for artist, count in counts.items():
        logger.info("Synced %d new transactions for %s", count, artist)

    total = sum(counts.values())
    logger.info("Total new transactions synced: %d", total)

    # Save last sync timestamp
    Path(LAST_SYNC_FILE).write_text(
        datetime.now(timezone.utc).isoformat(), encoding="utf-8"
    )
    logger.info("Last sync timestamp saved to %s", LAST_SYNC_FILE)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Sync Zettle transactions to Notion"
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date (YYYY-MM-DD)",
    )
    args = parser.parse_args()

    # Validate dates
    for label, val in [("start-date", args.start_date), ("end-date", args.end_date)]:
        try:
            datetime.strptime(val, "%Y-%m-%d")
        except ValueError:
            logger.error("Invalid %s format: %s (expected YYYY-MM-DD)", label, val)
            sys.exit(1)

    logger.info(
        "Starting Zettle sync: %s → %s", args.start_date, args.end_date
    )
    sync(args.start_date, args.end_date)


if __name__ == "__main__":
    main()
