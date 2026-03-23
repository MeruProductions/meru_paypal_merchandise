#!/usr/bin/env python3
"""
Zettle → Notion Daily Sync

Pulls transactions from two Zettle accounts (TimZingt & Matthijn),
enriches with fees from the Finance API and product catalog from Notion,
and pushes to two Notion databases:
  1. Zettle Transactions — one row per product line
  2. Zettle Dagomzet — one row per artist per day (aggregated)

Usage:
    python zettle_sync.py                          # sync yesterday
    python zettle_sync.py --start-date 2025-03-01 --end-date 2025-03-31
"""

import argparse
import base64
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ZETTLE_TOKEN_URL = "https://oauth.zettle.com/token"
ZETTLE_PURCHASES_URL = "https://purchase.izettle.com/purchases/v2"
ZETTLE_FINANCE_URL = "https://finance.izettle.com/v2/accounts/LIQUID/transactions"

NOTION_API_URL = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"

ACCOUNT_DEFS = [
    ("MERU_PAYPAL_TIMZINGT", "TimZingt"),
    ("MERU_PAYPAL_MATTHIJN", "Matthijn"),
]

ACCOUNTS = []
for env_var, artist_name in ACCOUNT_DEFS:
    api_key = os.getenv(env_var)
    if api_key:
        ACCOUNTS.append({"api_key": api_key, "artist_name": artist_name})

NOTION_API_KEY = os.getenv("NOTION_API_KEY", "")
NOTION_TRANSACTIONS_DB_ID = os.getenv("NOTION_TRANSACTIONS_DB_ID", "")
NOTION_DAGOMZET_DB_ID = os.getenv("NOTION_DAGOMZET_DB_ID", "")
NOTION_PRODUCTS_DB_ID = os.getenv("NOTION_PRODUCTS_DB_ID", "")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger("zettle_sync")
logger.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
logger.addHandler(console_handler)


# ---------------------------------------------------------------------------
# Zettle API
# ---------------------------------------------------------------------------


def zettle_get_token(api_key: str) -> str:
    payload = json.loads(base64.urlsafe_b64decode(api_key.split(".")[1] + "=="))
    client_id = payload.get("client_id") or payload.get("sub")

    # Check JWT expiry before attempting token exchange
    exp = payload.get("exp")
    if exp and exp < datetime.now(timezone.utc).timestamp():
        raise RuntimeError(
            f"Zettle API key is verlopen (exp: {datetime.fromtimestamp(exp, tz=timezone.utc).strftime('%Y-%m-%d')}). "
            "Genereer een nieuwe key in de Zettle Developer Portal."
        )

    resp = requests.post(
        ZETTLE_TOKEN_URL,
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "client_id": client_id,
            "assertion": api_key,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    if resp.status_code == 401:
        raise RuntimeError(
            f"Zettle auth mislukt (401). API key is mogelijk verlopen of ingetrokken. "
            f"Response: {resp.text[:200]}"
        )
    resp.raise_for_status()
    return resp.json()["access_token"]


def zettle_fetch_purchases(token: str, start_date: str, end_date: str) -> list[dict]:
    headers = {"Authorization": f"Bearer {token}"}
    all_purchases = []
    # Zettle API treats endDate as exclusive, so add one day to include end_date
    end_date_exclusive = (
        datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%Y-%m-%d")
    params = {"startDate": start_date, "endDate": end_date_exclusive, "limit": "1000"}

    while True:
        resp = requests.get(
            ZETTLE_PURCHASES_URL, headers=headers, params=params, timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
        purchases = data.get("purchases", [])
        if not purchases:
            break
        all_purchases.extend(purchases)
        last_hash = data.get("lastPurchaseHash")
        if not last_hash:
            break
        params["lastPurchaseHash"] = last_hash

    return all_purchases


def zettle_fetch_fees(token: str, start_date: str, end_date: str) -> dict[str, float]:
    """Fetch payment fees from Finance API. Returns {paymentUuid: feeEur}."""
    headers = {"Authorization": f"Bearer {token}"}
    fee_map: dict[str, float] = {}
    offset = 0

    while True:
        params = {
            "start": f"{start_date}T00:00:00.000Z",
            "end": f"{end_date}T23:59:59.999Z",
            "includeTransactionType": "PAYMENT_FEE",
            "limit": "10000",
            "offset": str(offset),
        }
        resp = requests.get(
            ZETTLE_FINANCE_URL, headers=headers, params=params, timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        for tx in data:
            fee_map[tx["originatingTransactionUuid"]] = abs(tx["amount"]) / 100
        offset += len(data)
        if len(data) < 10000:
            break

    return fee_map


# ---------------------------------------------------------------------------
# Notion API
# ---------------------------------------------------------------------------


def notion_headers() -> dict:
    return {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def notion_query_all(database_id: str) -> list[dict]:
    """Fetch all pages from a Notion database."""
    pages = []
    start_cursor = None
    while True:
        body: dict = {"page_size": 100}
        if start_cursor:
            body["start_cursor"] = start_cursor
        resp = requests.post(
            f"{NOTION_API_URL}/databases/{database_id}/query",
            headers=notion_headers(),
            json=body,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        pages.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        start_cursor = data.get("next_cursor")
    return pages


def notion_fetch_product_catalog() -> dict[str, dict]:
    """Fetch Zettle Products catalog. Returns {zettleProductUuid: {artist, productGroup, displayName, pageId}}."""
    if not NOTION_PRODUCTS_DB_ID:
        return {}
    catalog = {}
    for page in notion_query_all(NOTION_PRODUCTS_DB_ID):
        props = page["properties"]
        uuid_text = props.get("Zettle Product UUID", {}).get("rich_text", [])
        uuid = uuid_text[0]["plain_text"] if uuid_text else ""
        if uuid:
            title = props.get("Display Name", {}).get("title", [])
            catalog[uuid] = {
                "artist": props.get("Artist", {}).get("select", {}).get("name", ""),
                "productGroup": props.get("Product Group", {}).get("select", {}).get("name", ""),
                "displayName": title[0]["plain_text"] if title else "",
                "pageId": page["id"],
            }
    return catalog


def notion_query_filtered(database_id: str, date_filter: dict) -> list[dict]:
    """Fetch pages from a Notion database with a filter."""
    pages = []
    start_cursor = None
    while True:
        body: dict = {"page_size": 100, "filter": date_filter}
        if start_cursor:
            body["start_cursor"] = start_cursor
        resp = requests.post(
            f"{NOTION_API_URL}/databases/{database_id}/query",
            headers=notion_headers(),
            json=body,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        pages.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        start_cursor = data.get("next_cursor")
    return pages


def notion_fetch_existing_tx_ids(start_date: str, end_date: str) -> dict[str, str]:
    """Returns {transactionId: pageId} for transactions in the given date range only."""
    date_filter = {
        "and": [
            {"property": "Date", "date": {"on_or_after": start_date}},
            {"property": "Date", "date": {"on_or_before": end_date}},
        ]
    }
    result = {}
    for page in notion_query_filtered(NOTION_TRANSACTIONS_DB_ID, date_filter):
        rt = page["properties"].get("Transaction ID", {}).get("rich_text", [])
        if rt:
            result[rt[0]["plain_text"]] = page["id"]
    return result


def notion_fetch_existing_dagomzet(start_date: str, end_date: str) -> dict[str, str]:
    """Returns {'datum|artist': pageId} for dagomzet rows matching the date range."""
    if not NOTION_DAGOMZET_DB_ID:
        return {}
    # Datum is a title field, so we fetch all and filter in Python (small table)
    result = {}
    for page in notion_query_all(NOTION_DAGOMZET_DB_ID):
        props = page["properties"]
        datum_title = props.get("Datum", {}).get("title", [])
        artist_sel = props.get("Artist", {}).get("select", {})
        datum = datum_title[0]["plain_text"] if datum_title else ""
        artist = artist_sel.get("name", "")
        if datum and artist and start_date <= datum <= end_date:
            result[f"{datum}|{artist}"] = page["id"]
    return result


# ---------------------------------------------------------------------------
# Data Processing
# ---------------------------------------------------------------------------


def process_account(
    api_key: str, artist_name: str, start_date: str, end_date: str, catalog: dict
) -> list[dict]:
    """Fetch and enrich all product lines for one Zettle account."""
    token = zettle_get_token(api_key)
    purchases = zettle_fetch_purchases(token, start_date, end_date)
    fee_map = zettle_fetch_fees(token, start_date, end_date)

    logger.info(
        "  %s: %d purchases, %d fee records", artist_name, len(purchases), len(fee_map)
    )

    lines = []
    for purchase in purchases:
        purchase_id = purchase.get("purchaseUUID1") or purchase.get("purchaseUUID")
        timestamp = purchase.get("timestamp", "")
        date_str = timestamp.split("T")[0] if timestamp else ""

        # Calculate total fee for this purchase from Finance API
        total_fee = 0.0
        for payment in purchase.get("payments", []):
            fee = fee_map.get(payment["uuid"], 0)
            total_fee += fee

        total_gross = abs(purchase.get("amount", 0)) / 100

        for product in purchase.get("products", []):
            product_uuid = product.get("productUuid", "")
            cat = catalog.get(product_uuid, {})
            artist = cat.get("artist") or artist_name
            display_name = cat.get("displayName") or product.get("name", "Unknown")
            product_group = cat.get("productGroup", "")
            page_id = cat.get("pageId", "")

            qty = int(product.get("quantity", 1))
            unit_price = product.get("unitPrice", 0) / 100
            gross = round(unit_price * qty, 2)
            vat_pct = product.get("vatPercentage", 21)
            taxable = product.get("rowTaxableAmount", 0) / 100
            vat_amount = round(gross - taxable, 2)
            discount_val = (product.get("discountValue") or 0) / 100

            # Proportional fee
            prop_fee = round(total_fee * (gross / total_gross), 2) if total_gross else 0
            netto = round(gross - vat_amount - prop_fee - discount_val, 2)

            discount_name = ""
            for d in purchase.get("discounts", []):
                if d.get("name"):
                    discount_name = d["name"]
                    break

            lines.append({
                "transaction_id": purchase_id,
                "date": date_str,
                "artist": artist,
                "product_name": display_name,
                "product_uuid": product_uuid,
                "product_group": product_group,
                "product_ref_page_id": page_id,
                "quantity": qty,
                "gross": gross,
                "vat_rate": vat_pct / 100,
                "vat_amount": vat_amount,
                "net": taxable,
                "fee": prop_fee,
                "after_fees": round(gross - prop_fee, 2),
                "discount_amount": discount_val,
                "discount_name": discount_name,
                "netto_inkomsten": netto,
            })

    return lines


# ---------------------------------------------------------------------------
# Notion Sync
# ---------------------------------------------------------------------------


def sync_transactions(lines: list[dict], existing: dict[str, str]) -> tuple[int, int]:
    """Sync product lines to Zettle Transactions DB. Returns (created, updated)."""
    created = updated = 0

    for line in lines:
        props: dict = {
            "Product": {"title": [{"text": {"content": line["product_name"]}}]},
            "Transaction ID": {"rich_text": [{"text": {"content": line["transaction_id"]}}]},
            "Date": {"date": {"start": line["date"]}} if line["date"] else {"date": None},
            "Artist": {"select": {"name": line["artist"]}},
            "Quantity": {"number": line["quantity"]},
            "Amount Gross": {"number": line["gross"]},
            "Amount Net": {"number": line["net"]},
            "Amount After Fees": {"number": line["after_fees"]},
            "VAT Rate": {"number": line["vat_rate"]},
            "VAT Amount": {"number": line["vat_amount"]},
            "Zettle Fee": {"number": line["fee"]},
            "Discount Amount": {"number": line["discount_amount"]},
            "Discount Name": {"rich_text": [{"text": {"content": line["discount_name"]}}]},
        }
        if line["product_group"]:
            props["Product Group"] = {"select": {"name": line["product_group"]}}
        if line["product_ref_page_id"]:
            props["Product Ref"] = {"relation": [{"id": line["product_ref_page_id"]}]}

        try:
            page_id = existing.get(line["transaction_id"])
            if page_id:
                requests.patch(
                    f"{NOTION_API_URL}/pages/{page_id}",
                    headers=notion_headers(),
                    json={"properties": props},
                    timeout=30,
                ).raise_for_status()
                updated += 1
            else:
                requests.post(
                    f"{NOTION_API_URL}/pages",
                    headers=notion_headers(),
                    json={"parent": {"database_id": NOTION_TRANSACTIONS_DB_ID}, "properties": props},
                    timeout=30,
                ).raise_for_status()
                created += 1
        except requests.RequestException as e:
            logger.error("  Failed %s: %s", line["transaction_id"], e)

    return created, updated


def sync_dagomzet(lines: list[dict], existing: dict[str, str]) -> tuple[int, int]:
    """Aggregate lines per date+artist and sync to Dagomzet DB."""
    if not NOTION_DAGOMZET_DB_ID:
        return 0, 0

    agg: dict[str, dict] = {}
    for line in lines:
        key = f"{line['date']}|{line['artist']}"
        if key not in agg:
            agg[key] = {"bruto": 0, "btw": 0, "fee": 0, "korting": 0, "count": 0}
        a = agg[key]
        a["bruto"] += line["gross"]
        a["btw"] += line["vat_amount"]
        a["fee"] += line["fee"]
        a["korting"] += line["discount_amount"]
        a["count"] += 1

    created = updated = 0
    for key, a in agg.items():
        datum, artist = key.split("|")
        netto = round(a["bruto"] - a["btw"] - a["fee"] - a["korting"], 2)
        props = {
            "Datum": {"title": [{"text": {"content": datum}}]},
            "Artist": {"select": {"name": artist}},
            "Netto Inkomsten": {"number": netto},
            "Bruto": {"number": round(a["bruto"], 2)},
            "BTW": {"number": round(a["btw"], 2)},
            "Zettle Fee": {"number": round(a["fee"], 2)},
            "Korting": {"number": round(a["korting"], 2)},
            "Transacties": {"number": a["count"]},
        }

        try:
            page_id = existing.get(key)
            if page_id:
                requests.patch(
                    f"{NOTION_API_URL}/pages/{page_id}",
                    headers=notion_headers(),
                    json={"properties": props},
                    timeout=30,
                ).raise_for_status()
                updated += 1
            else:
                requests.post(
                    f"{NOTION_API_URL}/pages",
                    headers=notion_headers(),
                    json={"parent": {"database_id": NOTION_DAGOMZET_DB_ID}, "properties": props},
                    timeout=30,
                ).raise_for_status()
                created += 1
        except requests.RequestException as e:
            logger.error("  Failed dagomzet %s: %s", key, e)

    return created, updated


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def sync(start_date: str, end_date: str) -> None:
    if not ACCOUNTS:
        logger.error("No Zettle accounts configured. Set MERU_PAYPAL_* env vars.")
        sys.exit(1)
    if not NOTION_API_KEY or not NOTION_TRANSACTIONS_DB_ID:
        logger.error("Missing NOTION_API_KEY or NOTION_TRANSACTIONS_DB_ID.")
        sys.exit(1)

    logger.info("Fetching Notion product catalog...")
    catalog = notion_fetch_product_catalog()
    logger.info("Product catalog: %d products", len(catalog))

    logger.info("Fetching existing Notion data for %s → %s...", start_date, end_date)
    existing_tx = notion_fetch_existing_tx_ids(start_date, end_date)
    existing_dag = notion_fetch_existing_dagomzet(start_date, end_date)
    logger.info("Existing in range: %d transactions, %d dagomzet rows", len(existing_tx), len(existing_dag))

    logger.info("Fetching Zettle data (%s → %s)...", start_date, end_date)
    all_lines: list[dict] = []
    account_errors: list[str] = []
    for account in ACCOUNTS:
        try:
            lines = process_account(
                account["api_key"], account["artist_name"], start_date, end_date, catalog
            )
            all_lines.extend(lines)
        except Exception as e:
            msg = f"{account['artist_name']}: {e}"
            logger.error("FAILED fetching %s", msg)
            account_errors.append(msg)

    if account_errors and not all_lines:
        logger.error("All Zettle accounts failed, no data fetched:")
        for err in account_errors:
            logger.error("  - %s", err)
        sys.exit(1)
    elif account_errors:
        logger.warning("Some accounts failed but continuing with partial data:")
        for err in account_errors:
            logger.warning("  - %s", err)

    logger.info("Total product lines: %d", len(all_lines))

    if not all_lines:
        logger.info("No transactions found, nothing to sync.")
        return

    logger.info("Syncing transactions to Notion...")
    tx_created, tx_updated = sync_transactions(all_lines, existing_tx)
    logger.info("Transactions: %d created, %d updated", tx_created, tx_updated)

    logger.info("Syncing dagomzet to Notion...")
    dag_created, dag_updated = sync_dagomzet(all_lines, existing_dag)
    logger.info("Dagomzet: %d created, %d updated", dag_created, dag_updated)

    logger.info("Done!")


def main():
    parser = argparse.ArgumentParser(description="Sync Zettle transactions to Notion")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD), default: yesterday")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD), default: yesterday")
    args = parser.parse_args()

    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = args.start_date or yesterday
    end_date = args.end_date or yesterday

    logger.info("Zettle → Notion sync: %s → %s", start_date, end_date)
    sync(start_date, end_date)


if __name__ == "__main__":
    main()
