# Zettle → Notion Transaction Sync

Syncs transactions from two Zettle accounts into Notion databases with VAT/fee calculations and artist tagging.

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure credentials

```bash
cp .env.example .env
```

Edit `.env` with your actual credentials:

- **Zettle**: Create an API app at [Zettle Developer Portal](https://developer.zettle.com/) for each account. You need the `client_id` and a JWT assertion (`client_secret`).
- **Notion**: Create an integration at [Notion Developers](https://www.notion.so/my-integrations), share your databases with it, and copy the database IDs.

### 3. Notion database schema

#### Zettle Transactions database

| Property         | Type      |
|------------------|-----------|
| Product          | Title     |
| Transaction ID   | Rich Text |
| Date             | Date      |
| Artist           | Select    |
| Product Group    | Select    |
| Quantity         | Number    |
| Amount Gross     | Number    |
| VAT Rate         | Number    |
| VAT Amount       | Number    |
| Amount Net       | Number    |
| Zettle Fee       | Number    |
| Amount After Fees| Number    |

#### Zettle Summary database (optional)

| Property         | Type      |
|------------------|-----------|
| Product Group    | Title     |
| Artist           | Select    |
| Total Quantity   | Number    |
| Total Gross      | Number    |
| Total Net        | Number    |
| Total Fees       | Number    |
| Total After Fees | Number    |

## Usage

```bash
python zettle_sync.py --start-date 2026-03-01 --end-date 2026-03-31
```

### What it does

1. Authenticates with both Zettle accounts via OAuth2
2. Fetches all purchases within the date range
3. Enriches each product line with:
   - **VAT calculation**: 21% standard, 9% for food/drinks/books
   - **Fee distribution**: Zettle fee split proportionally across product lines
   - **Artist tag**: Based on which account the transaction came from
   - **Product group**: Extracted from the first word of the product name
4. Checks Notion for existing Transaction IDs (duplicate prevention)
5. Pushes new transactions to the Transactions database
6. Updates the Summary database with aggregated totals

### Idempotency

Safe to run multiple times — transactions are deduplicated by their unique Transaction ID. Running 3x/week will never create duplicate entries.

### Logs

- Console: summary info
- File: detailed debug log in `zettle_sync.log`
- Last sync timestamp saved in `.last_sync`

## VAT Rules (NL)

| Rate | Products                    |
|------|-----------------------------|
| 21%  | Default (merch, vinyl, etc) |
| 9%   | Food, drinks, books         |

To customize which product groups use 9% VAT, edit the `REDUCED_VAT_PRODUCTS` set in `zettle_sync.py`.
