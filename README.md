# Zettle → Notion Daily Sync

Dagelijkse sync van Zettle transacties (TimZingt & Matthijn) naar Notion.

## Wat het doet

1. Haalt purchases op van beide Zettle accounts
2. Haalt Zettle fees op via de Finance API
3. Zoekt product info op in de Notion product catalog (artist, product group, display name)
4. Pusht individuele transactie-regels naar **Zettle Transactions** database
5. Aggregeert per dag per artist en pusht naar **Zettle Dagomzet** database

## Dagelijkse cron

Draait automatisch elke dag om 06:00 UTC via GitHub Actions. Synct de verkopen van gisteren.

Handmatig starten: Actions → Daily Zettle → Notion Sync → Run workflow (optioneel met start/end date).

## GitHub Secrets

Stel de volgende secrets in via Settings → Secrets → Actions:

| Secret | Beschrijving |
|--------|-------------|
| `MERU_PAYPAL_TIMZINGT` | Zettle API key TimZingt |
| `MERU_PAYPAL_MATTHIJN` | Zettle API key Matthijn |
| `NOTION_API_KEY` | Notion integration token |
| `NOTION_TRANSACTIONS_DB_ID` | `600c35eb-040d-45bf-a0ff-a2bfdf7c5057` |
| `NOTION_DAGOMZET_DB_ID` | `322752dc-7cfe-8135-8ac2-c4d34cd7c2bd` |
| `NOTION_PRODUCTS_DB_ID` | `d564ed56-dd17-4ad8-a5c6-876122983299` |

## Lokaal draaien

```bash
cp .env.example .env
# Vul .env in met je credentials
pip install -r requirements.txt
python zettle_sync.py                                    # gisteren
python zettle_sync.py --start-date 2025-01-01 --end-date 2025-12-31  # custom range
```
