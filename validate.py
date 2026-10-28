import requests
import psycopg
import os
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://data.cambridgema.gov/resource/3gki-wyrb.json"

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "crime_db",
    "user": "crime_user",
    "password": os.getenv("DB_PASSWORD", os.getenv("POSTGRES_PASSWORD")),
}


def fetch_api_records():
    records = {}
    offset = 0
    limit = 5000

    print("Fetching API records...")

    while True:
        response = requests.get(
            API_URL,
            params={
                "$limit": limit,
                "$offset": offset,
                "$order": "id ASC",
        },
            timeout=30,
        )
        response.raise_for_status()
        batch = response.json()

        for record in batch:
            incident_id = record.get("id")
            if incident_id is not None:
                records[str(incident_id)] = record

        print(f"Fetched {len(batch)} records at offset {offset}")

        if len(batch) < limit:
            break

        offset += len(batch)

    return records


def fetch_database_records():
    records = {}

    print("\nFetching database records...")

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT incident_number, incident_date, offense,
                       location, description, last_updated
                FROM crime_records
                WHERE source = 'Cambridge Police Department'
            """)

            for row in cur.fetchall():
                records[str(row[0])] = {
                    "incident_date": row[1],
                    "offense": row[2],
                    "location": row[3],
                    "description": row[4],
                    "last_updated": row[5],
                }

    return records


def normalize(value):
    if value is None:
        return None

    if hasattr(value, "isoformat"):
        return value.isoformat(timespec="milliseconds")

    value = str(value).strip()

    # Normalize API timestamps such as:
    # 2025-04-20T17:43:00.000
    if "T" in value:
        try:
            from datetime import datetime

            parsed = datetime.fromisoformat(value)
            return parsed.isoformat(timespec="milliseconds")
        except ValueError:
            pass

    return value


def main():
    api_records = fetch_api_records()
    db_records = fetch_database_records()

    print("\nComparing API and database values...")

    common_ids = set(api_records) & set(db_records)
    mismatches = []

    for incident_id in common_ids:
        api = api_records[incident_id]
        db = db_records[incident_id]

        comparisons = {
            "incident_date": (api.get("date_time"), db["incident_date"]),
            "offense": (
                api.get("subtype") or api.get("type"),
                db["offense"],
            ),
            "location": (api.get("location"), db["location"]),
            "description": (api.get("description"), db["description"]),
            "last_updated": (api.get("last_updated"), db["last_updated"]),
        }

        for field, (api_value, db_value) in comparisons.items():
            if normalize(api_value) != normalize(db_value):
                mismatches.append((incident_id, field, api_value, db_value))

    print(f"API records: {len(api_records)}")
    print(f"Database records: {len(db_records)}")
    print(f"Common incident IDs: {len(common_ids)}")
    print(f"Field mismatches found: {len(mismatches)}")

    for incident_id, field, api_value, db_value in mismatches[:20]:
        print(f"\nID: {incident_id}")
        print(f"Field: {field}")
        print(f"API value: {api_value!r}")
        print(f"Database value: {db_value!r}")

    if len(mismatches) > 20:
        print(f"\nShowing first 20 of {len(mismatches)} mismatches.")

    print("\nValidation complete. Database was not modified.")


if __name__ == "__main__":
    main()