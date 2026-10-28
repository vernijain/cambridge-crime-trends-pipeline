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

def main():
    print("Fetching all police API records...")

    response = requests.get(
        API_URL,
        params={
            "$select": "id,last_updated",
            "$order": "id ASC",
            "$limit": 50000,
        },
        timeout=60,
    )
    response.raise_for_status()
    api_records = response.json()

    api_data = {
        str(row["id"]): row.get("last_updated")
        for row in api_records
        if row.get("id") is not None
    }

    print(f"API records fetched: {len(api_data)}")

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT incident_number, last_updated
                FROM crime_records
                WHERE source = 'Cambridge Police Department'
            """)
            db_data = {
                str(incident_id): updated.isoformat() if updated else None
                for incident_id, updated in cur.fetchall()
            }

    print(f"Database records found: {len(db_data)}")

    missing_in_db = set(api_data) - set(db_data)
    missing_in_api = set(db_data) - set(api_data)

    print(f"API IDs missing in database: {len(missing_in_db)}")
    print(f"Database IDs missing in API: {len(missing_in_api)}")

    if missing_in_db:
        print("Sample IDs missing in database:", list(missing_in_db)[:10])

    if missing_in_api:
        print("Sample IDs missing in API:", list(missing_in_api)[:10])

    print("Comparison complete. Database was not modified.")

if __name__ == "__main__":
    main()