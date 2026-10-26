
import requests

API_URL = "https://data.cambridgema.gov/resource/3gki-wyrb.json"
API_LIMIT = 5000
TIMEOUT = 30


def fetch_ids(params, label):
    params = {
        **params,
        "$select": "id",
        "$limit": API_LIMIT,
    }

    response = requests.get(
        API_URL,
        params=params,
        timeout=TIMEOUT,
    )
    response.raise_for_status()

    records = response.json()
    ids = {
        str(record["id"])
        for record in records
        if record.get("id") is not None
    }

    print(f"{label}: {len(ids)} unique IDs")
    return ids


def main():
    print("Checking extraction coverage...\n")

    incident_ids = fetch_ids(
        {
            "$where": "date_time IS NOT NULL",
            "$order": "date_time DESC",
        },
        "Incident-date batch",
    )

    update_ids = fetch_ids(
        {
            "$where": "last_updated IS NOT NULL",
            "$order": "last_updated DESC",
        },
        "Last-update batch",
    )

    combined_ids = incident_ids | update_ids

    print(f"\nCombined unique IDs: {len(combined_ids)}")
    print(f"Live API total: 12415")

    missing = 12415 - len(combined_ids)

    if missing == 0:
        print("Result: Both batches cover all current API records.")
    else:
        print(f"Result: The batches may miss {missing} API records.")
        print("Note: This estimate assumes the API total is still 12415.")


if __name__ == "__main__":
    main()