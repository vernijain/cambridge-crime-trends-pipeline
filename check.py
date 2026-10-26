import requests

url = "https://data.cambridgema.gov/resource/3gki-wyrb.json"

try:
    response = requests.get(
        url,
        params={"$select": "count(*)"},
        timeout=60
    )
    response.raise_for_status()

    result = response.json()
    print("API status:", response.status_code)
    print("Total records in live API:", result)

except requests.RequestException as error:
    print("API request failed:", error)