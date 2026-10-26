import logging
import os
from datetime import datetime

import psycopg
import requests


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================
# CONFIGURATION
# ============================================================

API_URL = "https://data.cambridgema.gov/resource/3gki-wyrb.json"

# Maximum number of records requested per API query.
API_LIMIT = 5000

API_TIMEOUT = 30

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5433")),
    "dbname": os.getenv("DB_NAME", "crime_db"),
    "user": os.getenv("DB_USER", "crime_user"),
    "password": os.getenv("DB_PASSWORD", os.getenv("POSTGRES_PASSWORD")),
}


# ============================================================
# EXTRACT
# ============================================================

def fetch_records():
    """
    Fetch all Cambridge Police API records in batches using pagination.
    Merge and deduplicate records by API incident ID.
    """

    combined = {}

    params = {
        "$limit": API_LIMIT,
        "$order": "id ASC",
    }

    offset = 0

    with requests.Session() as session:
        while True:
            page_params = {
                **params,
                "$offset": offset,
            }

            logger.info(
                "Fetching records starting at offset %s...",
                offset,
            )

            response = session.get(
                API_URL,
                params=page_params,
                timeout=API_TIMEOUT,
            )
            response.raise_for_status()

            batch = response.json()

            if not isinstance(batch, list):
                raise ValueError(
                    "Unexpected API response: expected a list."
                )

            logger.info("Received %s records.", len(batch))

            if not batch:
                break

            for record in batch:
                incident_id = record.get("id")

                if incident_id is None or str(incident_id).strip() == "":
                    logger.warning(
                        "Skipping API record without an incident ID."
                    )
                    continue

                incident_id = str(incident_id)
                existing = combined.get(incident_id)

                if existing is None:
                    combined[incident_id] = record
                else:
                    old_updated = parse_datetime(
                        existing.get("last_updated")
                    )
                    new_updated = parse_datetime(
                        record.get("last_updated")
                    )

                    if (
                        new_updated is not None
                        and (
                            old_updated is None
                            or new_updated >= old_updated
                        )
                    ):
                        combined[incident_id] = record

            offset += len(batch)

            if len(batch) < API_LIMIT:
                break

    records = list(combined.values())

    logger.info(
        "Unique records extracted: %s",
        len(records),
    )

    return records


# ============================================================
# TRANSFORM
# ============================================================

def parse_datetime(value):
    """
    Convert a date/time value to a Python datetime.

    Returns None for missing or invalid values.
    """

    if value is None or value == "":
        return None

    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(
                str(value).replace("Z", "+00:00")
            )
        except (ValueError, TypeError):
            logger.warning(
                "Invalid datetime value: %r",
                value,
            )
            return None

    # PostgreSQL timestamp columns commonly use timestamps
    # without time zone in this project. Normalize aware
    # values to UTC before removing the timezone information.
    if parsed.tzinfo is not None:
        from datetime import timezone

        parsed = parsed.astimezone(timezone.utc).replace(
            tzinfo=None
        )

    return parsed


def transform_records(records):
    """
    Validate and standardize API records for PostgreSQL.

    Records without an ID or valid incident date are skipped.
    """

    unique = {}
    invalid_count = 0

    for record in records:

        incident_id = record.get("id")

        if incident_id is None or str(incident_id).strip() == "":
            invalid_count += 1
            continue

        incident_date = parse_datetime(
            record.get("date_time")
        )

        last_updated = parse_datetime(
            record.get("last_updated")
        )

        if incident_date is None:
            invalid_count += 1
            continue

        incident_id = str(incident_id)

        transformed = {
            "incident_number": incident_id,
            "incident_date": incident_date,
            "offense": (
                record.get("subtype")
                or record.get("type")
            ),
            "location": record.get("location"),
            "city": "Cambridge",
            "source": "Cambridge Police Department",
            "description": record.get("description"),
            "last_updated": last_updated,
        }

        # Protect against duplicate IDs within the extracted data.
        previous = unique.get(incident_id)

        if previous is None:
            unique[incident_id] = transformed
        else:
            previous_updated = previous["last_updated"]

            if (
                last_updated is not None
                and (
                    previous_updated is None
                    or last_updated >= previous_updated
                )
            ):
                unique[incident_id] = transformed

    clean_records = list(unique.values())

    logger.info(
        "Valid unique records: %s",
        len(clean_records),
    )

    logger.info(
        "Invalid records skipped: %s",
        invalid_count,
    )

    return clean_records


# ============================================================
# LOAD
# ============================================================

UPSERT_SQL = """
INSERT INTO crime_records (
    incident_number,
    incident_date,
    offense,
    location,
    city,
    source,
    description,
    last_updated
)
VALUES (
    %(incident_number)s,
    %(incident_date)s,
    %(offense)s,
    %(location)s,
    %(city)s,
    %(source)s,
    %(description)s,
    %(last_updated)s
)
ON CONFLICT (incident_number)
DO UPDATE SET
    incident_date = EXCLUDED.incident_date,

    offense = COALESCE(
        EXCLUDED.offense,
        crime_records.offense
    ),

    location = COALESCE(
        EXCLUDED.location,
        crime_records.location
    ),

    city = EXCLUDED.city,

    source = EXCLUDED.source,

    description = COALESCE(
        EXCLUDED.description,
        crime_records.description
    ),

    last_updated = GREATEST(
        crime_records.last_updated,
        EXCLUDED.last_updated
    )

WHERE
    crime_records.source = EXCLUDED.source
    AND (
        EXCLUDED.last_updated IS NOT NULL
        AND (
            crime_records.last_updated IS NULL
            OR EXCLUDED.last_updated >= crime_records.last_updated
        )
    )
"""


def load_records(records):
    """
    Insert new records and update eligible existing records.

    Existing records from another source are not overwritten.
    The function never drops or truncates the table.
    """

    if not records:
        logger.warning(
            "No valid records available to load."
        )
        return

    logger.info(
        "Connecting to PostgreSQL..."
    )

    with psycopg.connect(**DB_CONFIG) as conn:

        with conn.cursor() as cur:

            cur.executemany(
                UPSERT_SQL,
                records,
            )

            cur.execute(
                """
                SELECT
                    COUNT(*) AS total_records,
                    COUNT(last_updated) AS records_with_update_time,
                    MAX(last_updated) AS latest_source_update,
                    MAX(incident_date) AS latest_incident_date
                FROM crime_records
                WHERE source = %s
                """,
                ("Cambridge Police Department",),
            )

            result = cur.fetchone()

    logger.info(
        "Database transaction completed successfully."
    )

    logger.info(
        "Total police records: %s",
        result[0],
    )

    logger.info(
        "Records with update timestamps: %s",
        result[1],
    )

    logger.info(
        "Latest source update stored: %s",
        result[2],
    )

    logger.info(
        "Latest incident date stored: %s",
        result[3],
    )


# ============================================================
# MAIN ETL PIPELINE
# ============================================================

def main():

    logger.info("=" * 60)
    logger.info("CAMBRIDGE CRIME ETL PIPELINE STARTED")
    logger.info("=" * 60)

    try:
        # EXTRACT
        records = fetch_records()

        if not records:
            raise RuntimeError(
                "The API returned no records. "
                "The database was not changed."
            )

        # TRANSFORM
        clean_records = transform_records(records)

        if not clean_records:
            raise RuntimeError(
                "No valid records were available for loading. "
                "The database was not changed."
            )

        # LOAD
        load_records(clean_records)

        logger.info("=" * 60)
        logger.info("CAMBRIDGE CRIME ETL PIPELINE COMPLETED")
        logger.info("=" * 60)

    except requests.RequestException:
        logger.exception(
            "API request failed. The ETL pipeline stopped."
        )
        raise

    except psycopg.Error:
        logger.exception(
            "Database operation failed. The ETL pipeline stopped."
        )
        raise

    except Exception:
        logger.exception(
            "ETL pipeline failed."
        )
        raise


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()