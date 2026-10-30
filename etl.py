import logging
import os
from datetime import datetime, timezone
from urllib.parse import urlsplit

import psycopg
import requests
from dotenv import load_dotenv


# ============================================================
# ENVIRONMENT
# ============================================================

load_dotenv()


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

API_LIMIT = 5000
API_TIMEOUT = 30


# ============================================================
# EXTRACT
# ============================================================

def fetch_records():
    """
    Fetch Cambridge Police API records in batches.

    Records are deduplicated by API incident ID.
    When duplicate IDs exist, prefer the newest source update.
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

            logger.info(
                "Received %s records.",
                len(batch),
            )

            if not batch:
                break

            for record in batch:
                incident_id = record.get("id")

                if (
                    incident_id is None
                    or str(incident_id).strip() == ""
                ):
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
# DATETIME HELPER
# ============================================================

def parse_datetime(value):
    """
    Convert a date/time value into a Python datetime.

    Invalid or missing values return None.
    Timezone-aware timestamps are normalized to UTC and
    converted to naive datetimes for PostgreSQL compatibility.
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

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(
            timezone.utc
        ).replace(tzinfo=None)

    return parsed


# ============================================================
# TRANSFORM
# ============================================================

def transform_records(records):
    """
    Validate and standardize API records for PostgreSQL.

    Records without an ID or valid incident date are skipped.
    Duplicate IDs are resolved using the newest update time.
    """

    unique = {}
    invalid_count = 0

    for record in records:
        incident_id = record.get("id")

        if (
            incident_id is None
            or str(incident_id).strip() == ""
        ):
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
# DATABASE CONNECTION
# ============================================================

def get_database_connection():
    """
    Connect to Render PostgreSQL using DATABASE_URL.

    Do not use DB_HOST as a fallback. DATABASE_URL must contain
    the complete PostgreSQL connection URL.
    """

    database_url = os.getenv(
        "DATABASE_URL",
        "",
    ).strip()

    if not database_url:
        raise RuntimeError(
            "DATABASE_URL is missing or empty. "
            "Check GitHub repository Settings > Secrets and "
            "variables > Actions."
        )

    try:
        parsed = urlsplit(database_url)

        scheme = parsed.scheme.lower()
        hostname = parsed.hostname

    except ValueError:
        raise RuntimeError(
            "DATABASE_URL is malformed. Copy the complete "
            "External Database URL from Render."
        ) from None

    if scheme not in ("postgres", "postgresql"):
        raise RuntimeError(
            "DATABASE_URL must begin with postgres:// "
            "or postgresql://."
        )

    if not hostname:
        raise RuntimeError(
            "DATABASE_URL does not contain a valid hostname."
        )

    hostname_labels = hostname.rstrip(".").split(".")

    if (
        len(hostname) > 253
        or any(len(label) > 63 for label in hostname_labels)
    ):
        raise RuntimeError(
            "The hostname in DATABASE_URL is invalid or too long."
        )

    logger.info(
        "DATABASE_URL validated. Connecting to PostgreSQL with SSL..."
    )

    try:
        connection = psycopg.connect(
            database_url,
            sslmode="require",
            connect_timeout=20,
        )

        logger.info("PostgreSQL connection established.")

        return connection

    except psycopg.Error:
        logger.error(
            "PostgreSQL connection failed. Check the Render "
            "database status, credentials, and DATABASE_URL secret."
        )
        raise


# ============================================================
# CREATE DATABASE TABLE
# ============================================================

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS crime_records (
    incident_number TEXT PRIMARY KEY,
    incident_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    offense TEXT,
    location TEXT,
    city TEXT,
    source TEXT,
    description TEXT,
    last_updated TIMESTAMP WITHOUT TIME ZONE
);
"""


def create_table(conn):
    """
    Create crime_records if it does not already exist.

    The primary key supports ON CONFLICT in the UPSERT statement.
    """

    logger.info("Checking whether crime_records exists...")

    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)

    logger.info("crime_records table is ready.")


# ============================================================
# UPSERT SQL
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
    );
"""


# ============================================================
# LOAD
# ============================================================

def load_records(records):
    """
    Create the destination table and load records.

    Existing records from another source are not overwritten.
    The transaction commits on success and rolls back on error.
    """

    if not records:
        logger.warning(
            "No valid records available to load."
        )
        return

    logger.info(
        "Connecting to PostgreSQL for data loading..."
    )

    with get_database_connection() as conn:

        # Create the table before attempting any INSERT.
        create_table(conn)

        with conn.cursor() as cur:

            logger.info(
                "Loading %s records...",
                len(records),
            )

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

    # Exiting the connection context commits the transaction.
    logger.info(
        "Database transaction completed successfully."
    )

    logger.info(
        "Total Cambridge police records: %s",
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
        # ----------------------------------------------------
        # EXTRACT
        # ----------------------------------------------------

        records = fetch_records()

        if not records:
            raise RuntimeError(
                "The API returned no records. "
                "The database was not changed."
            )

        # ----------------------------------------------------
        # TRANSFORM
        # ----------------------------------------------------

        clean_records = transform_records(records)

        if not clean_records:
            raise RuntimeError(
                "No valid records were available for loading. "
                "The database was not changed."
            )

        # ----------------------------------------------------
        # LOAD
        # ----------------------------------------------------

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