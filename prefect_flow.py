from prefect import flow, task
from prefect.client.schemas.schedules import CronSchedule

from etl import fetch_records, transform_records, load_records


@task(name="Extract Crime Data", log_prints=True, retries=2, retry_delay_seconds=10)
def extract_crime_data():
    records = fetch_records()

    if not records:
        raise ValueError("Extraction returned no records.")

    print(f"Extracted {len(records)} records.")
    return records


@task(name="Transform Crime Data", log_prints=True)
def transform_crime_data(records):
    clean_records = transform_records(records)

    print(f"Valid records: {len(clean_records)}.")
    print(f"Skipped records: {len(records) - len(clean_records)}.")

    if not clean_records:
        raise ValueError("Transformation returned no valid records.")

    return clean_records


@task(name="Load Crime Data", log_prints=True, retries=2, retry_delay_seconds=10)
def load_crime_data(records):
    if not records:
        raise ValueError("Refusing to load an empty record list.")

    load_records(records)
    print(f"Processed {len(records)} records for PostgreSQL.")


@flow(name="Cambridge Crime ETL", log_prints=True)
def crime_pipeline():
    records = extract_crime_data()
    clean_records = transform_crime_data(records)
    load_crime_data(clean_records)


if __name__ == "__main__":
    crime_pipeline.serve(
        name="cambridge-crime-daily",
        schedule=CronSchedule(
            cron="0 9 * * *",
            timezone="Asia/Kolkata",
        ),
        pause_on_shutdown=False,
    )