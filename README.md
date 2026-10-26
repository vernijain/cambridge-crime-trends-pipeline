# Cambridge Crime Trends ETL Pipeline

## Overview

This project builds an ETL (Extract, Transform, Load) pipeline to collect Cambridge crime data from an API and store it in a PostgreSQL database for analysis.

The project demonstrates data extraction, database loading, pipeline orchestration, and data validation.

## Technologies Used

- Python
- PostgreSQL
- Prefect
- Docker Compose
- Metabase
- REST API
- SQL

## Project Architecture

The pipeline follows this general workflow:

1. **Extract:** Retrieve crime data from the API.
2. **Transform:** Prepare the extracted records for database storage.
3. **Load:** Store the records in PostgreSQL.
4. **Orchestrate:** Use Prefect to manage and schedule pipeline execution.
5. **Validate:** Compare source data with database records.
6. **Visualize:** Use Metabase to explore and visualize the stored data.

## Project Files

| File | Purpose |
|---|---|
| `etl.py` | Extracts data from the API and loads it into the database. |
| `prefect_flow.py` | Defines the Prefect pipeline and orchestration. |
| `docker-compose.yml` | Configures the containerized services. |
| `check.py` | Contains project checking or inspection logic. |
| `compare.py` | Compares source data with database data. |
| `validate.py` | Validates data consistency. |
| `test.py` | Contains project tests. |
| `.gitignore` | Excludes local and unnecessary files from Git. |

## Prerequisites

Before running the project, install or configure:

- Python
- Docker Desktop
- PostgreSQL, if running it outside Docker
- Prefect and the Python packages required by the project

## Configuration

Create a local `.env` file in the project directory.

Add your database password:

```env
POSTGRES_PASSWORD=your_local_database_password
```

Use the database password configured for your environment.

**Security:** Never commit `.env` or publish database passwords, API keys, or other secrets to GitHub.

## Running the Project

1. Clone this repository.
2. Create and configure your local `.env` file.
3. Start the required services using Docker Compose.
4. Install the Python dependencies required by the project.
5. Run the ETL script.
6. Run the validation scripts to check the loaded data.
7. Configure and run the Prefect flow as needed.
8. Connect Metabase to PostgreSQL to explore the data.

The exact commands and dependencies should be documented after confirming the project's configuration.

## Data Validation

The project includes scripts for comparing source records with database records and checking data consistency.

Validation helps identify missing records and field-level differences between the source and destination.

## Dashboard

Metabase can be used to build dashboards and visualize the data stored in PostgreSQL.

_Add a screenshot of your dashboard here once it is ready._

## Future Improvements

- Add automated testing.
- Improve logging and error handling.
- Add monitoring for pipeline failures.
- Document database schemas and data transformations.
- Add dashboard screenshots and example insights.

## Author

Verni Jain

## License

No license has been specified yet.
