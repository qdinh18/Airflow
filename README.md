# Airflow Project

## Description

**Airflow Project** is my first project using [Apache Airflow](https://airflow.apache.org/) to automate the process of web scraping, data processing, and storing book data from Amazon into a PostgreSQL database. This repository includes two main files:

- `docker-compose.yaml`: Docker Compose configuration to set up the necessary services, including Airflow and PostgreSQL.
- `dag/dag.py`: The Airflow DAG (Directed Acyclic Graph) that defines the workflow for scraping, processing, and storing book data.

## Features

- **Web Scraping**: Collects information about books from Amazon, including title, author, rating, price, and availability.
- **Data Processing**: Cleans and transforms the scraped data to ensure consistency and quality.
- **Data Storage**: Inserts the processed data into a PostgreSQL database for easy querying and analysis.

## Requirements

- **Docker**: Ensure Docker is installed on your system. [Install Docker](https://docs.docker.com/get-docker/)
- **Docker Compose**: Install Docker Compose to manage multi-container Docker applications. [Install Docker Compose](https://docs.docker.com/compose/install/)

## Installation

### 1. Clone the Repository

```bash
git clone <repo_url>
cd airflow
```

### 2. Configure Docker Compose

Ensure that the `docker-compose.yaml` file is properly configured for your environment. Key configurations include ports and database connection details.

### 3. Start Docker Compose

```bash
docker-compose up -d
```

This command will start the Airflow and PostgreSQL services in detached mode.

### 4. Set Up PostgreSQL Connection in Airflow

1. Access the Airflow web interface at `http://localhost:8080`.
2. Log in with the default credentials (if not changed).
3. Navigate to **Admin > Connections**.
4. Add a new connection with the following details:
   - **Conn Id**: `books_connection`
   - **Conn Type**: `Postgres`
   - **Host**: `postgres` (service name defined in `docker-compose.yaml`)
   - **Schema**: `postgres`
   - **Login**: `postgres`
   - **Password**: `postgres` (or your configured password)
   - **Port**: `5432`

## Usage

### 1. Add the DAG to Airflow

Ensure that the `dag.py` file is placed inside the `dag/` directory. By default, Docker Compose mounts this directory to `/opt/airflow/dags` inside the Airflow container.

### 2. Activate the DAG

1. Go to the Airflow web interface at `http://localhost:8080`.
2. Locate the DAG named `fetch_and_store_books_data`.
3. Toggle the DAG from `Off` to `On` to activate it.
4. The DAG is scheduled to run daily based on the defined schedule interval.

### 3. Monitor the DAG

- Use the Airflow web interface to monitor the status of each task within the DAG.
- View logs and check for any issues during execution.

## Project Structure

```
airflow/
├── docker-compose.yaml
└── dag/
    └── dag.py
```

- **docker-compose.yaml**: Configures Docker Compose to run Airflow and PostgreSQL services.
- **dag/dag.py**: Defines the Airflow DAG responsible for scraping and storing book data from Amazon.

## DAG Details

The `fetch_and_store_books_data` DAG includes the following tasks:

1. **fetch_and_transform_data**: Scrapes book data from Amazon, processes it, and transforms it into a structured format.
2. **create_table**: Creates the `books` table in PostgreSQL if it doesn't already exist.
3. **insert_data_into_post**: Inserts the processed book data into the `books` table in PostgreSQL.

Task dependencies are set as follows: `fetch_and_transform_data` → `create_table` → `insert_data_into_post`.

## Contributing

As this is my first Airflow project, I welcome any feedback, suggestions, or contributions. Feel free to open an issue or submit a pull request.

## License

This project is licensed under the [MIT License](LICENSE).

## Contact

If you have any questions or need further assistance, please open an issue on GitHub or contact me directly.

---

*Thank you for checking out my first Airflow project!*
