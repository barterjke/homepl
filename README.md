# homepl
[hubspot API](https://developers.hubspot.com/beta-docs/reference/api)

## installation
```sh
AIRFLOW_VERSION=2.10.2
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# Initialize the Airflow database
airflow db init

# Create Airflow user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# start Airflow webserver (in a separate terminal)
airflow webserver --port 8080

# start Airflow scheduler (in another separate terminal)
airflow scheduler
```
