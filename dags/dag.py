import json
import os
from pathlib import Path

import requests
import snowflake.connector
from airflow import DAG
import datetime
from airflow.decorators import task
from dotenv import dotenv_values
from datetime import date

config = dotenv_values(".env")
API_KEY = config['API_KEY']
base_url = 'https://api.hubapi.com'
headers = {
    'authorization': f'Bearer {API_KEY}'
}

conn = snowflake.connector.connect(
    account=config['account'],
    user=config['user'],
    password=config['password'],
    role='ACCOUNTADMIN',
    warehouse='COMPUTE_WH',
    database='SNOWFLAKE_SAMPLE_DATA',
    schema='TPCDS_SF100TCL'
)


@task()
def export():
    with conn.cursor() as cursor:
        cursor.execute("select * from CALL_CENTER")
        results = cursor.fetchall()
        print(results)
    today_dir = date.today().strftime("%Y-%m-%d")
    path = f"../data/{today_dir}/0.json"
    Path(os.path.dirname(path)).mkdir(
        parents=True, exist_ok=True
    )
    with open(path, "w") as f:
        json.dump(results, f, default=str)


@task()
def get_info():
    response = requests.get(f'{base_url}/crm/v3/objects/contacts', headers=headers)
    print(response.text)
    assert response.status_code < 300, response.status_code


@task()
def batch_import():
    data = {
        "inputs": [
            {
                "properties": {
                    "phone": "5555555555"
                },
                "id": "test@test.com",
                "idProperty": "email"
            },
            {
                "properties": {
                    "phone": "7777777777"
                },
                "id": "example@hubspot.com",
                "idProperty": "email"
            }
        ]
    }
    response = requests.post(f'{base_url}/crm/v3/objects/contacts/batch/upsert', headers=headers, json=data)
    print(response.text)
    assert response.status_code < 300, response.status_code

@task()
def import_api():
    schema = {
      "name": "November Marketing Event Leads",
      "dateFormat": "DAY_MONTH_YEAR",
      "files": [
        {
          "fileName": "importFile.csv", # should match exactly, or else 400
          "fileFormat": "CSV",
          "fileImportPage": {
            "hasHeader": True, # firstname,lastname,email
            "columnMappings": [
              {
                "columnObjectTypeId": "0-1",
                "columnName": "First Name",
                "propertyName": "firstname"
              },
              {
                "columnObjectTypeId": "0-1",
                "columnName": "Last Name",
                "propertyName": "lastname"
              },
              {
                "columnObjectTypeId": "0-1",
                "columnName": "Email",
                "propertyName": "email",
                "associationIdentifierColumn": True
              }        
            ]
          }
        }
      ]
    }
    with open("results/result.json", "rb") as file_stream:
        response = requests.post(f'{base_url}/crm/v3/imports', headers=headers, files=[('files', file_stream)], data={"importRequest": json.jumps(schema)})
        # response returns import id, which we can later track
    print(response.text)
    assert response.status_code < 300, response.status_code


# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="demo", start_date=datetime.datetime(2024, 4, 10), schedule="0 0 * * *") as dag:
    # Set dependencies between tasks
    export() >> batch_import() >> get_info()

