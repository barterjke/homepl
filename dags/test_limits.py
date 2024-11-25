import json
import logging
import os
import pprint
import random
from datetime import date
from json import JSONDecodeError
from pathlib import Path

import requests
from dotenv import dotenv_values
from requests import Response
from time import sleep

from utils import generate_random_name, safe_indexing, DBHandler
from schemas import get_contacts_schema

config = dotenv_values(".env")
API_KEY = config['API_KEY']
base_url = 'https://api.hubapi.com'
headers = {
    'authorization': f'Bearer {API_KEY}'
}

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

logger.addHandler(ch)
logger.addHandler(DBHandler())

state_descriptions = {
    "STARTED": "HubSpot recognizes that the import exists, but the import hasn't started processing yet",
    "PROCESSING": "The import is actively being processed",
    "DONE": "The import is complete. All the objects, activities, or associations have been updated or created",
    "FAILED": "There was an error that was not detected when the import was started. The import was not completed",
    "CANCELED": "User cancelled the export while it was in any of the STARTED, PROCESSING, or DEFERRED states",
    "DEFERRED": "The maximum number of imports (three) are processing at the same time. "
                "The import will start once one of the other imports finishes processing",
}


def generate_error_message(response: Response) -> str:
    """
    possible responses:
    400 {"status":"error","message":"Too many rows detected","correlationId":"359d26ca-e173-4b8b-afd3-be240f1b481f","errors":[{"message":"Too many rows detected","code":"TOO_MANY_ROWS","context":{"maximumAllowedRows":["1048576"],"rowsDetected":["1048577"]}}],"category":"VALIDATION_ERROR"}
    429 {"status":"error","message":"This portal has surpassed the daily number of import rows it can import - 500000","errorType":"FREE_PORTAL_IMPORT_ROWS_LIMIT","errorTokens":{"totalImportRowsLimit":["500000"],"totalImportRows":["1049201"]},"correlationId":"4bc97d9d-ba57-44fa-8df5-66055973f42c"}
    """
    # todo: errors returned in an array. it's possible that if we use different schema, or send multiple files,
    #  0 indexing doesn't work properly
    try:
        result = response.json()
    except JSONDecodeError:
        return f"Unexpected error occured: {response.status_code} {response.text}"
    if response.status_code == 400 or "Too many rows detected" in response.text:
        logger.error(result)
        maximum_allowed_rows = safe_indexing(result, 'errors', 0, "context", 'maximumAllowedRows', 0)
        rows_detected = safe_indexing(result, 'errors', 0, "context", 'rowsDetected', 0)
        return ("Validation error: too many rows for a single request, import is not started."
                f"{maximum_allowed_rows=}, {rows_detected=}")
    if response.status_code == 429 or "This portal has surpassed the daily number of import rows it can import" in response.text:
        total_import_rows_limit = safe_indexing(result, "errorTokens", "totalImportRowsLimit", 0)
        total_import_rows = safe_indexing(result, "errorTokens", "totalImportRows", 0)
        message = result.get("message", "Surpassed the daily number of import rows")
        return f"{message}\n{total_import_rows_limit=}, {total_import_rows=}"
    return str(response)


MAX_ROWS_LIMIT = 1_048_576
MAX_MB = 512


def init_import(file_path: str) -> str:
    """
    You can import up to 80,000,000 rows per day (500_000 for free subscription).
    However, individual import files are limited to 1,048,576 rows or 512 MB, whichever is reached first.
    If your request exceeds either the row or size limit, HubSpot will respond with a 429 HTTP error.
    When approaching these limits, it's recommended to split your import into multiple requests.
    """
    # todo: test with mult files
    with open(file_path, "rb") as file_stream:
        response = requests.post(f'{base_url}/crm/v3/imports', headers=headers, files=[('files', file_stream)],
                                 data={"importRequest": json.dumps(get_contacts_schema(file_path))})
        if response.status_code >= 300:
            message = generate_error_message(response)
            logger.error(message)
            exit(1)
        result = response.json()
        pprint.pprint(result)
        state = result.get('state')
        assert state == 'STARTED', f"Expected `STARTED`, got `{state}`"
        import_id = result['id']
        logger.info('import id', import_id)
        return import_id


def check_import_state(import_id: str) -> dict:
    response = requests.get(f'{base_url}/crm/v3/imports/{import_id}', headers=headers)
    if response.status_code >= 300:
        logger.error(response.text)
        exit(1)
    return response.json()


def wait_import(import_id: str, max_wait_time: float = 600, interval: float = 60):
    waiting_time = 0
    while waiting_time < max_wait_time:
        result = check_import_state(import_id)
        state = result.get('state')
        state_description = state_descriptions.get(state, f"Unexpected state: {state}")
        match state:
            case 'DONE':
                print(result['metadata']['CREATED_OBJECTS'], result['metadata']['TOTAL_ROWS'])
                pprint.pprint(result)
                return
            case 'FAILED':
                logger.error(f"{state}: {state_description}")
                exit(1)
            case _:
                logger.info(f"{state}: {state_description}")
                sleep(interval)
                waiting_time += interval
    else:
        logger.error(f'{max_wait_time=} exceeded, while checking import state')
        exit(1)


def generate_file(size: int) -> str:
    content = "firstname,lastname,email\n"
    for i in range(size):
        line = [generate_random_name(random.randint(10, 15)),
                generate_random_name(random.randint(10, 15)),
                f"{generate_random_name(random.randint(10, 15))}@hubspot.com"]
        content += ','.join(line) + '\n'
    today_dir = date.today().strftime("%Y-%m-%d")
    path = f"../data/{today_dir}/0.csv"
    Path(os.path.dirname(path)).mkdir(
        parents=True, exist_ok=True
    )
    with open(path, "w") as file_stream:
        file_stream.write(content)
    return path


def main():
    logger.info('asdada')
    file_path = generate_file(MAX_ROWS_LIMIT - 1)
    import_id = init_import(file_path)
    wait_import(import_id)


if __name__ == "__main__":
    main()
