import requests
import os
import json
import base64
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat_ws, monotonically_increasing_id
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
server_hostname = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/SiMinL_Week11"
headers = {"Authorization": f"Bearer {access_token}"}
url = f"https://{server_hostname}/api/2.0"


def perform_query(path, headers, data={}):
    session = requests.Session()
    resp = session.request(
        "POST", url + path, data=json.dumps(data), verify=True, headers=headers
    )
    return resp.json()


def mkdirs(path, headers):
    _data = {"path": path}
    return perform_query("/dbfs/mkdirs", headers=headers, data=_data)


def create(path, overwrite, headers):
    _data = {"path": path, "overwrite": overwrite}
    return perform_query("/dbfs/create", headers=headers, data=_data)


def add_block(handle, data, headers):
    _data = {"handle": handle, "data": data}
    return perform_query("/dbfs/add-block", headers=headers, data=_data)


def close(handle, headers):
    _data = {"handle": handle}
    return perform_query("/dbfs/close", headers=headers, data=_data)


def put_file_from_url(url, dbfs_path, overwrite, headers):
    response = requests.get(url)
    if response.status_code == 200:
        content = response.content
        create_response = create(dbfs_path, overwrite, headers=headers)
        if "handle" in create_response:
            handle = create_response["handle"]
            print("Putting file: " + dbfs_path)
            for i in range(0, len(content), 2**20):
                add_block(
                    handle,
                    base64.standard_b64encode(content[i : i + 2**20]).decode(),
                    headers=headers,
                )
            close(handle, headers=headers)
            print(f"File {dbfs_path} uploaded successfully.")
        else:
            print(f"Error creating file on DBFS. Response: {create_response}")
    else:
        print(f"Error downloading file from {url}. Status code: {response.status_code}")


def extract(
    url="https://gist.githubusercontent.com/armgilles/194bcff35001e7eb53a2a8b441e8b2c6/raw/92200bc0a673d5ce2110aaad4544ed6c4010f687/pokemon.csv",
    file_path=FILESTORE_PATH + "/pokemon.csv",
    directory=FILESTORE_PATH,
    overwrite=True,
):
    """Extracts a URL to a file path on DBFS."""
    # Make the directory
    mkdirs(path=directory, headers=headers)
    # Upload the CSV file to DBFS
    put_file_from_url(url, file_path, overwrite, headers=headers)
    return file_path


if __name__ == "__main__":
  if not server_hostname:
        raise ValueError("SERVER_HOSTNAME environment variable is not set.")
  extract()
