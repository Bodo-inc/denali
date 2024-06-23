import subprocess
import re

import pytest
from pyiceberg.catalog.rest import RestCatalog


@pytest.fixture(scope="function")
def catalog(tmp_path):
    process = subprocess.Popen(
        ["./denali", "start"],
        env={
            "DENALI_API_PORT": "0",
            "DENALI_WAREHOUSE_PATH": str(tmp_path),
            "DENALI_DATABASE_URL": ":memory:",
            "DENALI_DATABASE_DIALECT": "sqlite3",
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    last_line = []
    while len(last_line) == 0 or "Started the Denali Catalog Server at" not in last_line[-1]:
        if process.stdout is None:
            continue
        process.stdout.readlines
        last_line.append(process.stdout.readline().decode("utf-8"))

    url = re.search(r"Started the Denali Catalog Server at `(?P<url>[\[\]\:\.\d]+)`", last_line[-1]).group("url")
    yield RestCatalog("rest_catalog", uri=f"http://{url}")
    process.kill()
