import subprocess

import pytest
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import ForbiddenError


@pytest.fixture(scope="session")
def catalog():
    process = subprocess.Popen(
        ["./denali", "start"],
        env={
            "DENALI_API_PORT": "5151",
            "DENALI_WAREHOUSE_PATH": "/tmp/iceberg",
            "DENALI_DATABASE_URL": ":memory:",
            "DENALI_DATABASE_TYPE": "sqlite3",
        }
    )

    breakpoint()
    yield RestCatalog("rest_catalog", uri="http://localhost:5151")
    process.kill()
