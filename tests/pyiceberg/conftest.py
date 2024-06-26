import subprocess
import re
import os
from pathlib import Path

import pytest
from pyiceberg.catalog.rest import RestCatalog


base_path = str(Path(os.path.realpath(__file__)).parent.parent.parent)


@pytest.fixture(scope="session")
def build_binary():
    subprocess.run(
        ["go", "build", "."],
        cwd=base_path,
        check=True,
        env={
            "PATH": os.environ.get("PATH", ""),
            "HOME": os.environ.get("HOME", ""),
        },
    )


@pytest.fixture(scope="function")
def catalog(tmp_path, build_binary):
    process = subprocess.Popen(
        ["./denali", "start"],
        cwd=base_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env={
            "DENALI_API_PORT": "0",
            "DENALI_WAREHOUSE_PATH": str(tmp_path),
            "DENALI_DATABASE_URL": ":memory:",
            "DENALI_DATABASE_DIALECT": "sqlite3",
        },
    )

    last_line: list[str] = []
    while len(last_line) == 0 or "Started the Denali Catalog Server at" not in last_line[-1]:
        if process.stdout is None:
            continue
        process.stdout.readlines
        last_line.append(process.stdout.readline().decode("utf-8"))
        if len(last_line) > 15:
            raise EnvironmentError("Failed to start Denali Catalog Server:\n\t" + "\n\t".join("`" + l + "`" for l in last_line))

    url = re.search(r"Started the Denali Catalog Server at `(?P<url>[\[\]\:\.\d]+)`", last_line[-1]).group("url")
    yield RestCatalog("rest_catalog", uri=f"http://{url}")
    process.kill()
