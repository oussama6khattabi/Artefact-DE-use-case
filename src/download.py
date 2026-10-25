import csv
import io
import os
import time
from pathlib import Path

import requests

URL = "https://data.cityofchicago.org/resource/wrvz-psew.csv"
DATA_DIR = Path("/opt/airflow/data/bronze")
OUTPUT = DATA_DIR / "trips.csv"


def download_trips():
    start = os.getenv("START", "2022-12-15T00:00:00")
    end = os.getenv("END", "2022-12-30T00:00:00")
    limit = int(os.getenv("LIMIT", "1000"))

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    where = (
        f"trip_start_timestamp >= '{start}' "
        f"AND trip_start_timestamp < '{end}'"
    )

    total = 0
    offset = 0

    # Start a fresh extraction on each DAG run.
    with requests.Session() as session:
        with OUTPUT.open("w", newline="", encoding="utf-8") as output:
            writer = None

            while True:
                params = {
                    "$where": where,
                    "$limit": limit,
                    "$offset": offset,
                }

                response = session.get(
                    URL,
                    params=params,
                )
                response.raise_for_status()

                rows = list(
                    csv.DictReader(io.StringIO(response.text))
                )

                if not rows:
                    break

                if writer is None:
                    writer = csv.DictWriter(
                        output,
                        fieldnames=list(rows[0].keys()),
                    )
                    writer.writeheader()

                writer.writerows(rows)
                output.flush()

                count = len(rows)
                total += count
                offset += count

                print(
                    f"Downloaded {count} rows; total={total}"
                )

                if count < limit:
                    break


    if total == 0:
        raise ValueError(
            f"No trips found between {start} and {end}"
        )

    print(f"Bronze CSV created: {OUTPUT}; rows={total}")
