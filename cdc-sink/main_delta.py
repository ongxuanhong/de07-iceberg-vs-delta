import json
import os
import time
from typing import Any

import pyarrow as pa
from deltalake import write_deltalake
from dotenv import load_dotenv
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBackpressureError, SinkBatch

load_dotenv()


class MyDeltaSink(BatchingSink):
    """Write consumed Kafka records into a Delta table on object storage."""

    def __init__(self) -> None:
        super().__init__()
        self.table_uri = os.getenv(
            "DELTA_TABLE_URI", "s3://bronze/delta/inventory/customers"
        )
        self.storage_options = self._create_storage_options()

    @staticmethod
    def _create_storage_options() -> dict[str, str]:
        options: dict[str, str] = {}

        access_key = os.getenv("AWS_ACCESS_KEY_ID")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        region = os.getenv("AWS_REGION")
        endpoint = os.getenv("DELTA_S3_ENDPOINT") or os.getenv("ICEBERG_S3_ENDPOINT")
        allow_http = os.getenv("DELTA_AWS_ALLOW_HTTP", "true")
        force_path_style = os.getenv("DELTA_S3_FORCE_PATH_STYLE", "true")

        if access_key:
            options["AWS_ACCESS_KEY_ID"] = access_key
        if secret_key:
            options["AWS_SECRET_ACCESS_KEY"] = secret_key
        if region:
            options["AWS_REGION"] = region
        if endpoint:
            options["AWS_ENDPOINT_URL"] = endpoint
        if allow_http:
            options["AWS_ALLOW_HTTP"] = allow_http.lower()
        if force_path_style:
            options["AWS_S3_ADDRESSING_STYLE"] = (
                "path" if force_path_style.lower() == "true" else "virtual"
            )

        return options

    @staticmethod
    def _normalize_record(record: Any) -> dict[str, Any]:
        if isinstance(record, dict):
            if "after" in record and isinstance(record["after"], dict):
                return record["after"]
            return record
        return {"value": record}

    def _to_arrow_table(self, data: list[Any]) -> pa.Table:
        rows = [self._normalize_record(item) for item in data]
        payloads = [json.dumps(row, default=str) for row in rows]
        return pa.table({"payload": payloads})

    def _write_to_db(self, data: list[Any]) -> None:
        if not data:
            return
        arrow_table = self._to_arrow_table(data)
        write_deltalake(
            self.table_uri,
            arrow_table,
            mode="append",
            storage_options=self.storage_options,
        )

    def write(self, batch: SinkBatch) -> None:
        attempts_remaining = 3
        data = [item.value for item in batch]
        while attempts_remaining:
            try:
                return self._write_to_db(data)
            except ConnectionError:
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
            except TimeoutError:
                raise SinkBackpressureError(
                    retry_after=30.0,
                    topic=batch.topic,
                    partition=batch.partition,
                )
        raise RuntimeError("Error while writing data to Delta table")


def main() -> None:
    app = Application(
        consumer_group="cdc-sink-group-delta",
        auto_create_topics=True,
        auto_offset_reset="earliest",
    )
    delta_sink = MyDeltaSink()
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)
    sdf = sdf.apply(lambda row: row).print(metadata=True)
    sdf.sink(delta_sink)
    app.run()


if __name__ == "__main__":
    main()
