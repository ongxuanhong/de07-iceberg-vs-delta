import json
import os
import time
from typing import Any

import pyarrow as pa
from deltalake import write_deltalake
from dotenv import load_dotenv
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBackpressureError, SinkBatch
from dotenv import load_dotenv

load_dotenv()

# Parameters preparation
broker_address = os.getenv("BROKER_ADDRESS")
consumer_group = os.getenv("CONSUMER_GROUP")
input_topic = os.getenv("INPUT_TOPIC")
bucket_name = os.getenv("BUCKET_NAME")
s3_path = os.getenv("S3_PATH")
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
endpoint_url = os.getenv("AWS_ENDPOINT_URL_S3")
aws_region = os.getenv("AWS_REGION")

# Quix application processing
app = Application(
    broker_address=broker_address,
    consumer_group=consumer_group,
    auto_offset_reset="earliest",
)
topic = app.topic(input_topic)
sdf = app.dataframe(topic)


# Configure the sink to write Delta files to S3
class MyDeltaSink(BatchingSink):
    """Write consumed Kafka records into a Delta table on object storage."""

    def __init__(self) -> None:
        super().__init__()
        self.table_uri = "s3://dev-asia-vn-bronze/event_streaming/customers"
        self.storage_options = self._create_storage_options()

    @staticmethod
    def _create_storage_options() -> dict[str, str]:
        options: dict[str, str] = {}
        force_path_style = os.getenv("DELTA_S3_FORCE_PATH_STYLE", "true")

        if aws_access_key_id:
            options["AWS_ACCESS_KEY_ID"] = aws_access_key_id
        if aws_secret_access_key:
            options["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key
        if aws_region:
            options["AWS_REGION"] = aws_region
        if force_path_style:
            options["AWS_S3_ADDRESSING_STYLE"] = (
                "path" if force_path_style.lower() == "true" else "virtual"
            )

        return options

    def _to_arrow_table(self, data: list[Any]) -> pa.Table:
        payloads = [json.dumps(row, default=str) for row in data]
        return pa.table({"payload": payloads})

    def _write_to_db(self, data: list[Any]) -> None:
        if not data:
            return
        print(f"Writing batch of {len(data)} records to Delta table...")
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


delta_sink = MyDeltaSink()


# Sink data to S3
sdf.sink(delta_sink)

if __name__ == "__main__":
    app.run()
