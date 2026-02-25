# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sinks, see https://quix.io/docs/quix-streams/connectors/sinks/index.html
import json
import os
import time
from typing import Any

import pyarrow as pa
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError,
)
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBackpressureError, SinkBatch

# for local dev, you can load env vars from a .env file
load_dotenv()


class MyDatabaseSink(BatchingSink):
    """Write consumed Kafka records into an Iceberg table on S3."""

    def __init__(self) -> None:
        super().__init__()
        self.namespace = os.getenv("ICEBERG_NAMESPACE")
        self.table_name = os.getenv("ICEBERG_TABLE")
        self.catalog = self._create_catalog()
        self.table = self._get_or_create_table()

    def _create_catalog(self):
        catalog_options: dict[str, str] = {
            "type": os.getenv("ICEBERG_CATALOG_TYPE"),
            "uri": os.getenv("ICEBERG_CATALOG_URI"),
            "warehouse": os.getenv("ICEBERG_WAREHOUSE"),
        }

        access_key = os.getenv("AWS_ACCESS_KEY_ID")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        region = os.getenv("AWS_REGION")
        endpoint = os.getenv("ICEBERG_S3_ENDPOINT")
        path_style = os.getenv("ICEBERG_S3_PATH_STYLE_ACCESS", "true")

        if access_key:
            catalog_options["s3.access-key-id"] = access_key
        if secret_key:
            catalog_options["s3.secret-access-key"] = secret_key
        if region:
            catalog_options["s3.region"] = region
        if endpoint:
            catalog_options["s3.endpoint"] = endpoint
        if path_style:
            catalog_options["s3.path-style-access"] = path_style.lower()

        return load_catalog(
            os.getenv("ICEBERG_CATALOG_NAME", "demo"),
            **catalog_options,
        )

    def _get_or_create_table(self):
        namespace_identifier = (self.namespace,)
        table_identifier = (*namespace_identifier, self.table_name)

        if not self.catalog.namespace_exists(namespace_identifier):
            try:
                self.catalog.create_namespace(namespace_identifier)
            except NamespaceAlreadyExistsError:
                pass

        try:
            return self.catalog.load_table(table_identifier)
        except (NoSuchNamespaceError, NoSuchTableError):
            pass

        schema = Schema(
            NestedField(
                field_id=1, name="payload", field_type=StringType(), required=False
            )
        )
        return self.catalog.create_table(identifier=table_identifier, schema=schema)

    @staticmethod
    def _normalize_record(record: Any) -> dict[str, Any]:
        if isinstance(record, dict):
            return record
        return {"value": record}

    def _to_arrow_table(self, data: list[Any]) -> pa.Table:
        rows = [self._normalize_record(item) for item in data]
        payloads = [json.dumps(row, default=str) for row in rows]
        return pa.table({"payload": payloads})

    def _write_to_db(self, data: list[Any]) -> None:
        if not data:
            return
        self.table.append(self._to_arrow_table(data))

    def write(self, batch: SinkBatch) -> None:
        """
        Every Sink requires a .write method.

        Here is where we attempt to write batches of data (multiple consumed messages,
        for the sake of efficiency/speed) to our destination.
        """
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
        raise RuntimeError("Error while writing data to Iceberg table")


def main() -> None:
    """Set up and run the Quix application."""
    app = Application(
        consumer_group="cdc-sink-group",
        auto_create_topics=True,
        auto_offset_reset="earliest",
    )
    my_db_sink = MyDatabaseSink()
    input_topic_name = os.environ["input"]
    print(f"Subscribing to topic: {input_topic_name}")
    
    input_topic = app.topic(name=input_topic_name)
    sdf = app.dataframe(topic=input_topic)
    sdf = sdf.apply(lambda row: row).print(metadata=True)
    sdf.sink(my_db_sink)
    app.run()


if __name__ == "__main__":
    main()
