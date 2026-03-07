import os
from quixstreams import Application
from quixstreams.sinks.community.file.s3 import S3FileSink
from quixstreams.sinks.community.file.formats import JSONFormat
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

# Quix application processing
app = Application(
    broker_address=broker_address,
    consumer_group=consumer_group,
    auto_offset_reset="earliest",
)
topic = app.topic(input_topic)

# Configure the sink to write JSON files to S3
file_sink = S3FileSink(
    bucket=bucket_name,
    directory=s3_path,
    format=JSONFormat(compress=True),
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)

sdf = app.dataframe(topic)

# Sink data to S3
sdf.sink(file_sink)

if __name__ == "__main__":
    app.run()