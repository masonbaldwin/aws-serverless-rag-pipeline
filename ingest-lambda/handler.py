import os
import json
import boto3
import tempfile

from unstructured.partition.pdf import partition_pdf
from unstructured.partition.docx import partition_docx
from unstructured.partition.txt import partition_text

from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

s3 = boto3.client("s3")

OPENSEARCH_HOST = os.environ["OPENSEARCH_HOST"]
OPENSEARCH_INDEX = os.environ.get("OPENSEARCH_INDEX", "documents")
REGION = os.environ.get("AWS_REGION", "us-east-1")

credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    REGION,
    "es",
    session_token=credentials.token,
)

opensearch = OpenSearch(
    hosts=[{"host": OPENSEARCH_HOST, "port": 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
)

def ingest_handler(event, context):
    try:
        for record in event["Records"]:
            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]

            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                s3.download_fileobj(bucket, key, tmp_file)
                tmp_path = tmp_file.name

            if key.endswith(".pdf"):
                elements = partition_pdf(filename=tmp_path)
            elif key.endswith(".docx"):
                elements = partition_docx(filename=tmp_path)
            elif key.endswith(".txt"):
                elements = partition_text(filename=tmp_path)
            else:
                raise ValueError(f"Unsupported file type: {key}")

            text = "\n".join([el.text for el in elements if hasattr(el, "text")])

            doc = {
                "s3_bucket": bucket,
                "s3_key": key,
                "content": text,
            }

            response = opensearch.index(index=OPENSEARCH_INDEX, body=doc)
            print("Indexed doc:", response)

        return {
            "statusCode": 200,
            "body": json.dumps("Document processed and indexed.")
        }

    except Exception as e:
        print("Error:", e)
        return {
            "statusCode": 500,
            "body": json.dumps(str(e))
        }