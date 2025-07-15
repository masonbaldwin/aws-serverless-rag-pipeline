import json
import boto3
import os
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from unstructured.partition.auto import partition
from unstructured.chunking.title import chunk_by_title
import tempfile

def lambda_handler(event, context):
    print("Received event:", json.dumps(event, indent=2))

    # Parse S3 trigger event
    try:
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
    except (KeyError, IndexError) as e:
        print("Error parsing S3 event:", e)
        raise

    # Download file from S3
    s3 = boto3.client('s3')
    tmp_file_path = os.path.join(tempfile.gettempdir(), os.path.basename(key))

    try:
        s3.download_file(bucket, key, tmp_file_path)
        print(f"Downloaded {key} from {bucket} to {tmp_file_path}")
    except Exception as e:
        print("Error downloading file from S3:", e)
        raise

    # Partition the file into elements
    try:
        elements = partition(filename=tmp_file_path)
        print(f"Partitioned document into {len(elements)} elements")
    except Exception as e:
        print("Error partitioning document:", e)
        raise

    # Chunk elements (with fallback)
    try:
        chunks = chunk_by_title(elements, multipage_sections=True, combine_text_under_n_chars=0)
        if not chunks:
            print("No chunks found — falling back to raw elements.")
            chunks = elements
        print(f"Chunked into {len(chunks)} sections")
    except Exception as e:
        print("Error chunking document:", e)
        raise

    # Prepare documents (don't filter by class type, just check for valid text)
    documents = [
        {
            "content": el.text,
            "metadata": {
                "type": getattr(el, "category", "Unknown"),
                "filename": key,
                "section_number": i
            }
        }
        for i, el in enumerate(chunks)
        if hasattr(el, "text") and el.text and el.text.strip()
    ]

    if not documents:
        print("No documents with text to index.")
        return {"statusCode": 204, "body": "No valid documents to index."}

    # Set up OpenSearch client
    region = os.environ["AWS_REGION"]
    host = os.environ["OPENSEARCH_HOST"]
    index = os.environ.get("OPENSEARCH_INDEX", "genai-index")

    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, "es", session_token=credentials.token)

    client = OpenSearch(
        hosts=[{"host": host, "port": 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
    )

    # Create index if not exists
    if not client.indices.exists(index=index):
        client.indices.create(index=index)

    # Index documents
    for i, doc in enumerate(documents):
        response = client.index(index=index, body=doc)
        print(f"Indexed document {i}: {response.get('_id', 'unknown')}")

    return {
        "statusCode": 200,
        "body": f"Successfully processed and indexed {len(documents)} sections from {key}"
    }