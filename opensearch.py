from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3, os
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent / "genai-api" / ".env"
load_dotenv(dotenv_path=env_path)

region = "us-east-1"
credentials = boto3.Session().get_credentials()

client = OpenSearch(
    hosts=[{"host": os.environ["OPENSEARCH_HOST"], "port": 443}],
    http_auth=AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        region,
        "es",
        session_token=credentials.token
    ),
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

response = client.delete_by_query(
    index=os.environ["OPENSEARCH_INDEX"],
    body={"query": {"match_all": {}}}
)

print("Deleted:", response["deleted"])