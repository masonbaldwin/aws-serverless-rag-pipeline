from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3

region = 'us-east-1'
service = 'es'

credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    region,
    service,
    session_token=credentials.token
)

host = 'search-genai-vector-db-3pccssj7pwu3trjlw6qbxq23uu.us-east-1.es.amazonaws.com'

client = OpenSearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

doc_count = client.count(index="genai-index")
print(f"Document count: {doc_count['count']}")

response = client.search(
    index="genai-index",
    body={
        "query": {
            "match_all": {}
        },
        "size": 5  # Adjust as needed
    }
)

print("Sample documents from genai-index:")
for hit in response['hits']['hits']:
    print(hit['_source'])