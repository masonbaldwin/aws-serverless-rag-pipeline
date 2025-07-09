import os, json, uuid, boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from unstructured.partition.auto import partition

OS_HOST = os.environ["OS_HOST"]          # e.g. search-rag-search-xxx.us-east-1.es.amazonaws.com
OS_PWD  = os.environ["OS_PWD"]           # the admin password you set
INDEX   = "docs"
EMBED_MODEL = "amazon.titan-embed-text-v1"

os_client = OpenSearch(
    hosts=[{"host": OS_HOST, "port": 443}],
    http_auth=("admin", OS_PWD),
    use_ssl=True, verify_certs=True,
    connection_class=RequestsHttpConnection,
)

brt = boto3.client("bedrock-runtime")

def embed(texts):
    out = []
    for t in texts:
        resp = brt.invoke_model(
            modelId=EMBED_MODEL,
            body=json.dumps({"inputText": t}),
            accept="application/json",
            contentType="application/json",
        )
        out.append(json.loads(resp["body"].read())["embedding"])
    return out

def handler(event, _):
    s3 = boto3.client("s3")
    rec = event["Records"][0]
    bucket, key = rec["s3"]["bucket"]["name"], rec["s3"]["object"]["key"]

    tmp = f"/tmp/{uuid.uuid4()}"
    s3.download_file(bucket, key, tmp)

    text = "\n".join(el.text for el in partition(filename=tmp) if el.text)
    chunks = [text[i:i+400] for i in range(0, len(text), 400)]
    vecs = embed(chunks)

    for i, (chunk, vec) in enumerate(zip(chunks, vecs)):
        os_client.index(
            index=INDEX,
            id=f"{key}::{i}",
            body={"text": chunk, "source": key, "chunk": i, "embedding": vec},
        )

    return {"statusCode": 200, "body": f"Indexed {len(chunks)} chunks from {key}"}