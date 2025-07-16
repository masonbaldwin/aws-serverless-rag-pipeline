# app/main.py

from fastapi import FastAPI, File, UploadFile, Request
from fastapi.responses import JSONResponse
import boto3, hashlib, os
from opensearchpy import OpenSearch, RequestsHttpConnection
from dotenv import load_dotenv
from pathlib import Path
from requests_aws4auth import AWS4Auth
from fastapi.middleware.cors import CORSMiddleware

region = "us-east-1"  # your actual AWS region
session = boto3.Session()
credentials = session.get_credentials()

# Force load .env from parent of current file's directory
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# Debug: print to verify .env loaded
print("DEBUG: OPENAI_API_KEY =", os.getenv("OPENAI_API_KEY"))

from utils import extract_text, chunk_text, index_chunks, ask_question
from mangum import Mangum

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

s3 = boto3.client("s3")

awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    region,
    "es",
    session_token=credentials.token,
)

opensearch = OpenSearch(
    hosts=[{"host": os.environ["OPENSEARCH_HOST"], "port": 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

BUCKET_NAME = os.environ["S3_BUCKET"]
INDEX_NAME = os.environ["OPENSEARCH_INDEX"]

print("DEBUG: Starting FastAPI app")
print("DEBUG: OPENAI_API_KEY =", os.environ.get("OPENAI_API_KEY"))

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    print("DEBUG: FastAPI app started successfully")
    print("UPLOAD ROUTE HIT")
    contents = await file.read()
    print(f"Received file: {file.filename}, size: {len(contents)} bytes")
    sha = hashlib.sha256(contents).hexdigest()
    key = f"uploads/{file.filename}"
    print(f"Generated SHA: {sha}")
    print(f"Uploading to S3 bucket {BUCKET_NAME} with key {key}")

    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=contents)
    print("S3 upload complete")
    text = extract_text(contents, file.filename)
    print(f"Extracted text length: {len(text)}")
    chunks = chunk_text(text)
    print(f"Chunked into {len(chunks)} parts")
    try:
        index_chunks(chunks, sha, file.filename, opensearch, INDEX_NAME)
        print("Chunks indexed in OpenSearch")
    except Exception as e:
        print(f"Error during indexing: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})
    print("Chunks indexed in OpenSearch")

    print("Returning success response")
    return {"message": "File indexed successfully", "sha": sha, "filename": file.filename}

@app.post("/ask")
async def ask(req: Request):
    print("ASK ROUTE HIT")
    data = await req.json()
    question = data.get("question")
    if not question:
        return JSONResponse(status_code=400, content={"error": "No question provided"})

    answer, sources = ask_question(
        question, opensearch, INDEX_NAME
    )
    return {"answer": answer, "sources": sources}

handler = Mangum(app)
