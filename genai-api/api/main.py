# app/main.py

from fastapi import FastAPI, File, UploadFile, Request
from fastapi.responses import JSONResponse
import boto3, hashlib
from opensearchpy import OpenSearch, RequestsHttpConnection
from sentence_transformers import SentenceTransformer
from utils import extract_text, chunk_text, index_chunks, ask_question
import os
from mangum import Mangum


app = FastAPI()

s3 = boto3.client("s3")
embedding_model = SentenceTransformer("all-MiniLM-L6-v2")

opensearch = OpenSearch(
    hosts=[{"host": os.environ["OPENSEARCH_HOST"], "port": 443}],
    http_auth=(os.environ["OS_USER"], os.environ["OS_PASS"]),
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

BUCKET_NAME = os.environ["S3_BUCKET"]
INDEX_NAME = os.environ["OPENSEARCH_INDEX"]

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    contents = await file.read()
    sha = hashlib.sha256(contents).hexdigest()
    key = f"uploads/{file.filename}"

    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=contents)
    text = extract_text(contents, file.filename)
    chunks = chunk_text(text)
    index_chunks(chunks, sha, file.filename, embedding_model, opensearch, INDEX_NAME)

    return {"message": "File indexed successfully", "sha": sha, "filename": file.filename}

@app.post("/ask")
async def ask(req: Request):
    data = await req.json()
    question = data.get("question")
    if not question:
        return JSONResponse(status_code=400, content={"error": "No question provided"})

    answer, sources = ask_question(
        question, embedding_model, opensearch, INDEX_NAME
    )
    return {"answer": answer, "sources": sources}

handler = Mangum(app)
