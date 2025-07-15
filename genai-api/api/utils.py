# app/utils.py

import fitz  # PyMuPDF
import io
from typing import List
from opensearchpy import OpenSearch
import openai
import os

openai.api_key = os.environ["OPENAI_API_KEY"]

def get_embedding(text: str) -> List[float]:
    response = openai.embeddings.create(
        model="text-embedding-3-small",
        input=text,
        encoding_format="float"
    )
    return response.data[0].embedding

def extract_text(file_bytes: bytes, filename: str) -> str:
    if filename.endswith(".pdf"):
        doc = fitz.open(stream=io.BytesIO(file_bytes), filetype="pdf")
        return "\n".join([page.get_text() for page in doc])
    else:
        return file_bytes.decode("utf-8", errors="ignore")

def chunk_text(text: str, max_tokens: int = 300) -> List[str]:
    sentences = text.split(". ")
    chunks, current = [], ""
    for sent in sentences:
        if len(current.split()) + len(sent.split()) > max_tokens:
            chunks.append(current.strip())
            current = sent
        else:
            current += sent + ". "
    if current:
        chunks.append(current.strip())
    return chunks

def index_chunks(chunks: List[str], doc_sha: str, filename: str, os_client: OpenSearch, index: str):
    for i, chunk in enumerate(chunks):
        vector = get_embedding(chunk)
        os_client.index(index=index, body={
            "text": chunk,
            "vector": vector,
            "metadata": {
                "chunk": i,
                "doc_sha": doc_sha,
                "filename": filename
            }
        })

def ask_question(question: str, os_client: OpenSearch, index: str, top_k: int = 4):
    q_vec = get_embedding(question)
    res = os_client.search(index=index, body={
        "size": top_k,
        "query": {
            "knn": {
                "vector": {
                    "vector": q_vec,
                    "k": top_k
                }
            }
        }
    })

    context_chunks = [hit["_source"]["text"] for hit in res["hits"]["hits"]]
    sources = [hit["_source"]["metadata"] for hit in res["hits"]["hits"]]

    prompt = f"""You are a helpful assistant. Answer the user's question using only the context below.

Context:
{chr(10).join(context_chunks)}

Question:
{question}

Answer:"""

    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3
    )

    return response["choices"][0]["message"]["content"], sources