# app/utils.py

import fitz  # PyMuPDF
import io
from typing import List
from opensearchpy import OpenSearch
import openai
from openai import OpenAI
import os
import requests

client = OpenAI()

def get_embedding(text: str) -> List[float]:
    response = client.embeddings.create(
        input=[text],
        model="text-embedding-ada-002"
    )
    return response.data[0].embedding
def extract_text(file_bytes: bytes, filename: str) -> str:
    if filename.endswith(".pdf"):
        doc = fitz.open(stream=io.BytesIO(file_bytes), filetype="pdf")
        return "\n".join([page.get_text() for page in doc])
    else:
        return file_bytes.decode("utf-8", errors="ignore")

def chunk_text(text: str, max_tokens: int = 500) -> List[str]:
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

def index_chunks(chunks, sha, filename, opensearch, index_name):
    print("DEBUG: Entered index_chunks")
    try:
        for i, chunk in enumerate(chunks):
            print(f"DEBUG: Indexing chunk {i+1}/{len(chunks)}")

            embedding = get_embedding(chunk)  # <- NEW: Embed the chunk here

            doc = {
                "sha": sha,
                "filename": filename,
                "chunk": chunk,
                "chunk_id": i,
                "embedding": embedding  # <- NEW: Include the vector
            }

            response = opensearch.index(index=index_name, body=doc)
            print(f"DEBUG: Chunk {i+1} indexed. Response: {response}")
    except Exception as e:
        print("ERROR in index_chunks:", e)
    print("DEBUG: Finished index_chunks")

def ask_question(question: str, os_client: OpenSearch, index: str, top_k: int = 4):
    print("DEBUG: Starting ask_question")

    # Step 1: Embed question
    print("DEBUG: Generating embedding for question...")
    embedding = get_embedding(question)
    print("DEBUG: Embedding generated")

    # Step 2: Query OpenSearch with KNN
    print("DEBUG: Querying OpenSearch...")
    response = os_client.search(
        index=index,
        body={
            "size": top_k,
            "query": {
                "knn": {
                    "embedding": {
                        "vector": embedding,
                        "k": top_k
                    }
                }
            }
        }
    )
    print(f"DEBUG: OpenSearch response: {response}")

    # Step 3: Extract relevant chunks and sources
    hits = response["hits"]["hits"]
    context_chunks = [hit["_source"]["chunk"] for hit in hits]
    sources = [hit["_source"]["filename"] for hit in hits]

    # Step 4: Build RAG prompt
    prompt = f"""You are a helpful assistant. Answer the user's question using only the context below.

Context:
{chr(10).join(context_chunks)}

Question:
{question}

Answer:"""

    # Step 5: Get answer from OpenAI
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3
    )
    return response.choices[0].message.content, sources