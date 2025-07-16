import fitz
import io
from typing import List
from opensearchpy import OpenSearch
import openai
from openai import OpenAI
import os
import requests
import pandas as pd

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
    elif filename.endswith(".xlsx"):
        try:
            excel_io = io.BytesIO(file_bytes)
            df_dict = pd.read_excel(excel_io, sheet_name=None)
            all_text = ""
            for sheet_name, df in df_dict.items():
                all_text += f"\nSheet: {sheet_name}\n"
                for row in df.to_dict(orient="records"):
                    row_str = ", ".join([f"{k}: {v}" for k, v in row.items()])
                    all_text += row_str + "\n"
            return all_text
        except Exception as e:
            return f"ERROR parsing Excel: {e}"
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

            embedding = get_embedding(chunk)  

            doc = {
                "sha": sha,
                "filename": filename,
                "chunk": chunk,
                "chunk_id": i,
                "embedding": embedding  
            }

            response = opensearch.index(index=index_name, body=doc)
            print(f"DEBUG: Chunk {i+1} indexed. Response: {response}")
    except Exception as e:
        print("ERROR in index_chunks:", e)
    print("DEBUG: Finished index_chunks")

def ask_question(question: str, os_client: OpenSearch, index: str, top_k: int = 4):
    print("DEBUG: Starting ask_question")

    print("DEBUG: Generating embedding for question...")
    embedding = get_embedding(question)
    print("DEBUG: Embedding generated")

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

    hits = response["hits"]["hits"]
    context_chunks = [hit["_source"]["chunk"] for hit in hits]
    sources = [hit["_source"]["filename"] for hit in hits]

    prompt = f"""You are a helpful assistant. Answer the user's question using only the context below.

Context:
{chr(10).join(context_chunks)}

Question:
{question}

Answer:"""

    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3
    )
    return response.choices[0].message.content, sources