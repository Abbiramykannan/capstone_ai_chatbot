

from dotenv import load_dotenv
load_dotenv()
import logging
import os
import uuid
import fitz
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, Distance, VectorParams
from google.generativeai import embed_content
from google.generativeai import GenerativeModel

qa_model = GenerativeModel("gemini-2.5-flash")

EMBED_MODEL = "models/gemini-embedding-001"

client = QdrantClient(
    url=os.getenv("QDRANT_URL"),
    api_key=os.getenv("QDRANT_API_KEY"),
    timeout=10.0  
)



def extract_text_chunks(file_path, chunk_size=500):
    """Extract text from PDF and split into chunks."""
    doc = fitz.open(file_path)
    text = ""
    for page in doc:
        text += page.get_text()
    return [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]


def ensure_collection_exists(vector_size: int, collection_name="documents"):
    """Ensure Qdrant collection exists with correct vector size; recreate if mismatched."""
    try:
        collections = client.get_collections().collections
        existing = next((c for c in collections if c.name == collection_name), None)

        if not existing:
            logging.info(f"Creating collection '{collection_name}' with dim={vector_size}...")
            client.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
            )
        else:
            coll_info = client.get_collection(collection_name)
            existing_dim = coll_info.config.params.vectors.size

            if existing_dim != vector_size:
                logging.warning(
                    f"Dimension mismatch for '{collection_name}': expected {vector_size}, got {existing_dim}. Recreating..."
                )
                client.recreate_collection(
                    collection_name=collection_name,
                    vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
                )
            else:
                logging.info(f"Collection '{collection_name}' already exists with correct dim={existing_dim}.")
    except Exception as e:
        logging.exception("Error ensuring collection")


def index_document(text_chunks, file_name: str, collection_name="documents"):
    """Index document chunks into Qdrant with embeddings."""
    embeddings = [
        embed_content(content=chunk, model=EMBED_MODEL, task_type="retrieval_document")['embedding']
        for chunk in text_chunks
    ]

    ensure_collection_exists(vector_size=len(embeddings[0]), collection_name=collection_name)

    points = [
        PointStruct(
            id=str(uuid.uuid4()),
            vector=emb,
            payload={"text": chunk, "file_name": file_name}
        )
        for chunk, emb in zip(text_chunks, embeddings)
    ]

    client.upsert(collection_name=collection_name, points=points)
    print(f"✅ File '{file_name}' indexed successfully!")


def search_similar(query, collection_name="documents"):
    """Search similar text chunks and generate answer."""
    embedding = embed_content(content=query, model=EMBED_MODEL, task_type="retrieval_query")['embedding']
    hits = client.search(collection_name, query_vector=embedding, limit=5)

    if not hits:
       return "Please upload a file first"


    context = "\n\n".join(hit.payload["text"] for hit in hits if "text" in hit.payload)

    prompt = f"""
You are an intelligent assistant. Based on the following document content, answer the question concisely and clearly.

Document Content:
\"\"\"
{context}
\"\"\"

Question: {query}
Answer:
"""
    response = qa_model.generate_content(prompt)
    return response.text.strip()


def check_embeddings_exist(file_name: str, collection_name="documents") -> bool:
    """Check if a file's embeddings already exist in Qdrant."""
    try:
        scroll_result = client.scroll(
            collection_name=collection_name,
            limit=1,
            with_payload=True
        )
        for point in scroll_result[0]:
            if point.payload.get("file_name") == file_name:
                return True
        return False
    except Exception as e:
        print("Error in checking embeddings:", e)
        return False
