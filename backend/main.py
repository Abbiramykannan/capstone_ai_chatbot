from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
import router, sql_handler, doc_handler, embedding
from sql_handler import get_last_table, set_last_table, get_last_file_type, set_last_file_type,execute_sql_query, quote_column, get_selected_columns
from router import model
from functions import functions_prompt, table_metadata, tool_defs
from embedding import index_document, check_embeddings_exist, extract_text_chunks
from fastapi.encoders import jsonable_encoder
from decision import decide_tool_call
from dispatcher import convert_where_clause, proto_to_dict,dispatch_function
import os
import re
import logging
import json
import sqlite3
import pandas as pd
import time
import io
import sys


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("app.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

app = FastAPI()


@app.on_event("startup")
def startup_event():
    try:
        logging.info("Ensuring Qdrant collection exists...")
        from embedding import ensure_collection_exists
        ensure_collection_exists(vector_size=3072, collection_name="documents")
        logging.info("Qdrant collection ready.")
    except Exception:
        logging.exception("Qdrant collection init error")

@app.post("/reset")
async def reset_data():
    try:
        from embedding import client
        client.delete_collection("documents")

        db_path = "data.db"
        if os.path.exists(db_path):
            os.remove(db_path)

        return {"message": "✅ All data has been reset. Please upload a new file."}
    except Exception as e:
        logging.exception("Reset error")
        return {"error": str(e)}




from dispatcher import FUNCTION_REGISTRY 

@app.post("/query")
async def handle_query(query: str = Form(...)):
    try:
        tool_call = decide_tool_call(query)
        if not tool_call:
            return {"response": "Sorry, no relevant function was triggered by the query."}

        function_call = tool_call["function_call"]
        function_name = function_call["name"]
        args = function_call["arguments"]

        if isinstance(args, str):
            args = json.loads(args)
        elif hasattr(args, "items"):
            args = proto_to_dict(args)
        elif not isinstance(args, dict):
            raise ValueError(f"Unexpected type for args: {type(args)} - {args}")

        logging.info(f"Dispatching function: {function_name} with args: {args}")

        if function_name in FUNCTION_REGISTRY:
            raw_result = FUNCTION_REGISTRY[function_name](args)
        else:
            raw_result = {"message": f"Function '{function_name}' not supported."}
        
        logging.info(f"it is the raw result :{raw_result}")
        format_prompt = f"""
        The user asked: {query}
        The raw function result is: {raw_result}

        Your task: Reformat this into a clear, conversational, user-friendly response.

        Guidelines:
        - If the result looks like structured tabular data (list of dicts), return it as a nice table.
        - If it's a single value, explain it naturally in one sentence.
        - If it's unstructured text, return a clean, short answer.
        - If the user asked a question before uploading any file, politely say: "⚠️ Please upload a file first to answer this query."
        - Avoid exposing raw JSON or SQL.

        # Examples:

        User: "What is the shipping status of order 1001?"
        Raw Result: [{{"Shipping Status": "Delivered"}}]  
        Answer: The shipping status of order **1001** is **Delivered**.

        ---

        User: "Show me total price and sale date from sales details"
        Raw Result: [
            {{"Sale Date": "2025-07-01", "Total Price": 500}},
            {{"Sale Date": "2025-07-02", "Total Price": 700}}
        ]  
        Answer: Here are the sales details:

        | Sale Date   | Total Price |
        |-------------|-------------|
        | 2025-07-01  | 500         |
        | 2025-07-02  | 700         |

        ---


        User: "Give me a summary of priya sharma's purchase"
        Raw Result: [{{'Order ID': 'ORD100', 'Product Name': 'Wireless Mouse', 'Quantity': 2, 'Total Price': 1000, 'Sale Date': '2025-07-04 00:00:00'}}]
        Answer: Priya Sharma ordered Wireless Mouse on 2025-07-04 00:00:00. She ordered 2 quantities of it for Total Price 1000 and here order id is ORD100.


        ---

        User: "Tell me about the refund policy"
        Raw Result: "Our refund policy allows returns within 30 days."  
        Answer: Our refund policy allows returns **within 30 days**.

        ---

        User: "Can you calculate profit margin?"
        Raw Result: "Sorry, I need the uploaded file to answer this."  
        Answer: Please upload a file first to answer queries about data.

        ---

        Now, reformat the given raw result for this query accordingly:
        """

        llm_response = model.generate_content(format_prompt)
        formatted = llm_response.candidates[0].content.parts[0].text

        return JSONResponse({"response": formatted})

    except Exception as e:
        logging.exception("Query error")
        raise HTTPException(status_code=500, detail=str(e))



@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        filepath = f"uploads/{file.filename}"
        logging.info(f"Starting upload for file: {file.filename}")
        os.makedirs("uploads", exist_ok=True)
        filepath = os.path.join("uploads", file.filename)
        file_already_exists = os.path.exists(filepath)


        if file_already_exists:
            logging.warning("File already exists.")

            if not check_embeddings_exist(file.filename):
                logging.info("Embeddings not found in Qdrant. Reprocessing the file...")
                filepath = f"uploads/{file.filename}"
                text_chunks = extract_text_chunks(filepath)
                index_document(text_chunks, file.filename)

            else:
                logging.info("Embeddings already exist. Skipping reprocessing.")


            return {"message": "File already exists, no changes made."}
        
        with open(filepath, "wb") as f:
            f.write(await file.read())
        logging.info(f"File saved at: {filepath}")
        msg = "File processed successfully."

        if file.filename.endswith(".csv"):
            raw_name = os.path.splitext(file.filename)[0]
            table_name = re.sub(r'\W+', '_', raw_name).strip('_').lower()
            logging.info(f"Processing CSV: {table_name}")
            sql_handler.load_csv_to_sqlite(filepath, table_name)
            set_last_table(table_name)
            set_last_file_type("csv")
            msg = f"CSV uploaded and indexed in SQLite as '{table_name}'."

        elif file.filename.endswith(".xlsx"):
            logging.info("Processing Excel with multiple sheets...")
            xls = pd.ExcelFile(filepath)
            conn = sqlite3.connect("data.db")
            for sheet_name in xls.sheet_names:
                start = time.time()
                df = pd.read_excel(xls, sheet_name=sheet_name)

            
                table_name = re.sub(r'\W+', '_', sheet_name).strip('_').lower()
                df.to_sql(table_name, conn, if_exists="replace", index=False, chunksize=1000)
                logging.info(f"Inserted sheet '{sheet_name}' as table '{table_name}' in {time.time() - start:.2f}s")
            conn.close()
            set_last_table(xls.sheet_names[0])
            set_last_file_type("xlsx")
            msg = f"Excel file uploaded. Sheets saved as tables: {xls.sheet_names}"

        elif file.filename.endswith(".pdf"):
            logging.info("Extracting PDF...")
            text = doc_handler.extract_pdf_text(filepath)
            embedding.index_document(text.split(". "), file.filename)
            set_last_file_type("pdf")
            msg = "PDF uploaded and indexed."

        elif file.filename.endswith(".docx"):
            logging.info("Extracting DOCX...")
            text = doc_handler.extract_docx_text(filepath)
            embedding.index_document(text.split(". "))
            set_last_file_type("docx")
            msg = "DOCX uploaded and indexed."

        else:
            raise HTTPException(400, "Unsupported file type.")

        return {"message": msg}

    except HTTPException:
        raise
    except Exception as e:
        logging.exception("Upload error occurred")
        raise HTTPException(status_code=500, detail=str(e))


@app.exception_handler(Exception)
async def handle_unexpected_exceptions(request, exc):
    if isinstance(exc, HTTPException):
        return JSONResponse(status_code=exc.status_code, content={"error": exc.detail})
    return JSONResponse(status_code=500, content={"error": str(exc)})
