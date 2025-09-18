import pandas as pd
import streamlit as st
import requests
import json
import os

st.set_page_config(page_title="AI Chatbot", layout="centered")

BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8001")

def safe_json(response):
    try:
        return response.json()
    except Exception:
        return {"error": response.text or "Invalid response from backend"}

st.title("AI-Powered Document Chatbot")

if st.button("Reset Data"):
    response = requests.post(f"{BACKEND_URL}/reset")
    st.success(response.json().get("message", "Data reset."))

query = st.text_input("Ask a question")
if st.button("Submit"):
    if not query.strip():
        st.error("Please enter a question before submitting.")
    else:
        with st.spinner("Thinking..."):
            res = requests.post(f"{BACKEND_URL}/query", data={"query": query})
            data = safe_json(res)

            st.subheader("Query Result:")

            if "table_result" in data and isinstance(data["table_result"], list):
                import pandas as pd
                df = pd.DataFrame(data["table_result"])
                if df.empty:
                    st.info("No records found for this query.")
                else:
                    st.dataframe(df)  

            elif "response" in data:
                st.write(data["response"])

            else:
                st.error(data.get("error", "Unexpected error occurred"))

st.markdown("---")
st.markdown("### Upload CSV, PDF, DOCX or XLSX")
uploaded = st.file_uploader("Choose a file", type=["csv", "pdf", "docx", "xlsx"])
if uploaded:
    files = {"file": (uploaded.name, uploaded, uploaded.type)}
    with st.spinner("Uploading..."):
        res = requests.post(f"{BACKEND_URL}/upload", files=files)
        data = safe_json(res)

        if "message" in data:
            st.success(data["message"])
        else:
            st.error(data.get("error", "Upload failed."))
