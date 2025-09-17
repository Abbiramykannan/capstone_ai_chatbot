import sqlite3
import pandas as pd
import logging
import csv
import os
import re

last_uploaded_table = None
last_uploaded_file_type = None 

def load_csv_to_sqlite(filepath, table_name, db_name="data.db"):
    global last_uploaded_table, last_uploaded_file_type
    try:
        df = pd.read_csv(filepath, encoding="utf-8")
    except UnicodeDecodeError:
        print("UTF-8 failed, trying ISO-8859-1...")
        df = pd.read_csv(filepath, encoding="ISO-8859-1")
    with sqlite3.connect(db_name) as conn:
        df.to_sql(table_name, conn, if_exists='replace', index=False)
    last_uploaded_table = table_name
    last_uploaded_file_type = "csv" 

def get_last_table():
    return last_uploaded_table

def set_last_table(name):
    global last_uploaded_table
    last_uploaded_table = name

def get_last_file_type():
    return last_uploaded_file_type 

def set_last_file_type(ftype: str):
    global last_uploaded_file_type
    last_uploaded_file_type = ftype 

def list_tables(db_name="data.db"):
    with sqlite3.connect(db_name) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
    return [t[0] for t in tables]

def ask_sql_question(query: str, table_name: str, db_name="data.db") -> str:
    with sqlite3.connect(db_name) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)

            if df.shape == (1, 1):
                return str(df.iat[0, 0])
            else:
                return df.to_string(index=False)

        except Exception as e:
            return str(e)


def quote_column(col):
        func_match = re.match(r'^\s*(COUNT|SUM|AVG|MIN|MAX)\s*\((.+)\)\s*$', col, re.IGNORECASE)
        if func_match:
            func_name = func_match.group(1)
            inner_col = func_match.group(2).strip()
            if not (inner_col.startswith('"') and inner_col.endswith('"')):
                inner_col = f'"{inner_col}"'
            return f'{func_name}({inner_col})'
        if not (col.startswith('"') and col.endswith('"')):
            return f'"{col}"'
        return col


def get_selected_columns(table_name, columns=None, where_clause=None, aggregations=None, group_by=None, distinct=False):
    logging.info("get selected column is called")
    conn = sqlite3.connect("data.db")
    cursor = conn.cursor()

    if columns is None:
        columns = []
    if aggregations is None:
        aggregations = []
    if group_by is None:
        group_by = []
    select_parts = []

    if distinct:
        select_parts.append("DISTINCT")

    if aggregations:
        for agg in aggregations:
            select_parts.append(f'{agg["operation"]}("{agg["column"]}")')
    else:
        for col in columns:
            select_parts.append(quote_column(col))

    if not select_parts:
        select_parts.append("*") 

    select_clause = ", ".join(select_parts)
    quoted_table = f'"{table_name}"'
    query = f"SELECT {select_clause} FROM {quoted_table}"

    if where_clause:
        query += f" WHERE {where_clause} COLLATE NOCASE"

    if group_by:
        group_sql = ", ".join([f'"{col}"' for col in group_by])
        query += f" GROUP BY {group_sql}"

    print("🧠 Final SQL Query -->", query)
    return execute_sql_query(query)


def execute_sql_query(query: str, db_name="data.db"):
    logging.info("Execute sql query is executed")    
    try:
        logging.info(f"This is the input of execute_sql_query:{query}")

        if not os.path.exists(db_name):
            return "Please upload a file first."

        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            headers = [desc[0] for desc in cursor.description] if cursor.description else []
        result =  [dict(zip(headers, row)) for row in rows] if headers else []
        logging.info(f"this is the raw result after execution:{result}")

            
        return result
    except Exception as e:
        print(f"SQL Execution Error: {e}")
        return []



