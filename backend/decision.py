import json
import logging
import re
from functions import tool_defs, functions_prompt, table_metadata
from google.generativeai import GenerativeModel
from dotenv import load_dotenv
import os

load_dotenv()
api_key = os.getenv("GOOGLE_API_KEY")

model = GenerativeModel(
    model_name="gemini-2.5-flash",
    tools=tool_defs,
    system_instruction=functions_prompt + "\n" + json.dumps(table_metadata),
)

def decide_tool_call(user_query: str):
    try:
        prompt = f"""{functions_prompt}

        You are an intelligent assistant that answers user questions using both structured and unstructured data.  
You must decide when to generate SQL queries for structured tables and when to use semantic search for unstructured text.  
Always choose the most reliable method depending on the query. It should not be case sensitive.

General Rules:
1. Structured Data (SQL / Tables)  
   - Use the provided table metadata to map user queries to correct tables and columns. 
   - If the user query provides an incomplete date (like "July 4th" without a year), 
  interpret it flexibly:
    • Match all years for that month/day. Example: "July 4th" → any row where Sale Date LIKE '%-07-04'.  
    • Match a whole month if only the month is provided. Example: "July 2025" → any row where Sale Date LIKE '2025-07%'.  
    • Match across all years if only the month is provided without year. Example: "July" → any row where Sale Date LIKE '%-07-%'.  
   - Normalize data formats:  
     • Dates → always `YYYY-MM-DD`. Even when the year is not mentioned in query from user, give results related to that.  
     • Numeric values → ensure correct math (SUM, AVG, MIN, MAX, COUNT).  
     • Text fields → support filtering and grouping (e.g., Category, Country, Payment Method).
  Date & datetime rules:
- If the user gives a full date (YYYY-MM-DD), generate an equality on the date portion:
    DATE("<column>") = 'YYYY-MM-DD'
  This matches values stored as 'YYYY-MM-DD' and 'YYYY-MM-DD HH:MM:SS'.
- If the user gives a month & day (e.g. "July 4") or a partial date without year, match month/day across all years:
    strftime('%m-%d', "<column>") = '07-04'
- If the user gives a month (e.g. "July 2025" or "July"), use:
    • year+month -> strftime('%Y-%m', "<column>") = '2025-07'
    • month only   -> strftime('%m', "<column>") = '07' (or use LIKE on text)
- For date ranges use BETWEEN or `DATE("<col>") BETWEEN 'start' AND 'end'`.
- **Do not** append COLLATE NOCASE to DATE(...) or strftime(...) expressions. Dates are compared by DATE/strftime, not by collation.
   - Resolve ambiguity using column descriptions.  
   - If the user query implies comparison, totals, averages, or grouping, build the SQL query accordingly.  
   - Return the result in a clean, user-friendly format, not raw SQL.

2. Unstructured Data (Documents, Policies, Notes) 
   - Use semantic search (via embeddings) when the question is about documents, policies, product manuals, or unstructured text.  
   - Retrieve the most relevant chunks of text and answer in clear natural language.  
   - If exact information is not found, return the closest relevant context, but **do not hallucinate**. Clearly state when information is missing.

3. When Both Could Apply  
   - If a question can be answered from either structured or unstructured data, prefer structured data (SQL) first for accuracy.  
   - If structured data has no match, fall back to unstructured data search.

4. Formatting the Answer  
   - Keep responses concise and easy to read.  
   - Use tables for tabular results, plain text for policy explanations.  
   - Never expose raw SQL unless explicitly asked.  
   - Always explain the result in natural language.

The metadata below contains table schemas and descriptions. Use them only when SQL is required.


Here is the table metadata you can use:
{json.dumps(table_metadata)}

User query: {user_query}"""

        response = model.generate_content(
            prompt,
            tools=tool_defs,
            tool_config={"function_calling_config": "auto"}
        )

        logging.info("Gemini raw response: %s", response)

        try:
            tool_call = response.candidates[0].content.parts[0].function_call
            if tool_call:
                logging.info(f"✅ Structured tool call: {tool_call}")
                return {
                    "function_call": {
                        "name": tool_call.name,
                        "arguments": tool_call.args
                    }
                }
        except Exception as structured_err:
            logging.warning("No structured tool call. Trying fallback.")
            logging.exception(structured_err)

        try:
            text = response.text or response.candidates[0].content.parts[0].text
            match = re.search(r'\{.*\}', text, re.DOTALL)
            if match:
                tool_dict = json.loads(match.group())
                logging.info(f"Parsed fallback function_call: {tool_dict}")
                return tool_dict
        except Exception as fallback_err:
            logging.warning("Fallback parsing failed.")
            logging.exception(fallback_err)

        return None

    except Exception as e:
        logging.warning("Exception in decide_tool_call:")
        logging.exception(e)
        return None
