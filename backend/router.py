import os
from dotenv import load_dotenv
import google.generativeai as genai
from functions import functions_prompt, tool_defs 

load_dotenv()
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

model = genai.GenerativeModel(
    model_name="gemini-2.5-flash",
    system_instruction=functions_prompt,
    tools=[{"function_declarations": tool_defs}]
)
