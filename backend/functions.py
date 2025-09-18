import json
import os

BASE_DIR = os.path.dirname(__file__)
json_path = os.path.join(BASE_DIR, "functions.json")


with open("functions.json", "r") as f:
    data = json.load(f)

if isinstance(data, list):
    data = data[0]

functions_prompt = data["prompt"]
user_query = data["User Query"]
table_metadata = data["table_metadata"]
tool_defs = data["functions"]

