from backend.sql_handler import get_selected_columns
from backend.embedding import search_similar
from google.protobuf.json_format import MessageToDict
import re
import json
from proto.marshal.collections.maps import MapComposite
from google.protobuf.struct_pb2 import Struct


def clean_args(raw_args):
    if isinstance(raw_args, str):
        try:
            return json.loads(raw_args)
        except Exception:
            return {}
    elif hasattr(raw_args, "items"):
        return dict(raw_args)
    elif isinstance(raw_args, dict):
        return raw_args
    else:
        return {}


def convert_where_clause(where):
    if isinstance(where, dict):
        parts = []
        for k, v in where.items():
            v = str(v).strip()
            if re.match(r"\d{4}-\d{2}-\d{2}", v):
                parts.append(f'DATE("{k}") = \'{v}\'')
            elif "%" in v or "_" in v:
                parts.append(f'"{k}" LIKE \'{v}\' COLLATE NOCASE')
            else:
                parts.append(f'"{k}" = \'{v}\' COLLATE NOCASE')
        return " AND ".join(parts)

    elif isinstance(where, str):
        s = re.sub(r"=\s*\'([^']*[%_][^']*)\'", r"LIKE \'\1\'", where)
        return re.sub(r"([\w\s]+)(\s*=\s*|LIKE\s*)", 
                      lambda m: f'"{m.group(1).strip()}"{m.group(2)} COLLATE NOCASE', s)

    return None


def proto_to_dict(proto_obj):
    if isinstance(proto_obj, MapComposite):
        return {k: proto_to_dict(v) for k, v in proto_obj.items()}
    elif isinstance(proto_obj, Struct):
        return {k: proto_to_dict(v) for k, v in proto_obj.fields.items()}
    elif hasattr(proto_obj, "items"): 
        return {k: proto_to_dict(v) for k, v in proto_obj.items()}
    elif isinstance(proto_obj, list):
        return [proto_to_dict(v) for v in proto_obj]
    elif hasattr(proto_obj, "string_value"):
        return proto_obj.string_value
    elif hasattr(proto_obj, "number_value"):
        return proto_obj.number_value
    elif hasattr(proto_obj, "bool_value"):
        return proto_obj.bool_value
    else:
        return proto_obj

def flatten_aggregations(aggs):
    if not aggs:
        return []
    return [proto_to_dict(agg) for agg in aggs]


FUNCTION_REGISTRY = {
    "get_order_details": lambda args: get_selected_columns(
        table_name=args["table_name"],
        columns=args.get("columns", []),
        where_clause=convert_where_clause(
            proto_to_dict(args.get("whereClause") or args.get("where_clause"))
        ),
        aggregations=flatten_aggregations(args.get("aggregations")),
        group_by=[proto_to_dict(g) for g in args.get("group_by", [])],
        distinct=args.get("distinct", False)
    ),


    "get_policy_info": lambda args: search_similar(
        proto_to_dict(args)["query"]
    ),

    "handle_unknown_query": lambda args: proto_to_dict(args).get(
        "message", "Sorry, I couldn’t understand your request."
    ),
}

def dispatch_function(func_name, raw_args):
    """
    Main dispatcher: cleans up raw protobuf args and calls the correct function.
    """
    args = proto_to_dict(raw_args) 

    if func_name in FUNCTION_REGISTRY:
        return FUNCTION_REGISTRY[func_name](args)
    else:
        return {"error": f"Unknown function {func_name}"}
