from flask import jsonify
from google.cloud import bigquery
from datetime import datetime

import anthropic
import functions_framework
import json
import os

client = anthropic.Client(
    api_key=os.environ.get('ANTHROPIC_API_KEY')
)
bigquery_client = bigquery.Client()

dataset_id = os.environ.get('BIGQUERY_DATASET_ID')
table_id = os.environ.get('BIGQUERY_TABLE_ID')
table_ref = f"{bigquery_client.project}.{dataset_id}.{table_id}"

@functions_framework.http
def create_record(request):
    """
    Extract order details and save them to a BigQuery table
    """

    request_json = request.get_json()

    if not request_json:
        return "Order JSON payload is missing", 400
    
    order = request_json["order"]
    prompt = f"""
    You are an expert email order reader, who needs to extract order information from customer emails.
    You MUST return your extraction in this EXACT format, as it will be ingested into a data analytics tool:
    {{
        "customer": "<the customer making the order>",
        "product": "<the product being ordered>",
        "quantity": "<the quantity of the product being ordered>",
    }}

    If you do not know the customer, product, or quantity, return a null value.
    Use only the information explicitly written in the order.

    Extract the customer, product, and quantity from the following order:
    "{order}"
    """

    message = client.messages.create(
        model="claude-3-5-sonnet-20241022",
        max_tokens=4096,
        temperature=0.3,
        messages=[
            {
                "role": "user",
                "content": prompt
            }
        ],
        tools=[{
            "name": "order_extractor",
            "description": "Extract the order using well-structured JSON.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "customer": {"type": "string", "description": "The person placing the order"},
                    "product": {"type": "string", "description": "The type/name of product being ordered"},
                    "quantity": {"type": "number", "description": "The quantity of product being ordered"},
                },
                "required": ["customer"],
            }
        }],
        tool_choice={"type": "tool", "name": "order_extractor"}
    )

    extracted_order = message.content[0].input

    row = {
        "timestamp": datetime.utcnow().isoformat(),
        "email": order,
        "customer": extracted_order["customer"],
        "product": extracted_order["product"],
        "quantity": extracted_order["quantity"],
    }

    bigquery_client.insert_rows_json(
        table_ref,
        [row]
    )

    return jsonify(extracted_order), 200

