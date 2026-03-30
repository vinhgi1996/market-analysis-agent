from datetime import datetime
from decimal import Decimal

from dotenv import load_dotenv
from fastapi import APIRouter
import logging
from entity.query import Query
from business_logic.agent_processing.asking_process import AskingProcess
from constant.sql.analysis.ohlvc_agent_analysis import OhlvcAgentAnalysisQueries
from util.models_util import ModelUtil
from util.postgre_sql import PostgresSQLUtil

import json

load_dotenv()

from google import genai

# Initialize the client
client = genai.Client(api_key="AIzaSyD7mitJjWxpBoPpfDC3vlFwGQ-IgJY7rtI")

# -------------------------
# Setup
# -------------------------

router = APIRouter()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# -------------------------

# Function to convert data to JSON-serializable format
def serialize_for_prompt(data):
    serialized = []
    for row in data:
        new_row = {}
        for key, value in row.items():
            if isinstance(value, Decimal):
                new_row[key] = float(value)  # Convert Decimal to float
            elif isinstance(value, datetime):
                new_row[key] = value.isoformat()  # Convert datetime to ISO string
            else:
                new_row[key] = value
        serialized.append(new_row)
    return serialized


# Main endpoint
@router.post("/v1/ask")
async def ask(query: Query):
    try:
        contexts = PostgresSQLUtil.run_sql(
            OhlvcAgentAnalysisQueries.GET_DATA_BY_DATE_RANGE,
            (query.date,query.symbol),
        )
        print(contexts)
        # # Get the serialized version
        # prompt_context = serialize_for_prompt(contexts)
        #
        # prompt_string = json.dumps(prompt_context, indent=2)
        #
        # print(prompt_string)

        columns = ['time', 'symbol', 'close', 'atr_14_w', 'rsi_14', 'sm_1', 'sm_3', 'sm_6',
                   'sma_20', 'sma_50', 'vnim_1', 'vnim_3', 'vnim_6', 'vol_20d_pct_126']

        # Compute column widths for alignment
        col_widths = {col: max(len(col), *(
        len(f"{row[col].isoformat()}" if isinstance(row[col], datetime) else f"{float(row[col]):.4f}" if isinstance(
            row[col], Decimal) else str(row[col])) for row in contexts))
                      for col in columns}

        # Build header
        header = " | ".join(col.ljust(col_widths[col]) for col in columns)
        separator = "-+-".join("-" * col_widths[col] for col in columns)

        # Build rows
        rows = []
        for row in contexts:
            row_str = " | ".join(
                f"{row[col].isoformat()}" if isinstance(row[col], datetime) else
                f"{float(row[col]):.4f}" if isinstance(row[col], Decimal) else str(row[col])
                for col in columns
            )
            # Align each cell to column width
            row_cells = [row_str.split(" | ")[i].ljust(col_widths[columns[i]]) for i in range(len(columns))]
            rows.append(" | ".join(row_cells))

        # Join everything into the final table string
        table_string = "\n".join([header, separator] + rows)

        print(table_string)

        # Count tokens **without sending the prompt to the model**
        token_count = client.models.count_tokens(
            model="gemini-3-pro-preview",
            contents=table_string
        )

        print(f"Your prompt will use {token_count} tokens")
        prompt = AskingProcess.build_conversation_prompt(table_string)


        gen_resp = await ModelUtil.gemini_generate_answer(prompt)
        answer = gen_resp.text

        return {
            "answer": answer
        }

    except Exception as e:
        logger.error("[ERROR] /ask failed: %s", e)
        return "Agent is busy please try again later"

