import logging
import re

class AskingProcess:

    # =====================================================================
    # 4️⃣ Build final conversation prompt for response generation
    # =====================================================================
    @staticmethod
    def build_conversation_prompt(table_string):
        """
        Build the full LLM prompt used for answer generation.

        This merges:
          - prior conversation turns (to maintain context continuity)
          - retrieved document contexts (semantic + metadata)
          - the user’s final question

        Args:
            table_string (str): stock analysis data.


        Returns:
            str: Complete prompt string ready to send to the LLM for answer generation.
        """

        prompt_parts = [
            "You are a stock analyst with access to historical stock data in table format. The table contains the following columns:",
            "- time: trading session datetime", "- symbol: stock symbol", "- close: stock close price",
            "- atr_14_w: 14-day Wilder smoothed average true range", "- rsi_14: 14-day relative strength index",
            "- sm_1: 1-month stock momentum", "- sm_3: 3-month stock momentum", "- sm_6: 6-month stock momentum",
            "- sma_20: 20-day simple moving average", "- sma_50: 50-day simple moving average",
            "- vnim_1: 1-month VNINDEX momentum", "- vnim_3: 3-month VNINDEX momentum",
            "- vnim_6: 6-month VNINDEX momentum", "- vol_20d_pct_126: 126-day percentile of 20-day volatility",
            "Your task is to **analyze how each factor behaves and relates to the stock close price**. Focus on identifying **patterns and relationships between the factors and the price** in the following scenarios:",
            "1. When the stock close price starts to increase",
            "2. When the stock close price keeps increasing over a period",
            "3. When the stock close price starts to decrease",
            "For each scenario, provide a **structured analysis**, including:",
            "- Patterns observed for individual factors",
            "- How each factor correlates or influences the stock close price",
            "- Relationships or interactions between factors that appear important in that scenario",
            "- Any notable trends or anomalies",
            "Do not assume which factors are important; let the data reveal the relationships naturally.",
            "\n--- Stock data table  ---\n", table_string]
        # Add retrieved contextual documents

        # Combine everything into a single string
        return "\n".join(prompt_parts)
