import aiohttp
import asyncio
import re


class LLMService:
    def __init__(self):
        self.url = "http://127.0.0.1:11434/api/generate"
        self.model = "llama3.1:8b"
        self.schema = """
### POSTGRESQL DATABASE SCHEMA
You must generate a SQL query that strictly uses ONLY the following 3 tables.

**TABLE 1: ad_sales (3,696 records)**
- `item_id`: TEXT (Primary key for joins)
- `date`: DATE
- `ad_sales`: DECIMAL(15,2) (Revenue from ads)
- `impressions`: INTEGER
- `ad_spend`: DECIMAL(12,2) (Cost of ads)
- `clicks`: INTEGER
- `units_sold`: INTEGER (Units sold via ads)

**TABLE 2: total_sales (702 records)**
- `item_id`: TEXT (Foreign key for joins)
- `date`: DATE
- `total_sales`: DECIMAL(15,2) (Total product revenue)
- `total_units_ordered`: INTEGER (Total units sold)

**TABLE 3: eligibility (4,381 records)**
- `item_id`: TEXT (Foreign key for joins)
- `eligibility_datetime_utc`: TIMESTAMP
- `eligibility`: TEXT (Status: 'eligible' or 'ineligible')
- `message`: TEXT (Reason for eligibility status)
"""
        # Prompt to guide the LLM in generating SQL
        self.prompt_template = """system
You are an expert PostgreSQL analyst. Your ONLY task is to generate an accurate SQL query that directly answers the user's specific question. You MUST NOT generate any other query except the one that answers the exact question asked.


**MANDATORY SQL FORMATTING RULES:**
- Columns in SELECT clause must be ordered logically and clearly (e.g., id fields first, then metrics).
- SQL clauses must appear in this order: SELECT → FROM → WHERE → GROUP BY (if any) → ORDER BY → LIMIT.
- Use proper indentation and line breaks for readability (optional).
- Always include explicit aliases and calculations in SELECT with ROUND(...::numeric, 2) formatting.


**MANDATORY ANALYSIS PROCESS:**
1. READ the user's question word by word
2. IDENTIFY the exact data they want
3. DETERMINE which tables contain that data
4. GENERATE SQL that retrieves EXACTLY what they asked for

**COMPLETE BUSINESS METRIC DEFINITIONS:**

**ROI (Return on Investment):**
- Formula: ((total_sales - ad_spend) / NULLIF(ad_spend, 0)) * 100
- Requires: ad_sales table JOIN total_sales table
- Returns: Percentage profit on investment

**ROAS (Return on Ad Spend):**
- Formula: ad_sales / NULLIF(ad_spend, 0)
- Requires: ad_sales table only
- Returns: Revenue multiple from ad spend

**CPC (Cost Per Click):**
- Formula: ad_spend / NULLIF(clicks, 0)
- Requires: ad_sales table only
- Returns: Cost per click in dollars

**CTR (Click-Through Rate):**
- Formula: (clicks / NULLIF(impressions, 0)) * 100
- Requires: ad_sales table only
- Returns: Click rate as percentage

**Revenue Metrics:**
- Total Revenue = SUM(total_sales) from total_sales table
- Advertising Revenue = SUM(ad_sales) from ad_sales table
- Ad Spend = SUM(ad_spend) from ad_sales table

**QUESTION INTERPRETATION RULES:**

1. **"Which product" or "What product"** → Find specific item_id with requested metric
2. **"Top X" or "First X" or "X highest/lowest"** → Use ORDER BY + LIMIT X
3. **"Calculate" or "Show me" without limit** → Show ALL results, NO LIMIT
4. **"Total" or "Sum"** → Use SUM() aggregation
5. **"Average"** → Use AVG() aggregation
6. **"Highest" or "Maximum"** → Use MAX() or ORDER BY DESC LIMIT 1
7. **"Lowest" or "Minimum"** → Use MIN() or ORDER BY ASC LIMIT 1
8. **"Compare" or "Between"** → Show multiple items for comparison
9. **"All products"** → Return all relevant records, NO LIMIT

**EXACT QUERY MATCHING EXAMPLES:**

Example 1 - Highest/Maximum Questions:
SELECT item_id, ad_spend, clicks, ROUND((ad_spend / NULLIF(clicks, 0))::numeric, 2) as cpc FROM ad_sales WHERE clicks > 0 ORDER BY cpc DESC LIMIT 1

Example 2 - Top N Questions:
SELECT item_id, ad_sales, ad_spend, ROUND((ad_sales / NULLIF(ad_spend, 0))::numeric, 2) as roas FROM ad_sales WHERE ad_spend > 0 ORDER BY roas DESC LIMIT 5

Example 3 - Calculate/Show Questions (No Limit):
SELECT item_id, ad_sales, ad_spend, ROUND((ad_sales / NULLIF(ad_spend, 0))::numeric, 2) as roas FROM ad_sales WHERE ad_spend > 0 ORDER BY item_id

Example 4 - ROI Questions:
SELECT a.item_id, t.total_sales, a.ad_spend, ROUND(((t.total_sales - a.ad_spend) / NULLIF(a.ad_spend, 0))::numeric * 100, 2) as roi_percentage FROM ad_sales a LEFT JOIN total_sales t ON a.item_id = t.item_id WHERE a.ad_spend > 0 AND t.total_sales IS NOT NULL ORDER BY roi_percentage DESC

Example 5 - Total/Sum Questions:
SELECT SUM(total_sales) as total_revenue FROM total_sales

**MANDATORY VALIDATION CHECKLIST:**
Before generating ANY query, check:
1. Does my SQL directly answer the user's exact question?
2. Am I using the correct metric formula for what they asked?
3. If they said "top X", do I have LIMIT X?
4. If they said "calculate" or "show", do I have NO LIMIT?
5. Am I using only the 3 allowed tables?
6. Do I have NULLIF protection for divisions?

**DATABASE SCHEMA:**
{schema}

**CRITICAL: Your response must contain ONLY a SQL code block. No explanations, no text before or after.**

User Question: "{question}"
"""

    async def convert_to_sql(self, question: str) -> str:
        print(f"🔍 GENERATING EXACT SQL FOR: {question}")
        prompt = self.prompt_template.format(schema=self.schema, question=question)

        try:
            async with aiohttp.ClientSession() as session:
                payload = {
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.0,
                        "top_p": 0.7,
                        "num_predict": 2048,
                        "repeat_penalty": 1.1,
                        "num_ctx": 20480,
                        "stop": ["```\n\n", "<|eot_id|>", "User Question:", "Example"]
                    }
                }

                print("🚀 REQUESTING EXACT SQL GENERATION...")
                async with session.post(self.url, json=payload, timeout=240) as response:
                    if response.status == 200:
                        data = await response.json()
                        llama_response = data.get("response", "")
                        print(f"📝 LLAMA RESPONSE: {llama_response[:200]}...")

                        sql = self._extract_and_validate_sql(llama_response, question)

                        if sql and self._validate_only_3_tables(sql):
                            print(f"✅ EXACT SQL GENERATED")
                            return sql
                        else:
                            print("⚠️ Using question-specific fallback")
                            return self._get_question_specific_fallback(question)
                    else:
                        print(f"❌ HTTP Error: {response.status}")
                        return self._get_question_specific_fallback(question)

        except Exception as e:
            print(f"❌ Error: {e}")
            return self._get_question_specific_fallback(question)

    def _extract_and_validate_sql(self, response: str, question: str) -> str:
        patterns = [
            r"(SELECT[\s\S]*?;)",
            r"(SELECT[\s\S]*?)(?:\n\n|\Z)"
        ]

        for pattern in patterns:
            match = re.search(pattern, response, re.DOTALL | re.IGNORECASE)
            if match:
                sql_query = match.group(1).strip()
                if sql_query.upper().startswith('SELECT'):
                    if sql_query.endswith(';'):
                        sql_query = sql_query[:-1]
                    if self._validate_question_match(sql_query, question):
                        return sql_query
        print("❌ No valid SQL found or SQL doesn't match question intent")
        return ""

    def _validate_question_match(self, sql: str, question: str) -> bool:
        sql_lower = sql.lower()
        question_lower = question.lower()
        validation_errors = []

        if any(term in question_lower for term in ['roi', 'return on investment']):
            if 'total_sales' not in sql_lower:
                validation_errors.append("ROI question requires total_sales table")
            if 'ad_sales /' in sql_lower and 'total_sales -' not in sql_lower:
                validation_errors.append("ROI calculation incorrect - using ROAS formula")

        if any(term in question_lower for term in ['roas', 'return on ad spend']):
            if 'ad_sales /' not in sql_lower:
                validation_errors.append("ROAS question requires ad_sales/ad_spend calculation")

        if any(term in question_lower for term in ['cpc', 'cost per click']):
            if 'ad_spend /' not in sql_lower or 'clicks' not in sql_lower:
                validation_errors.append("CPC question requires ad_spend/clicks calculation")

        if any(term in question_lower for term in ['total revenue', 'total sales']):
            if 'sum(' not in sql_lower:
                validation_errors.append("Total question requires SUM aggregation")

        limit_patterns = [
            (r'top (\d+)', 'top'),
            (r'first (\d+)', 'first'),
            (r'(\d+) highest', 'highest'),
            (r'(\d+) lowest', 'lowest')
        ]

        for pattern, description in limit_patterns:
            match = re.search(pattern, question_lower)
            if match:
                requested_num = int(next(g for g in match.groups() if g))
                sql_limit = re.search(r'limit\s+(\d+)', sql_lower)
                if not sql_limit or int(sql_limit.group(1)) != requested_num:
                    validation_errors.append(f"Expected LIMIT {requested_num} for '{description}'")

        if any(ind in question_lower for ind in ['calculate', 'show me all', 'list all', 'display']):
            if 'limit' in sql_lower and not any(k in question_lower for k in ['top', 'first', 'highest', 'lowest']):
                validation_errors.append("Query has LIMIT but question did not request it")

        if validation_errors:
            print(f"❌ Validation errors: {validation_errors}")
            return False

        return True

    def _validate_only_3_tables(self, sql: str) -> bool:
        if not sql:
            return False

        sql_upper = sql.upper()
        required_tables = ['AD_SALES', 'TOTAL_SALES', 'ELIGIBILITY']
        forbidden_tables = [
            'SALES_DATA', 'PRODUCT', 'CAMPAIGN', 'USER', 'CUSTOMER', 'CLIENT',
            'ORDER', 'TRANSACTION', 'INVENTORY', 'CATEGORY', 'BRAND', 'ITEM',
            'PERFORMANCE', 'METRICS', 'ANALYSIS', 'REPORT', 'DASHBOARD',
            'BUSINESS', 'REVENUE', 'PROFIT', 'COST', 'EXPENSE', 'TOTAL_SALE'
        ]

        for forbidden in forbidden_tables:
            if f' {forbidden} ' in f' {sql_upper} ' or f'{forbidden}.' in sql_upper:
                print(f"❌ Forbidden table: {forbidden}")
                return False

        return any(t in sql_upper for t in required_tables)

    def _get_question_specific_fallback(self, question: str) -> str:
        q = question.lower()

        if 'cpc' in q or 'cost per click' in q:
            if 'highest' in q:
                return (
                    "SELECT item_id, ad_spend, clicks, "
                    "ROUND((ad_spend / NULLIF(clicks, 0))::numeric, 2) as cpc "
                    "FROM ad_sales WHERE clicks > 0 ORDER BY cpc DESC LIMIT 1"
                )
            elif 'lowest' in q:
                return (
                    "SELECT item_id, ad_spend, clicks, "
                    "ROUND((ad_spend / NULLIF(clicks, 0))::numeric, 2) as cpc "
                    "FROM ad_sales WHERE clicks > 0 ORDER BY cpc ASC LIMIT 1"
                )
            else:
                return (
                    "SELECT item_id, ad_spend, clicks, "
                    "ROUND((ad_spend / NULLIF(clicks, 0))::numeric, 2) as cpc "
                    "FROM ad_sales WHERE clicks > 0 ORDER BY item_id"
                )

        elif 'roi' in q:
            return (
                "SELECT a.item_id, t.total_sales, a.ad_spend, "
                "ROUND(((t.total_sales - a.ad_spend) / NULLIF(a.ad_spend, 0))::numeric * 100, 2) as roi_percentage "
                "FROM ad_sales a LEFT JOIN total_sales t ON a.item_id = t.item_id "
                "WHERE a.ad_spend > 0 AND t.total_sales IS NOT NULL ORDER BY roi_percentage DESC"
            )

        elif 'roas' in q:
            return (
                "SELECT item_id, ad_sales, ad_spend, "
                "ROUND((ad_sales / NULLIF(ad_spend, 0))::numeric, 2) as roas "
                "FROM ad_sales WHERE ad_spend > 0 ORDER BY roas DESC"
            )

        elif 'total revenue' in q or 'total sales' in q:
            return "SELECT SUM(total_sales) as total_revenue FROM total_sales"

        elif 'ad spend' in q:
            return "SELECT SUM(ad_spend) as total_ad_spend FROM ad_sales"

        # Default fallback: Return ROAS top 20 products
        return (
            "SELECT a.item_id, a.ad_sales, a.ad_spend, t.total_sales, "
            "ROUND((a.ad_sales / NULLIF(a.ad_spend, 0))::numeric, 2) as roas "
            "FROM ad_sales a LEFT JOIN total_sales t ON a.item_id = t.item_id "
            "WHERE a.ad_spend > 0 ORDER BY roas DESC LIMIT 20"
        )


async def main():
    llm_service = LLMService()
    test_questions = [
        "Which product had the highest CPC (Cost Per Click)?",
        "Show me top 5 products with highest ROI",
        "Calculate ROAS for all products",
        "What's our total revenue?",
        "Which product has the lowest ROAS?",
        "Show me top 10 products by total sales",
        "What's the average ad spend per product?"
    ]

    for question in test_questions:
        print(f"\n{'='*80}\n🤔 QUESTION: {question}\n{'='*80}")
