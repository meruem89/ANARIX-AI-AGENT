from fastapi import FastAPI, HTTPException
import asyncpg
from services.response_formatter import ResponseFormatter
from services.llm_service import LLMService
from services.visualization_service import VisualizationService
from data.mock_data import MockDataService
from database.connection import create_tables
import uvicorn

# ────────────────────────────
# App & Service Initialisation
# ────────────────────────────
app = FastAPI(title="ANARIX AI Agent – Pure LLaMA System", version="4.0.0")

formatter = ResponseFormatter()
llm_service = LLMService()  # Pure LLaMA 3.1 service
viz_service = VisualizationService()
mock_data = MockDataService()

# create tables (noop if they already exist)
create_tables()

# ────────────────────────────
# Helper: async PG connection
# ────────────────────────────
async def get_pg_connection():
    try:
        return await asyncpg.connect(
            host="localhost",
            port=5432,
            user="postgres",
            password="ANARIX_AI_2024!",
            database="sales_analytics"
        )
    except Exception as e:
        print("❌ PostgreSQL connection failed:", e)
        return None

# ────────────────────────────
# Health-check
# ────────────────────────────
@app.get("/")
async def health():
    return {
        "status": "ANARIX AI Agent - Pure LLaMA 3.1",
        "version": "4.0.0",
        "ai_model": "LLaMA 3.1 8B - Zero Patterns",
        "mode": "Pure AI Generation",
        "endpoints": ["/query", "/visualize/total-sales", "/visualize/roas", "/visualize/highest-cpc"]
    }

# ────────────────────────────
# PURE LLAMA 3.1 ENDPOINT - NO PATTERN MATCHING
# ────────────────────────────
@app.post("/query")
async def pure_llama_query(request: dict):
    """
    Pure LLaMA 3.1 SQL generation - zero pattern matching
    LLaMA decides everything based on user question and PostgreSQL data
    """
    question = request.get("question", "").strip()
    if not question:
        return {"success": False, "error": "Empty question"}

    print(f"🔍 PURE LLAMA PROCESSING: {question}")

    # 1. Let LLaMA generate SQL directly - NO PATTERN CHECKING
    try:
        sql = await llm_service.convert_to_sql(question)
        print(f"🤖 LLAMA GENERATED: {sql}")
    except Exception as e:
        return {"success": False, "error": f"LLaMA error: {e}"}

    if not sql or sql.strip() == "":
        return {"success": False, "error": "No SQL generated"}

    # 2. Execute whatever LLaMA generated - NO CLASSIFICATION
    conn = await get_pg_connection()
    if not conn:
        return {"success": False, "error": "Database connection failed"}

    try:
        # Try different execution methods without pattern restrictions
        result_data = None
        query_type = "unknown"
        
        try:
            # Try multi-row result first
            results = await conn.fetch(sql)
            if results:
                result_data = [dict(row) for row in results]
                query_type = "multi_row"
                record_count = len(results)
            else:
                result_data = []
                query_type = "empty_result"
                record_count = 0
        except:
            try:
                # Try single value
                single_result = await conn.fetchval(sql)
                result_data = single_result
                query_type = "single_value"
                record_count = 1 if single_result is not None else 0
            except:
                try:
                    # Try single row
                    row = await conn.fetchrow(sql)
                    if row:
                        result_data = dict(row)
                        query_type = "single_row"
                        record_count = 1
                    else:
                        result_data = None
                        query_type = "no_result"
                        record_count = 0
                except Exception as final_error:
                    await conn.close()
                    return {
                        "success": False,
                        "error": f"Query execution failed: {final_error}",
                        "generated_sql": sql,
                        "question": question
                    }
        
        await conn.close()
        
        # 3. Return pure results without pattern-based formatting
        return {
            "success": True,
            "question": question,
            "result": result_data,
            "query_type": query_type,
            "record_count": record_count,
            "ai_model": "🤖 LLaMA 3.1 8B (Pure Generation)",
            "generated_sql": sql,
            "processing_mode": "zero_patterns"
        }
        
    except Exception as e:
        if conn:
            await conn.close()
        return {
            "success": False,
            "error": f"Database error: {e}",
            "generated_sql": sql,
            "question": question
        }

# ────────────────────────────
# VISUALISATION ENDPOINTS
# ────────────────────────────
@app.get("/visualize/total-sales")
async def visualize_sales():
    conn = await get_pg_connection()
    if not conn:
        data, source = mock_data.get_total_sales(), "Mock"
    else:
        total = await conn.fetchval("SELECT SUM(total_sales) FROM total_sales")
        await conn.close()
        data, source = [{"total_sales": float(total) if total else 0}], "PostgreSQL"

    chart = viz_service.generate_chart("total sales", data, "Total Sales Overview")
    return {"data_source": source, "data": data, "visualization": chart}

@app.get("/visualize/roas")
async def visualize_roas():
    conn = await get_pg_connection()
    if not conn:
        data, source = mock_data.get_roas(), "Mock"
    else:
        row = await conn.fetchrow(
            "SELECT AVG(ad_sales/NULLIF(ad_spend,0)) AS avg_roas, "
            "SUM(ad_sales) AS sales, SUM(ad_spend) AS spend "
            "FROM ad_sales WHERE ad_spend > 0"
        )
        await conn.close()
        data, source = [{
            "avg_roas": float(row["avg_roas"]) if row["avg_roas"] else 0,
            "total_sales": float(row["sales"]) if row["sales"] else 0,
            "total_ad_spend": float(row["spend"]) if row["spend"] else 0
        }], "PostgreSQL"

    chart = viz_service.generate_chart("roas", data, "Return on Ad Spend")
    return {"data_source": source, "data": data, "visualization": chart}

@app.get("/visualize/highest-cpc")
async def visualize_cpc():
    conn = await get_pg_connection()
    if not conn:
        data, source = mock_data.get_highest_cpc_product(), "Mock"
    else:
        rows = await conn.fetch(
            "SELECT item_id, MAX(ad_spend/NULLIF(clicks,0)) AS max_cpc, "
            "AVG(ad_spend/NULLIF(clicks,0)) AS avg_cpc, COUNT(*) AS campaigns "
            "FROM ad_sales WHERE clicks > 0 "
            "GROUP BY item_id ORDER BY max_cpc DESC LIMIT 5"
        )
        await conn.close()
        data = [{"product_name": r["item_id"], "highest_cpc": float(r["max_cpc"]),
                 "avg_cpc": float(r["avg_cpc"]), "campaign_count": r["campaigns"]}
                for r in rows]
        source = "PostgreSQL"

    chart = viz_service.generate_chart("cpc", data, "Products by Cost Per Click")
    return {"data_source": source, "data": data, "visualization": chart}

# ────────────────────────────
# Entry point
# ────────────────────────────
if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
