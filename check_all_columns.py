import asyncpg 
import asyncio 
 
async def check_real_columns(): 
    conn = await asyncpg.connect('postgresql://postgres:ANARIX_AI_2024!@localhost:5432/sales_analytics') 
    for table in ['ad_sales', 'total_sales', 'eligibility']: 
        result = await conn.fetch(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}' ORDER BY ordinal_position") 
        print(f'=== {table} table columns ===') 
        for row in result: 
            print(f'- {row[0]}') 
        print() 
    await conn.close() 
 
asyncio.run(check_real_columns()) 
