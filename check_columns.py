import asyncpg 
import asyncio 
 
async def check_columns(): 
    conn = await asyncpg.connect('postgresql://postgres:ANARIX_AI_2024!@localhost:5432/sales_analytics') 
    result = await conn.fetch("SELECT column_name FROM information_schema.columns WHERE table_name = 'ad_sales' ORDER BY ordinal_position") 
    print('=== ad_sales table columns ===') 
    for row in result: 
        print(f'- {row[0]}') 
    await conn.close() 
 
asyncio.run(check_columns()) 
