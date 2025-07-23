import asyncpg 
import asyncio 
 
async def show_existing_connections(): 
    conn = await asyncpg.connect('postgresql://postgres:ANARIX_AI_2024!@localhost:5432/sales_analytics') 
    print('?? YOUR EXCEL DATA IS ALREADY CONNECTED IN POSTGRESQL!') 
    print('=' * 60) 
    print() 
    print('?? EXISTING CONNECTED QUERY EXAMPLE:') 
    result = await conn.fetch('SELECT a.item_id, a.ad_sales, t.total_sales, e.eligibility FROM ad_sales a JOIN total_sales t ON a.item_id = t.item_id JOIN eligibility e ON a.item_id = e.item_id LIMIT 5') 
    for i, row in enumerate(result, 1): 
        print(f'  {i}. Product {row[0]}:') 
        print(f'     Ad Sales: ${row[1]}, Total Sales: ${row[2]}, Status: {row[3]}') 
    print() 
    print('? ALL YOUR EXCEL TABLES ARE READY FOR CROSS-TABLE QUERIES!') 
    await conn.close() 
 
asyncio.run(show_existing_connections()) 
