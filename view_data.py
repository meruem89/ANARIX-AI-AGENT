import asyncpg 
import asyncio 
 
async def view_excel_data(): 
    conn = await asyncpg.connect('postgresql://postgres:ANARIX_AI_2024!@localhost:5432/sales_analytics') 
    print('=== AD SALES DATA (First 10 records) ===') 
    result = await conn.fetch('SELECT * FROM ad_sales LIMIT 10') 
    for row in result: 
        print(dict(row)) 
    print() 
    print('=== TOTAL SALES DATA (First 10 records) ===') 
    result = await conn.fetch('SELECT * FROM total_sales LIMIT 10') 
    for row in result: 
        print(dict(row)) 
    print() 
    print('=== ELIGIBILITY DATA (First 10 records) ===') 
    result = await conn.fetch('SELECT * FROM eligibility LIMIT 10') 
    for row in result: 
        print(dict(row)) 
    await conn.close() 
 
asyncio.run(view_excel_data()) 
