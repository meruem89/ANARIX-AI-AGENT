import asyncpg 
import asyncio 
 
async def show_complete_connected_database(): 
    conn = await asyncpg.connect('postgresql://postgres:ANARIX_AI_2024!@localhost:5432/sales_analytics') 
    print('??? COMPLETE EXCEL-TO-POSTGRESQL DATABASE VIEW') 
    print('=' * 60) 
    print() 
    print('?? ALL TABLES CONNECTED VIA item_id:') 
    print() 
    for table in ['ad_sales', 'total_sales', 'eligibility']: 
        print(f'?? {table.upper()} TABLE:') 
        columns = await conn.fetch(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}' ORDER BY ordinal_position") 
        for col in columns: 
            highlight = ' ? JOINING KEY' if col[0] == 'item_id' else '' 
            print(f'  - {col[0]} ({col[1]}){highlight}') 
        count = await conn.fetchval(f'SELECT COUNT(*) FROM {table}') 
        unique_items = await conn.fetchval(f'SELECT COUNT(DISTINCT item_id) FROM {table}') 
        print(f'  ?? Records: {count} | Unique Products: {unique_items}') 
        print() 
    print('?? CROSS-TABLE RELATIONSHIPS:') 
    print('  ad_sales.item_id  total_sales.item_id  eligibility.item_id') 
    print() 
    common_items = await conn.fetchval('SELECT COUNT(DISTINCT a.item_id) FROM ad_sales a INNER JOIN total_sales t ON a.item_id = t.item_id INNER JOIN eligibility e ON a.item_id = e.item_id') 
    print(f'?? PRODUCTS WITH COMPLETE DATA ACROSS ALL TABLES: {common_items}') 
    print() 
    print('?? SAMPLE CONNECTED DATA (Top 5 products):') 
    result = await conn.fetch('''SELECT a.item_id, a.ad_sales, a.ad_spend, t.total_sales, e.eligibility FROM ad_sales a INNER JOIN total_sales t ON a.item_id = t.item_id INNER JOIN eligibility e ON a.item_id = e.item_id LIMIT 5''') 
    for i, row in enumerate(result, 1): 
        print(f'  {i}. Product {row["item_id"]}:') 
        print(f'     Ad Sales: ${row["ad_sales"]:.2f}, Ad Spend: ${row["ad_spend"]:.2f}') 
        print(f'     Total Sales: ${row["total_sales"]:.2f}, Status: {row["eligibility"]}') 
    await conn.close() 
 
asyncio.run(show_complete_connected_database()) 
