import asyncpg 
import asyncio 
 
async def show_database_tables(): 
    conn = await asyncpg.connect('postgresql://postgres:ANARIX_AI_2024!@localhost:5432/sales_analytics') 
    print('=== YOUR POSTGRESQL DATABASE TABLES ===') 
    print() 
    result = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'") 
    print('Tables in your database:') 
    for row in result: 
        print(f'- {row[0]}') 
    print() 
    for table in ['ad_sales', 'total_sales', 'eligibility']: 
        print(f'=== {table.upper()} TABLE STRUCTURE ===') 
        columns = await conn.fetch(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}' ORDER BY ordinal_position") 
        for col in columns: 
            print(f'  {col[0]} ({col[1]})') 
        count = await conn.fetchval(f'SELECT COUNT(*) FROM {table}') 
        print(f'  Records: {count}') 
        print() 
    await conn.close() 
 
asyncio.run(show_database_tables()) 
