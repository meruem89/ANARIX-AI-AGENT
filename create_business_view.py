import asyncpg 
import asyncio 
 
async def create_connected_view(): 
    conn = await asyncpg.connect('postgresql://postgres:ANARIX_AI_2024!@localhost:5432/sales_analytics') 
    print('?? CREATING CONNECTED BUSINESS INTELLIGENCE VIEW...') 
    print() 
    drop_view = 'DROP VIEW IF EXISTS business_intelligence_complete' 
    await conn.execute(drop_view) 
    create_view = '''CREATE VIEW business_intelligence_complete AS SELECT a.item_id, a.date as ad_date, t.date as sales_date, a.ad_sales, a.impressions, a.ad_spend, a.clicks, a.units_sold, t.total_sales, t.total_units_ordered, e.eligibility_datetime_utc, e.eligibility, e.message, (a.ad_sales + t.total_sales) as combined_revenue, CASE WHEN a.ad_spend  THEN (a.ad_sales / a.ad_spend) ELSE 0 END as roas, CASE WHEN a.clicks  THEN (a.ad_spend / a.clicks) ELSE 0 END as cpc FROM ad_sales a FULL OUTER JOIN total_sales t ON a.item_id = t.item_id FULL OUTER JOIN eligibility e ON a.item_id = e.item_id''' 
    await conn.execute(create_view) 
    print('? BUSINESS INTELLIGENCE VIEW CREATED!') 
    print() 
    count = await conn.fetchval('SELECT COUNT(*) FROM business_intelligence_complete') 
    print(f'?? CONNECTED VIEW CONTAINS: {count} comprehensive records') 
    print() 
    print('?? VIEW CAPABILITIES:') 
    print('  - All Excel data from 3 sheets combined') 
    print('  - Automatic RoAS calculation') 
    print('  - Automatic CPC calculation') 
    print('  - Combined revenue tracking') 
    print('  - Cross-table product analysis') 
    print() 
    print('?? SAMPLE CONNECTED BUSINESS DATA:') 
    sample = await conn.fetch('SELECT item_id, combined_revenue, roas, cpc, eligibility FROM business_intelligence_complete WHERE combined_revenue  LIMIT 3') 
    for i, row in enumerate(sample, 1): 
        print(f'  {i}. {row["item_id"]}: Revenue ${row["combined_revenue"]:.2f}, RoAS {row["roas"]:.2f}, CPC ${row["cpc"]:.2f}, Status: {row["eligibility"]}') 
    await conn.close() 
 
asyncio.run(create_connected_view()) 
