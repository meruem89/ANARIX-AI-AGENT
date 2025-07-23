import asyncpg 
import asyncio 

async def create_optimized_view(): 
    conn = await asyncpg.connect('postgresql://postgres:ANARIX_AI_2024!@localhost:5432/sales_analytics') 
    print('🚀 CREATING OPTIMIZED BUSINESS INTELLIGENCE VIEW...') 
    print() 
    
    # Drop existing view
    drop_view = 'DROP VIEW IF EXISTS business_intelligence_complete' 
    await conn.execute(drop_view) 
    
    # Create clean view that provides data access without hardcoded calculations
    create_view = '''
    CREATE VIEW business_intelligence_complete AS 
    SELECT 
        a.item_id,
        a.date as ad_date,
        t.date as sales_date,
        a.ad_sales,
        a.impressions,
        a.ad_spend,
        a.clicks,
        a.units_sold,
        t.total_sales,
        t.total_units_ordered,
        e.eligibility_datetime_utc,
        e.eligibility,
        e.message
    FROM ad_sales a
    FULL OUTER JOIN total_sales t ON a.item_id = t.item_id
    FULL OUTER JOIN eligibility e ON a.item_id = e.item_id
    '''
    
    await conn.execute(create_view) 
    print('✅ OPTIMIZED BUSINESS INTELLIGENCE VIEW CREATED!') 
    print() 
    
    count = await conn.fetchval('SELECT COUNT(*) FROM business_intelligence_complete') 
    print(f'📊 VIEW CONTAINS: {count} comprehensive records') 
    print() 
    
    print('🎯 VIEW CAPABILITIES:') 
    print('  ✅ All Excel data from 3 sheets combined') 
    print('  ✅ Optimized for LLaMA SQL generation') 
    print('  ✅ No hardcoded calculations - LLaMA decides') 
    print('  ✅ Cross-table product analysis ready')
    print() 
    
    print('🚀 LLAMA CAN NOW GENERATE QUERIES LIKE:')
    print('  • SELECT item_id, ad_sales + total_sales as combined_revenue FROM view')
    print('  • SELECT *, ad_sales/NULLIF(ad_spend,0) as roas FROM view WHERE ad_spend > 0')
    print('  • SELECT *, ad_spend/NULLIF(clicks,0) as cpc FROM view WHERE clicks > 0')
    print('  • Complex CTEs, window functions, statistical analysis, etc.')
    print() 
    
    print('📋 SAMPLE DATA STRUCTURE:') 
    sample = await conn.fetch('''
        SELECT item_id, ad_sales, total_sales, ad_spend, clicks, eligibility 
        FROM business_intelligence_complete 
        WHERE (ad_sales IS NOT NULL OR total_sales IS NOT NULL)
        LIMIT 5
    ''') 
    
    for i, row in enumerate(sample, 1): 
        print(f'  {i}. Product {row["item_id"]}: Ad Sales ${row["ad_sales"] or 0:.2f}, '
              f'Total Sales ${row["total_sales"] or 0:.2f}, Status: {row["eligibility"]}') 
    
    print()
    print('🎯 READY FOR PURE LLAMA 3.1 GENERATION!')
    print('   • LLaMA will calculate RoAS, CPC, combined metrics as needed')
    print('   • No pre-computed values restricting AI flexibility')
    print('   • Optimized data access for unlimited SQL complexity')
    
    await conn.close() 

asyncio.run(create_optimized_view())
