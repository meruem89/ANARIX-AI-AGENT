import asyncio
import asyncpg
from datetime import datetime, timedelta
import random
from decimal import Decimal
import os
from pathlib import Path

# Add the parent directory to the path to import from services
import sys
sys.path.append(str(Path(__file__).parent.parent))

async def create_connection():
    """Create async PostgreSQL connection"""
    try:
        connection = await asyncpg.connect(
            host='localhost',
            port=5432,
            user='postgres',
            password='ANARIX_AI_2024!',
            database='sales_analytics'
        )
        return connection
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        return None

async def ensure_correct_schema(connection):
    """Ensure your existing 3-table schema exists"""
    
    # Create ad_sales table (matches your models.py)
    await connection.execute("""
        CREATE TABLE IF NOT EXISTS ad_sales (
            id SERIAL PRIMARY KEY,
            item_id VARCHAR(100),
            date DATE,
            ad_sales DECIMAL(15,2) DEFAULT 0,
            impressions INTEGER DEFAULT 0,
            ad_spend DECIMAL(12,2) DEFAULT 0,
            clicks INTEGER DEFAULT 0,
            units_sold INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    
    # Create total_sales table (matches your models.py)  
    await connection.execute("""
        CREATE TABLE IF NOT EXISTS total_sales (
            id SERIAL PRIMARY KEY,
            item_id VARCHAR(100),
            date DATE,
            total_sales DECIMAL(15,2) DEFAULT 0,
            total_units_ordered INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    
    # Create eligibility table (matches your models.py)
    await connection.execute("""
        CREATE TABLE IF NOT EXISTS eligibility (
            id SERIAL PRIMARY KEY,
            item_id VARCHAR(100),
            eligibility_datetime_utc TIMESTAMP,
            eligibility VARCHAR(50),
            message TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    
    print("✅ Verified correct 3-table schema exists")

async def populate_sample_data_for_testing():
    """Generate sample data for your existing 3-table schema (testing only)"""
    
    connection = await create_connection()
    if not connection:
        return False
    
    try:
        # Ensure correct schema exists
        await ensure_correct_schema(connection)
        
        print("📊 Generating sample data for testing (if no real data exists)...")
        
        # Check if real data already exists
        ad_sales_count = await connection.fetchval("SELECT COUNT(*) FROM ad_sales")
        total_sales_count = await connection.fetchval("SELECT COUNT(*) FROM total_sales")
        eligibility_count = await connection.fetchval("SELECT COUNT(*) FROM eligibility")
        
        if ad_sales_count > 0 or total_sales_count > 0 or eligibility_count > 0:
            print("✅ Real data already exists - skipping sample data generation")
            print(f"   📈 ad_sales: {ad_sales_count} records")
            print(f"   💰 total_sales: {total_sales_count} records") 
            print(f"   ✅ eligibility: {eligibility_count} records")
            return True
        
        print("🔧 No real data found - generating minimal test data...")
        
        # Generate minimal sample data for testing only
        sample_products = ['TEST_PRODUCT_001', 'TEST_PRODUCT_002', 'TEST_PRODUCT_003']
        base_date = datetime.now() - timedelta(days=30)
        
        # Insert minimal ad_sales test data
        for i, product in enumerate(sample_products):
            await connection.execute("""
                INSERT INTO ad_sales (item_id, date, ad_sales, impressions, ad_spend, clicks, units_sold)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            """, 
                product,
                base_date + timedelta(days=i),
                random.uniform(100, 1000),  # Simple test values
                random.randint(100, 1000),
                random.uniform(50, 500),
                random.randint(10, 100),
                random.randint(1, 20)
            )
        
        # Insert minimal total_sales test data
        for i, product in enumerate(sample_products):
            await connection.execute("""
                INSERT INTO total_sales (item_id, date, total_sales, total_units_ordered)
                VALUES ($1, $2, $3, $4)
            """, 
                product,
                base_date + timedelta(days=i),
                random.uniform(200, 2000),  # Simple test values
                random.randint(5, 50)
            )
        
        # Insert minimal eligibility test data
        for i, product in enumerate(sample_products):
            await connection.execute("""
                INSERT INTO eligibility (item_id, eligibility_datetime_utc, eligibility, message)
                VALUES ($1, $2, $3, $4)
            """, 
                product,
                datetime.now(),
                'eligible',
                'Test product - generated for development'
            )
        
        print(f"✅ Generated minimal test data for {len(sample_products)} products")
        
        # Verify test data
        ad_count = await connection.fetchval("SELECT COUNT(*) FROM ad_sales")
        sales_count = await connection.fetchval("SELECT COUNT(*) FROM total_sales")
        elig_count = await connection.fetchval("SELECT COUNT(*) FROM eligibility")
        
        print(f"\n📊 Test Data Summary:")
        print(f"   📈 ad_sales: {ad_count} records")
        print(f"   💰 total_sales: {sales_count} records")
        print(f"   ✅ eligibility: {elig_count} records")
        
        return True
        
    except Exception as e:
        print(f"❌ Error generating sample data: {e}")
        return False
    finally:
        await connection.close()

async def main():
    """Main execution function"""
    print("🚀 ANARIX AI Agent - Test Data Generation")
    print("=" * 50)
    print("⚠️  This generates minimal test data ONLY if no real data exists")
    print("⚠️  Use your Excel integration scripts for real data loading")
    
    success = await populate_sample_data_for_testing()
    
    if success:
        print("\n✅ Test data check completed!")
        print("\n🎯 Next steps for real data:")
        print("1. Use: python database/excel_to_sql_converter.py")
        print("2. Use: python create_business_view.py") 
        print("3. Test: uvicorn app.main:app --reload")
        print("4. Your LLaMA 3.1 system will work with either test or real data")
    else:
        print("\n❌ Test data generation failed!")

if __name__ == "__main__":
    asyncio.run(main())
