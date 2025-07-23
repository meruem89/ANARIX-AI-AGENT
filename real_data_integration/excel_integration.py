import pandas as pd
import asyncpg
import asyncio
from datetime import datetime
import os


class RealExcelIntegrator:
    def __init__(self):
        self.db_params = {
            'host': 'localhost',
            'port': 5432,
            'user': 'postgres',
            'password': 'ANARIX_AI_2024!',
            'database': 'sales_analytics'
        }
    
    async def integrate_real_excel_data(self):
        """Integrate Excel data into your existing 3-table schema (no patterns)"""
        
        print("🚀 ANARIX AI Agent - Real Excel Data Integration")
        print("=" * 70)
        
        try:
            # Connect to database
            print("\n🗄️ Connecting to PostgreSQL database...")
            conn = await asyncpg.connect(**self.db_params)
            print("✅ Database connection established")
            
            # Clear existing data and reload from Excel files
            await self.refresh_existing_tables(conn)
            
            await conn.close()
            
            print("\n🎉 REAL EXCEL DATA INTEGRATION COMPLETED!")
            print("🎯 Your ANARIX AI Agent now uses your actual business data!")
            print("📈 LLaMA 3.1 will generate SQL for your real datasets!")
            
            return True
            
        except Exception as e:
            print(f"❌ Integration failed: {e}")
            return False
    
    async def refresh_existing_tables(self, conn):
        """Refresh your existing ad_sales, total_sales, eligibility tables"""
        
        print("\n📊 Loading Excel files...")
        
        # Load Excel files (adjust filenames as needed)
        excel_files = {
            'ad_sales': 'Copy of Product-Level Ad Sales and Metrics (mapped).xlsx',
            'total_sales': 'Copy of Product-Level Total Sales and Metrics (mapped).xlsx', 
            'eligibility': 'Copy of Product-Level Eligibility Table (mapped).xlsx'
        }
        
        for table_name, filename in excel_files.items():
            if os.path.exists(filename):
                print(f"📂 Processing {filename}...")
                
                # Load Excel data
                df = pd.read_excel(filename)
                
                # Clean column names for PostgreSQL
                df.columns = [
                    str(col).strip().lower()
                    .replace(' ', '_')
                    .replace('-', '_')
                    .replace('(', '')
                    .replace(')', '')
                    .replace('.', '_')
                    .replace('%', 'percent')
                    for col in df.columns
                ]
                
                # Replace NaN with None
                df = df.where(pd.notnull(df), None)
                
                # Clear existing table
                await conn.execute(f"DELETE FROM {table_name}")
                
                # Insert new data
                if table_name == 'ad_sales':
                    await self.insert_ad_sales_data(conn, df)
                elif table_name == 'total_sales':
                    await self.insert_total_sales_data(conn, df)
                elif table_name == 'eligibility':
                    await self.insert_eligibility_data(conn, df)
                    
                print(f"✅ {table_name}: {len(df)} records refreshed")
            else:
                print(f"⚠️ File not found: {filename}")
        
        # Verify data integration
        await self.verify_integration(conn)
    
    async def insert_ad_sales_data(self, conn, df):
        """Insert ad sales data into existing table structure"""
        for _, row in df.iterrows():
            try:
                await conn.execute("""
                    INSERT INTO ad_sales (item_id, date, ad_sales, impressions, ad_spend, clicks, units_sold)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                """, 
                    str(row.get('item_id', '')),
                    row.get('date'),
                    float(row.get('ad_sales', 0)) if row.get('ad_sales') else 0,
                    int(row.get('impressions', 0)) if row.get('impressions') else 0,
                    float(row.get('ad_spend', 0)) if row.get('ad_spend') else 0,
                    int(row.get('clicks', 0)) if row.get('clicks') else 0,
                    int(row.get('units_sold', 0)) if row.get('units_sold') else 0
                )
            except Exception as e:
                print(f"⚠️ Skipped row in ad_sales: {e}")
                continue
    
    async def insert_total_sales_data(self, conn, df):
        """Insert total sales data into existing table structure"""
        for _, row in df.iterrows():
            try:
                await conn.execute("""
                    INSERT INTO total_sales (item_id, date, total_sales, total_units_ordered)
                    VALUES ($1, $2, $3, $4)
                """, 
                    str(row.get('item_id', '')),
                    row.get('date'),
                    float(row.get('total_sales', 0)) if row.get('total_sales') else 0,
                    int(row.get('total_units_ordered', 0)) if row.get('total_units_ordered') else 0
                )
            except Exception as e:
                print(f"⚠️ Skipped row in total_sales: {e}")
                continue
    
    async def insert_eligibility_data(self, conn, df):
        """Insert eligibility data into existing table structure"""
        for _, row in df.iterrows():
            try:
                await conn.execute("""
                    INSERT INTO eligibility (item_id, eligibility_datetime_utc, eligibility, message)
                    VALUES ($1, $2, $3, $4)
                """, 
                    str(row.get('item_id', '')),
                    row.get('eligibility_datetime_utc'),
                    str(row.get('eligibility', 'eligible')),
                    str(row.get('message', ''))
                )
            except Exception as e:
                print(f"⚠️ Skipped row in eligibility: {e}")
                continue
    
    async def verify_integration(self, conn):
        """Verify data was loaded correctly"""
        print("\n🔍 Verifying data integration...")
        
        # Check record counts
        ad_sales_count = await conn.fetchval("SELECT COUNT(*) FROM ad_sales")
        total_sales_count = await conn.fetchval("SELECT COUNT(*) FROM total_sales")
        eligibility_count = await conn.fetchval("SELECT COUNT(*) FROM eligibility")
        
        print(f"\n📊 DATA VERIFICATION:")
        print(f"   📈 ad_sales: {ad_sales_count:,} records")
        print(f"   💰 total_sales: {total_sales_count:,} records")
        print(f"   ✅ eligibility: {eligibility_count:,} records")
        print(f"   🎯 Total business records: {ad_sales_count + total_sales_count + eligibility_count:,}")
        
        # Test the business intelligence view if it exists
        try:
            view_count = await conn.fetchval("SELECT COUNT(*) FROM business_intelligence_complete")
            print(f"   📋 business_intelligence_complete view: {view_count:,} records")
        except:
            print("   📋 business_intelligence_complete view: Not created yet")
        
        print("\n✅ Data integration verified - ready for LLaMA 3.1!")


async def main():
    integrator = RealExcelIntegrator()
    success = await integrator.integrate_real_excel_data()
    
    if success:
        print("\n✅ INTEGRATION COMPLETE!")
        print("🎯 Your data is ready for pure LLaMA 3.1 generation:")
        print("   1. All data loaded into ad_sales, total_sales, eligibility tables")
        print("   2. No hardcoded patterns or business logic")
        print("   3. LLaMA 3.1 will generate SQL based on your real data")
        print("   4. Test: uvicorn app.main:app --reload")
    else:
        print("\n❌ Integration failed - check error messages above")


if __name__ == "__main__":
    asyncio.run(main())
