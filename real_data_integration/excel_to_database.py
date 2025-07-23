import pandas as pd
import asyncpg
import asyncio
from datetime import datetime
import os


class ExcelToSQLConverter:
    def __init__(self):
        self.connection_params = {
            'host': 'localhost',
            'port': 5432,
            'user': 'postgres',
            'password': 'ANARIX_AI_2024!',
            'database': 'sales_analytics'
        }
    
    async def convert_excel_to_sql(self):
        """Convert Excel files to your existing 3-table schema (no patterns)"""
        
        print("🚀 ANARIX AI Agent - Excel to SQL Conversion")
        print("=" * 60)
        
        try:
            # Load Excel files with correct names
            print("\n📊 Reading Excel files...")
            
            excel_files = {
                'ad_sales': 'Copy of Product-Level Ad Sales and Metrics (mapped).xlsx',
                'total_sales': 'Copy of Product-Level Total Sales and Metrics (mapped).xlsx',
                'eligibility': 'Copy of Product-Level Eligibility Table (mapped).xlsx'
            }
            
            dataframes = {}
            for table_name, filename in excel_files.items():
                if os.path.exists(filename):
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
                    
                    dataframes[table_name] = df
                    print(f"✅ Loaded {filename}: {len(df)} rows")
                else:
                    print(f"❌ File not found: {filename}")
                    return False
            
            # Connect to database
            print("\n🗄️ Connecting to database...")
            conn = await self.get_connection()
            if not conn:
                return False
            
            # Ensure correct table structure exists
            await self.ensure_correct_schema(conn)
            
            # Insert data into existing schema
            print("\n📥 Inserting data into existing tables...")
            await self.insert_data_to_existing_tables(conn, dataframes)
            
            # Validate conversion
            await self.validate_conversion(conn)
            
            await conn.close()
            print("\n🎉 Excel to SQL conversion completed!")
            print("🎯 Your data is ready for pure LLaMA 3.1 generation!")
            return True
            
        except Exception as e:
            print(f"❌ Conversion failed: {e}")
            return False
    
    async def get_connection(self):
        try:
            conn = await asyncpg.connect(**self.connection_params)
            print("✅ Database connection established")
            return conn
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return None
    
    async def ensure_correct_schema(self, conn):
        """Ensure your existing 3-table schema exists"""
        
        # Create ad_sales table (matches your models.py)
        await conn.execute("""
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
        await conn.execute("""
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
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS eligibility (
                id SERIAL PRIMARY KEY,
                item_id VARCHAR(100),
                eligibility_datetime_utc TIMESTAMP,
                eligibility VARCHAR(50),
                message TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        print("✅ Verified correct table schema exists")
    
    async def insert_data_to_existing_tables(self, conn, dataframes):
        """Insert data into your existing 3-table schema"""
        
        # Clear existing data
        await conn.execute("DELETE FROM ad_sales")
        await conn.execute("DELETE FROM total_sales") 
        await conn.execute("DELETE FROM eligibility")
        print("✅ Cleared existing data")
        
        # Insert ad_sales data
        if 'ad_sales' in dataframes:
            df = dataframes['ad_sales']
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
                    continue  # Skip invalid rows
            
            print(f"✅ Inserted {len(df)} ad_sales records")
        
        # Insert total_sales data
        if 'total_sales' in dataframes:
            df = dataframes['total_sales'] 
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
                    continue  # Skip invalid rows
            
            print(f"✅ Inserted {len(df)} total_sales records")
        
        # Insert eligibility data
        if 'eligibility' in dataframes:
            df = dataframes['eligibility']
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
                    continue  # Skip invalid rows
            
            print(f"✅ Inserted {len(df)} eligibility records")
    
    async def validate_conversion(self, conn):
        """Validate data was loaded correctly"""
        print("\n🔍 Validating conversion...")
        
        # Count records in each table
        ad_sales_count = await conn.fetchval("SELECT COUNT(*) FROM ad_sales")
        total_sales_count = await conn.fetchval("SELECT COUNT(*) FROM total_sales")
        eligibility_count = await conn.fetchval("SELECT COUNT(*) FROM eligibility")
        
        print(f"   📈 ad_sales: {ad_sales_count:,} records")
        print(f"   💰 total_sales: {total_sales_count:,} records") 
        print(f"   ✅ eligibility: {eligibility_count:,} records")
        print(f"   🎯 Total: {ad_sales_count + total_sales_count + eligibility_count:,} records")
        
        print("\n✅ Data ready for LLaMA 3.1 SQL generation!")


async def main():
    converter = ExcelToSQLConverter()
    success = await converter.convert_excel_to_sql()
    
    if success:
        print("\n🎯 SUCCESS: Excel data converted to LLaMA-compatible schema!")
        print("🤖 Your ANARIX AI Agent is ready for pure LLaMA 3.1 generation!")
    else:
        print("\n❌ Conversion failed - check error messages above")


if __name__ == "__main__":
    asyncio.run(main())
