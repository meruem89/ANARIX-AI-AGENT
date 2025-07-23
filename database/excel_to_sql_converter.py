import pandas as pd
import psycopg2
import os
from sqlalchemy import create_engine

print("🔥 ANARIX AI - POSTGRESQL DATA CONVERTER")
print("=" * 50)

# EXACT file names from your directory
files = {
    "ad_sales": "Copy of Product-Level Ad Sales and Metrics (mapped).xlsx",
    "eligibility": "Copy of Product-Level Eligibility Table (mapped).xlsx", 
    "total_sales": "Copy of Product-Level Total Sales and Metrics (mapped).xlsx"
}

def convert_excel_to_postgresql():
    try:
        print(f"📍 Working directory: {os.getcwd()}")
        
        # PostgreSQL connection using SQLAlchemy (simpler than asyncpg)
        engine = create_engine('postgresql://postgres:ANARIX_AI_2024!@localhost:5432/sales_analytics')

        print("✅ PostgreSQL connection established!")
        
        total_records = 0
        successful_tables = []
        
        for table_name, filename in files.items():
            print(f"\n📊 Processing: {filename}")
            
            if os.path.exists(filename):
                print(f"✅ File found: {filename}")
                
                # Read Excel file
                df = pd.read_excel(filename)
                records = len(df)
                columns = len(df.columns)
                
                print(f"✅ Data loaded: {records} rows, {columns} columns")
                print(f"📋 Sample columns: {list(df.columns)[:3]}...")
                
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
                
                # Handle duplicates and PostgreSQL reserved words
                cleaned_columns = []
                for i, col in enumerate(df.columns):
                    if col in cleaned_columns or col in ['order', 'user', 'table']:
                        col = f"{col}_{i}"
                    cleaned_columns.append(col)
                df.columns = cleaned_columns
                
                # Replace NaN with None for SQL compatibility
                df = df.where(pd.notnull(df), None)
                
                # Save to PostgreSQL database
                df.to_sql(table_name, engine, if_exists='replace', index=False, method='multi')
                
                print(f"🎉 SUCCESS: {table_name} table created in PostgreSQL!")
                print(f"📊 Records inserted: {records}")
                
                total_records += records
                successful_tables.append(table_name)
                
            else:
                print(f"❌ ERROR: File not found - {filename}")
        
        print("\n" + "=" * 50)
        print("🚀 POSTGRESQL CONVERSION SUMMARY:")
        print(f"✅ Successful tables: {len(successful_tables)}")
        print(f"📊 Total records: {total_records:,}")
        print(f"🗄️ Database: sales_analytics (PostgreSQL)")
        print("✅ READY FOR LLaMA INTEGRATION!")
        
        return len(successful_tables) == 3
        
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = convert_excel_to_postgresql()
    
    if success:
        print("\n✅ PostgreSQL conversion complete! Moving to Phase 2...")
    else:
        print("\n❌ CONVERSION FAILED! Check PostgreSQL connection.")
