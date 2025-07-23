import asyncpg
import asyncio
from sqlalchemy import create_engine, pool
from sqlalchemy.orm import sessionmaker
from database.models import Base
import os
import logging


# Configure logging for database operations
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Enhanced PostgreSQL connection for ANARIX AI Agent with Excel data optimization
DATABASE_URL = "postgresql://postgres:ANARIX_AI_2024!@localhost:5432/sales_analytics"


# Optimized SQLAlchemy engine for complex business intelligence queries
engine = create_engine(
    DATABASE_URL,
    # Connection pool optimization for complex queries
    poolclass=pool.QueuePool,
    pool_size=20,          # Support multiple concurrent complex queries
    max_overflow=30,       # Handle peak business intelligence workloads
    pool_timeout=300,      # 5-minute timeout for complex SQL operations
    pool_recycle=3600,     # Recycle connections every hour
    # Query optimization settings
    echo=False,            # Set to True for SQL debugging if needed
    future=True,           # Use SQLAlchemy 2.0 features
)


SessionLocal = sessionmaker(
    autocommit=False, 
    autoflush=False, 
    bind=engine,
    # Optimize for business intelligence operations
    expire_on_commit=False
)


def create_tables():
    """Create all tables for Excel-converted business data"""
    try:
        logger.info("Creating tables for ANARIX AI Agent Excel data...")
        Base.metadata.create_all(bind=engine)
        logger.info("✅ All tables created successfully for ad_sales, total_sales, eligibility")
    except Exception as e:
        logger.error(f"❌ Table creation failed: {e}")
        raise


def get_database():
    """Get SQLAlchemy database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Enhanced async PostgreSQL connection for unlimited SQL complexity
async def get_async_connection():
    """
    Get optimized async PostgreSQL connection for complex business intelligence queries
    Configured for unlimited SQL complexity on 8,779+ Excel records
    """
    try:
        connection = await asyncpg.connect(
            host='localhost',
            port=5432,
            user='postgres',
            password='ANARIX_AI_2024!',
            database='sales_analytics',
            # Optimization for complex queries on Excel data
            command_timeout=300,        # 5-minute timeout for sophisticated analysis
            server_settings={
                'application_name': 'ANARIX_AI_Agent',
                'jit': 'off',           # Disable JIT for consistent performance
                'work_mem': '256MB',    # Increase memory for complex operations
                'temp_buffers': '32MB', # Buffer for temporary tables in CTEs
            }
        )
        logger.info("✅ Async database connection established for Excel data processing")
        return connection
    except Exception as e:
        logger.error(f"❌ Async database connection failed: {e}")
        print(f"Database connection failed: {e}")
        return None


async def test_excel_data_connectivity():
    """
    Test connectivity to your Excel-converted tables
    Verify all 3 tables are accessible for business intelligence
    """
    conn = await get_async_connection()
    if not conn:
        logger.error("❌ Cannot establish connection to test Excel data tables")
        return False
    
    try:
        # Test each Excel-converted table
        ad_sales_count = await conn.fetchval("SELECT COUNT(*) FROM ad_sales")
        total_sales_count = await conn.fetchval("SELECT COUNT(*) FROM total_sales")  
        eligibility_count = await conn.fetchval("SELECT COUNT(*) FROM eligibility")
        
        logger.info(f"✅ Excel Data Tables Verified:")
        logger.info(f"   📊 ad_sales: {ad_sales_count:,} records")
        logger.info(f"   💰 total_sales: {total_sales_count:,} records")
        logger.info(f"   ✔️ eligibility: {eligibility_count:,} records")
        logger.info(f"   🎯 Total business records: {ad_sales_count + total_sales_count + eligibility_count:,}")
        
        # Test complex join capability
        join_test = await conn.fetchval("""
            SELECT COUNT(DISTINCT COALESCE(a.item_id, t.item_id, e.item_id))
            FROM ad_sales a
            FULL OUTER JOIN total_sales t ON a.item_id = t.item_id
            FULL OUTER JOIN eligibility e ON COALESCE(a.item_id, t.item_id) = e.item_id
        """)
        logger.info(f"   🔗 Unique products across all tables: {join_test:,}")
        
        await conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Excel data table verification failed: {e}")
        await conn.close()
        return False


def initialize_anarix_database():
    """
    Complete ANARIX AI Agent database initialization
    Sets up all tables and verifies Excel data connectivity
    """
    logger.info("🚀 Initializing ANARIX AI Agent database system...")
    
    try:
        # Create all tables
        create_tables()
        
        # Test async connectivity
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        connectivity_test = loop.run_until_complete(test_excel_data_connectivity())
        loop.close()
        
        if connectivity_test:
            logger.info("🎉 ANARIX AI Agent database initialization complete!")
            logger.info("💪 System ready for unlimited SQL complexity on Excel business data")
            return True
        else:
            logger.error("⚠️ Database initialization completed but connectivity test failed")
            return False
            
    except Exception as e:
        logger.error(f"❌ Database initialization failed: {e}")
        return False


# Connection health check for monitoring
async def health_check():
    """Check database health for Excel data processing"""
    conn = await get_async_connection()
    if not conn:
        return {"status": "unhealthy", "error": "Cannot connect to database"}
    
    try:
        # Quick health check query
        result = await conn.fetchval("SELECT 1")
        await conn.close()
        return {"status": "healthy", "database": "sales_analytics", "test_query": "passed"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


# Optional: Auto-initialize on module import (comment out if not needed)
if __name__ == "__main__":
    initialize_anarix_database()
