from sqlalchemy import Column, Integer, String, Numeric, Date, DateTime, Text, Index, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

class AdSales(Base):
    """
    Ad Sales data from Excel conversion - 3,696 records
    Primary table for advertising performance metrics
    """
    __tablename__ = "ad_sales"
    
    id = Column(Integer, primary_key=True, index=True)
    item_id = Column(String(100), index=True, nullable=False)  # Primary join key for business intelligence
    date = Column(Date, index=True)  # Indexed for time-based analysis
    ad_sales = Column(Numeric(15, 2), nullable=False, default=0)  # Advertising revenue from Excel
    impressions = Column(Integer, nullable=False, default=0)  # Ad impression counts
    ad_spend = Column(Numeric(12, 2), nullable=False, default=0)  # Advertising investment
    clicks = Column(Integer, nullable=False, default=0)  # Click-through counts
    units_sold = Column(Integer, nullable=False, default=0)  # Units sold via advertising
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Performance optimization indexes for complex queries
    __table_args__ = (
        Index('idx_ad_sales_item_date', 'item_id', 'date'),
        Index('idx_ad_sales_performance', 'ad_sales', 'ad_spend'),
        Index('idx_ad_sales_engagement', 'clicks', 'impressions'),
    )

class TotalSales(Base):
    """
    Total Sales data from Excel conversion - 702 records
    Overall product revenue and unit sales
    """
    __tablename__ = "total_sales"
    
    id = Column(Integer, primary_key=True, index=True)
    item_id = Column(String(100), index=True, nullable=False)  # Foreign key to ad_sales for JOINs
    date = Column(Date, index=True)  # Transaction date from Excel
    total_sales = Column(Numeric(15, 2), nullable=False, default=0)  # Total product revenue
    total_units_ordered = Column(Integer, nullable=False, default=0)  # Total units sold
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Performance optimization indexes for business intelligence
    __table_args__ = (
        Index('idx_total_sales_item_date', 'item_id', 'date'),
        Index('idx_total_sales_revenue', 'total_sales', 'total_units_ordered'),
    )

class Eligibility(Base):
    """
    Eligibility data from Excel conversion - 4,381 records
    Product compliance and availability status
    """
    __tablename__ = "eligibility"
    
    id = Column(Integer, primary_key=True, index=True)
    item_id = Column(String(100), index=True, nullable=False)  # Foreign key for multi-table analysis
    eligibility_datetime_utc = Column(DateTime, index=True)  # Eligibility check timestamp
    eligibility = Column(String(50), nullable=False, index=True)  # 'eligible'/'ineligible' status
    message = Column(Text)  # Detailed eligibility reason from Excel
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Business intelligence indexes
    __table_args__ = (
        Index('idx_eligibility_item_status', 'item_id', 'eligibility'),
        Index('idx_eligibility_datetime', 'eligibility_datetime_utc'),
    )

# Legacy table maintained for backward compatibility
class SalesData(Base):
    """
    Legacy sales data table - maintained for compatibility
    Use the 3-table structure above for new business intelligence queries
    """
    __tablename__ = "sales_data"
    
    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, index=True)
    product_id = Column(String(50), index=True)
    product_name = Column(String(200))
    sales_amount = Column(Numeric(10, 2), default=0)
    cost_per_click = Column(Numeric(8, 4), default=0)
    ad_spend = Column(Numeric(10, 2), default=0)
    region = Column(String(100), index=True)
    created_at = Column(DateTime, default=datetime.utcnow)

# Database relationship helper for complex query validation
class BusinessIntelligenceView:
    """
    Virtual view class for complex business intelligence queries
    Defines the relationships between your Excel-converted tables
    """
    @staticmethod
    def get_comprehensive_join_sql():
        return """
        SELECT 
            COALESCE(a.item_id, t.item_id, e.item_id) as item_id,
            a.ad_sales, a.ad_spend, a.clicks, a.impressions, a.units_sold,
            t.total_sales, t.total_units_ordered,
            e.eligibility, e.message, e.eligibility_datetime_utc
        FROM ad_sales a
        FULL OUTER JOIN total_sales t ON a.item_id = t.item_id
        FULL OUTER JOIN eligibility e ON COALESCE(a.item_id, t.item_id) = e.item_id
        """
    
    @staticmethod
    def get_table_info():
        return {
            "ad_sales": {"records": 3696, "primary_metrics": ["ad_sales", "ad_spend", "clicks", "impressions"]},
            "total_sales": {"records": 702, "primary_metrics": ["total_sales", "total_units_ordered"]},
            "eligibility": {"records": 4381, "primary_metrics": ["eligibility", "message"]},
            "total_records": 8779,
            "join_key": "item_id"
        }
