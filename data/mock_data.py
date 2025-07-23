from datetime import date, timedelta
import random

class MockDataService:
    def __init__(self):
        self.sample_data = self.generate_sample_business_data()
    
    def generate_sample_business_data(self):
        """Generate realistic business data for testing visualizations"""
        
        products = [
            {"id": "PRD001", "name": "Gaming Laptop Pro"},
            {"id": "PRD002", "name": "Wireless Headphones"},
            {"id": "PRD003", "name": "Smart Phone X"},
            {"id": "PRD004", "name": "Fitness Tracker"},
            {"id": "PRD005", "name": "Coffee Maker Deluxe"}
        ]
        
        regions = ["North", "South", "East", "West", "Central"]
        
        # Generate sales data
        sales_data = []
        for i in range(50):
            product = random.choice(products)
            sales_record = {
                "id": i + 1,
                "date": date.today() - timedelta(days=random.randint(1, 365)),
                "product_id": product["id"],
                "product_name": product["name"],
                "sales_amount": round(random.uniform(100, 5000), 2),
                "cost_per_click": round(random.uniform(0.5, 8.0), 4),
                "ad_spend": round(random.uniform(50, 1000), 2),
                "region": random.choice(regions)
            }
            sales_data.append(sales_record)
        
        return sales_data
    
    def get_total_sales(self):
        """Mock response for 'What is my total sales?' query"""
        total = sum(record["sales_amount"] for record in self.sample_data)
        return [{"total_sales": total}]
    
    def get_roas(self):
        """Mock response for 'Calculate the RoAS' query"""
        total_sales = sum(record["sales_amount"] for record in self.sample_data)
        total_ad_spend = sum(record["ad_spend"] for record in self.sample_data)
        roas = total_sales / total_ad_spend if total_ad_spend > 0 else 0
        return [{"roas": round(roas, 2)}]
    
    def get_highest_cpc_product(self):
        """Mock response for 'Which product had the highest CPC?' query"""
        # Group by product and find highest CPC
        product_cpc = {}
        for record in self.sample_data:
            product = record["product_name"]
            cpc = record["cost_per_click"]
            if product not in product_cpc or cpc > product_cpc[product]:
                product_cpc[product] = cpc
        
        # Convert to list format for visualization
        result = [{"product_name": product, "cost_per_click": cpc} 
                 for product, cpc in product_cpc.items()]
        return sorted(result, key=lambda x: x["cost_per_click"], reverse=True)
    
    def get_sales_by_region(self):
        """Regional sales breakdown"""
        region_sales = {}
        for record in self.sample_data:
            region = record["region"]
            if region not in region_sales:
                region_sales[region] = 0
            region_sales[region] += record["sales_amount"]
        
        return [{"region": region, "total_sales": sales} 
                for region, sales in region_sales.items()]
