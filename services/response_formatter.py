class ResponseFormatter:
    """Simplified response formatter for pure LLaMA 3.1 system"""
    
    def format_simple_response(self, query, result, query_type="llama_generated"):
        """Generic formatter that doesn't impose patterns on LLaMA results"""
        try:
            if isinstance(result, list):
                return {
                    "business_summary": f"Query executed successfully: {len(result)} records",
                    "query": query,
                    "result": result,
                    "record_count": len(result),
                    "query_type": query_type,
                    "processing": "pure_llama_generation"
                }
            elif isinstance(result, dict):
                return {
                    "business_summary": f"Query executed successfully",
                    "query": query,
                    "result": result,
                    "query_type": query_type,
                    "processing": "pure_llama_generation"
                }
            else:
                return {
                    "business_summary": f"Query result: {result}",
                    "query": query,
                    "result": result,
                    "query_type": query_type,
                    "processing": "pure_llama_generation"
                }
        except Exception as e:
            return {
                "error": str(e),
                "query": query,
                "processing": "pure_llama_generation"
            }
    
    # Keep legacy methods for visualization endpoints
    def format_sales_response(self, total_sales):
        """Legacy method for visualization endpoints"""
        sales_value = float(total_sales) if total_sales else 0
        return {
            "business_summary": f"Total Sales: ${sales_value:,.2f}",
            "total_sales": sales_value
        }
    
    def format_roas_response(self, avg_roas, total_sales, total_ad_spend):
        """Legacy method for visualization endpoints"""
        return {
            "business_summary": f"RoAS Analysis: {avg_roas:.2f}x",
            "avg_roas": avg_roas,
            "total_sales": total_sales,
            "total_ad_spend": total_ad_spend
        }
    
    def format_cpc_response(self, product_name, highest_cpc, avg_cpc):
        """Legacy method for visualization endpoints"""
        return {
            "business_summary": f"CPC Analysis: {product_name}",
            "product": product_name,
            "highest_cpc": highest_cpc,
            "avg_cpc": avg_cpc
        }
