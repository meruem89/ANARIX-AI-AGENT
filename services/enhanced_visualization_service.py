import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
from typing import Dict, List, Any
import sqlite3

class EnhancedVisualizationService:
    def __init__(self, db_path: str = "sales_data.db"):
        self.db_path = db_path
    
    def get_data_from_db(self, query: str) -> pd.DataFrame:
        """Execute SQL query and return DataFrame"""
        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        except Exception as e:
            print(f"Database error: {e}")
            return pd.DataFrame()
    
    def create_interactive_chart(self, df: pd.DataFrame, chart_type: str = "line", 
                               x_col: str = None, y_col: str = None, 
                               title: str = "Business Intelligence Chart") -> str:
        """Create interactive Plotly chart"""
        
        if df.empty:
            return self.create_error_chart("No data available")
        
        # Auto-detect columns if not provided
        if not x_col and not y_col:
            x_col = df.columns[0] if len(df.columns) > 0 else None
            y_col = df.columns[1] if len(df.columns) > 1 else None
        
        try:
            if chart_type == "line":
                fig = px.line(df, x=x_col, y=y_col, title=title,
                             template="plotly_dark")
            elif chart_type == "bar":
                fig = px.bar(df, x=x_col, y=y_col, title=title,
                            template="plotly_dark")
            elif chart_type == "scatter":
                fig = px.scatter(df, x=x_col, y=y_col, title=title,
                               template="plotly_dark")
            elif chart_type == "pie":
                fig = px.pie(df, names=x_col, values=y_col, title=title,
                            template="plotly_dark")
            else:
                fig = px.line(df, x=x_col, y=y_col, title=title,
                             template="plotly_dark")
            
            # Update layout for better appearance
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color='white'),
                title_font_size=20,
                height=500
            )
            
            return fig.to_html(include_plotlyjs='cdn', div_id="chart-container")
            
        except Exception as e:
            return self.create_error_chart(f"Chart creation error: {str(e)}")
    
    def create_dashboard_html(self, charts_data: List[Dict]) -> str:
        """Create complete dashboard HTML"""
        
        charts_html = []
        for chart_info in charts_data:
            query = chart_info.get('query', '')
            chart_type = chart_info.get('type', 'line')
            title = chart_info.get('title', 'Chart')
            
            df = self.get_data_from_db(query)
            chart_html = self.create_interactive_chart(df, chart_type, title=title)
            charts_html.append(chart_html)
        
        dashboard_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>ANARIX AI Agent - Business Intelligence Dashboard</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                body {{
                    background: linear-gradient(135deg, #1e293b 0%, #1e40af 50%, #1e293b 100%);
                    color: white;
                    font-family: 'Arial', sans-serif;
                    margin: 0;
                    padding: 20px;
                    min-height: 100vh;
                }}
                .dashboard-container {{
                    max-width: 1200px;
                    margin: 0 auto;
                }}
                .header {{
                    text-align: center;
                    margin-bottom: 30px;
                    background: rgba(255,255,255,0.1);
                    padding: 20px;
                    border-radius: 15px;
                    backdrop-filter: blur(10px);
                }}
                .chart-container {{
                    margin-bottom: 30px;
                    background: rgba(255,255,255,0.1);
                    border-radius: 15px;
                    padding: 20px;
                    backdrop-filter: blur(10px);
                }}
                .stats-grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                    gap: 20px;
                    margin-bottom: 30px;
                }}
                .stat-card {{
                    background: rgba(255,255,255,0.1);
                    padding: 20px;
                    border-radius: 15px;
                    text-align: center;
                    backdrop-filter: blur(10px);
                }}
                .stat-number {{
                    font-size: 2em;
                    font-weight: bold;
                    color: #60a5fa;
                }}
            </style>
        </head>
        <body>
            <div class="dashboard-container">
                <div class="header">
                    <h1>🤖 ANARIX AI Agent</h1>
                    <h2>Business Intelligence Dashboard</h2>
                    <p>Real-time data visualization and analytics</p>
                </div>
                
                <div class="stats-grid">
                    <div class="stat-card">
                        <div class="stat-number">{len(charts_data)}</div>
                        <div>Active Charts</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-number">Live</div>
                        <div>Status</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-number">6411</div>
                        <div>Data Points</div>
                    </div>
                </div>
                
                {''.join(f'<div class="chart-container">{chart}</div>' for chart in charts_html)}
            </div>
        </body>
        </html>
        """
        
        return dashboard_html
    
    def create_error_chart(self, error_message: str) -> str:
        """Create error chart when data is unavailable"""
        fig = go.Figure()
        fig.add_annotation(
            text=f"⚠️ {error_message}",
            x=0.5, y=0.5,
            font=dict(size=20, color="red"),
            showarrow=False
        )
        fig.update_layout(
            template="plotly_dark",
            title="Chart Error",
            height=400,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)'
        )
        return fig.to_html(include_plotlyjs='cdn')
    
    def generate_sample_dashboard(self) -> str:
        """Generate a sample dashboard with common business charts"""
        
        sample_charts = [
            {
                'query': 'SELECT * FROM your_sales_table LIMIT 100',  # Replace with your actual table
                'type': 'line',
                'title': 'Sales Trend Over Time'
            },
            {
                'query': 'SELECT category, SUM(amount) as total FROM your_sales_table GROUP BY category',  # Replace with your actual query
                'type': 'bar',
                'title': 'Sales by Category'
            }
        ]
        
        return self.create_dashboard_html(sample_charts)

# Usage example
if __name__ == "__main__":
    viz_service = EnhancedVisualizationService()
    
    # Generate and save dashboard
    dashboard_html = viz_service.generate_sample_dashboard()
    
    with open("dashboard.html", "w") as f:
        f.write(dashboard_html)
    
    print("Dashboard saved as 'dashboard.html'")
    print("Open this file in your web browser to view the interactive charts!")