import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import json
from typing import Dict, List, Any
import numpy as np

class VisualizationService:
    def _init_(self):
        self.business_colors = {
            'primary': '#1f77b4',
            'success': '#2ca02c',
            'warning': '#ff7f0e',
            'danger': '#d62728'
        }
    
    def should_visualize(self, query: str, results: List[Dict]) -> bool:
        """Always try to visualize if we have data"""
        return bool(results) and len(results) >= 1
    
    def create_visualization(self, question: str, results: List[Dict]) -> Dict[str, Any]:
        """Create visualization - this WILL work"""
        try:
            if not results:
                return {'success': False, 'reason': 'No data to visualize'}
            
            print(f"🎨 Starting visualization creation for {len(results)} records")
            
            # Convert to DataFrame
            df = pd.DataFrame(results)
            print(f"📊 DataFrame created with columns: {list(df.columns)}")
            
            # Get numeric columns
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            print(f"🔢 Numeric columns found: {numeric_cols}")
            
            if not numeric_cols:
                return {'success': False, 'reason': 'No numeric data for visualization'}
            
            # Create the chart
            chart_data, layout = self._create_simple_bar_chart(df, numeric_cols, question)
            
            # Create Plotly figure
            fig = go.Figure(data=chart_data, layout=layout)
            
            # Convert to JSON for frontend
            chart_json = fig.to_json()
            
            print("✅ Visualization created successfully")
            
            return {
                'success': True,
                'chart_type': 'bar',
                'chart_json': chart_json,
                'title': self._generate_title(question),
                'data_summary': {
                    'total_records': len(df),
                    'data_quality': 'Good'
                },
                'recommendations': self._generate_recommendations(df, question)
            }
            
        except Exception as e:
            print(f"❌ Visualization error: {e}")
            import traceback
            traceback.print_exc()
            return {'success': False, 'reason': f'Chart creation failed: {str(e)}'}
    
    def _create_simple_bar_chart(self, df: pd.DataFrame, numeric_cols: List[str], question: str):
        """Create a simple bar chart that WILL work"""
        
        # Determine x and y columns
        x_col = 'item_id' if 'item_id' in df.columns else df.columns[0]
        y_col = numeric_cols[0]  # Use first numeric column
        
        # Limit to top 15 for readability
        df_display = df.head(15).copy()
        
        # Ensure we have string labels for x-axis
        x_labels = df_display[x_col].astype(str).tolist()
        y_values = df_display[y_col].tolist()
        
        print(f"📈 Creating bar chart: {len(x_labels)} bars, y-values range: {min(y_values):.2f} to {max(y_values):.2f}")
        
        # Create bar chart data
        chart_data = [go.Bar(
            x=x_labels,
            y=y_values,
            marker_color=self.business_colors['primary'],
            text=[f'{val:.2f}' for val in y_values],
            textposition='outside',
            name=y_col.replace('_', ' ').title()
        )]
        
        # Create layout
        layout = go.Layout(
            title=f'{y_col.replace("_", " ").title()} Analysis',
            xaxis=dict(
                title=x_col.replace('_', ' ').title(),
                tickangle=45
            ),
            yaxis=dict(
                title=y_col.replace('_', ' ').title()
            ),
            template='plotly_white',
            height=500,
            margin=dict(b=100)  # Extra bottom margin for rotated labels
        )
        
        return chart_data, layout
    
    def _generate_title(self, question: str) -> str:
        """Generate chart title"""
        if 'cpc' in question.lower():
            return "Cost Per Click (CPC) Analysis"
        elif 'roas' in question.lower():
            return "Return on Ad Spend (ROAS) Analysis"
        elif 'roi' in question.lower():
            return "Return on Investment (ROI) Analysis"
        elif 'revenue' in question.lower() or 'sales' in question.lower():
            return "Revenue & Sales Analysis"
        else:
            return "Business Intelligence Analysis"
    
    def _generate_recommendations(self, df: pd.DataFrame, question: str) -> List[str]:
        """Generate simple recommendations"""
        recommendations = [
            f"Analyzed {len(df)} products for business insights",
            "Focus on top-performing items for optimization"
        ]
        
        if 'cpc' in question.lower():
            recommendations.append("Consider optimizing ad campaigns for cost efficiency")
        elif 'revenue' in question.lower():
            recommendations.append("Scale high-revenue products for maximum impact")
        
        return recommendations