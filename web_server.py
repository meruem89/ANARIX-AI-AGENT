from flask import Flask, render_template_string, jsonify, request
import sqlite3
import pandas as pd
from services.visualization_service import EnhancedVisualizationService
import json

app = Flask(__name__)

# Initialize visualization service
viz_service = EnhancedVisualizationService("sales_data.db")

@app.route('/')
def dashboard():
    """Main dashboard route"""
    # Read the HTML template
    with open('templates/dashboard.html', 'r') as f:
        dashboard_html = f.read()
    return dashboard_html

@app.route('/api/data')
def get_data():
    """API endpoint to get data for charts"""
    try:
        # Replace this query with your actual data query
        query = """
        SELECT 
            strftime('%Y-%m', date) as month,
            SUM(amount) as total_sales,
            COUNT(*) as transaction_count,
            AVG(amount) as avg_transaction
        FROM your_sales_table 
        WHERE date >= date('now', '-12 months')
        GROUP BY strftime('%Y-%m', date)
        ORDER BY month
        """
        
        conn = sqlite3.connect("sales_data.db")
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Convert DataFrame to JSON format expected by the frontend
        data = []
        for _, row in df.iterrows():
            data.append({
                'name': row['month'],
                'value': float(row['total_sales']) if pd.notna(row['total_sales']) else 0,
                'revenue': float(row['total_sales']) if pd.notna(row['total_sales']) else 0,
                'customers': int(row['transaction_count']) if pd.notna(row['transaction_count']) else 0
            })
        
        return jsonify(data)
        
    except Exception as e:
        # Return sample data if there's an error
        print(f"Error fetching data: {e}")
        sample_data = [
            {'name': 'Jan', 'value': 400, 'revenue': 2400, 'customers': 120},
            {'name': 'Feb', 'value': 300, 'revenue': 1398, 'customers': 98},
            {'name': 'Mar', 'value': 200, 'revenue': 9800, 'customers': 150},
            {'name': 'Apr', 'value': 278, 'revenue': 3908, 'customers': 189},
            {'name': 'May', 'value': 189, 'revenue': 4800, 'customers': 234},
            {'name': 'Jun', 'value': 239, 'revenue': 3800, 'customers': 267}
        ]
        return jsonify(sample_data)

@app.route('/api/tables')
def get_tables():
    """Get list of available tables"""
    try:
        conn = sqlite3.connect("sales_data.db")
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [table[0] for table in cursor.fetchall()]
        conn.close()
        return jsonify(tables)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/query', methods=['POST'])
def execute_query():
    """Execute custom SQL query"""
    try:
        query = request.json.get('query', '')
        conn = sqlite3.connect("sales_data.db")
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Convert to JSON
        result = df.to_dict('records')
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/plotly-dashboard')
def plotly_dashboard():
    """Alternative dashboard using Plotly"""
    sample_charts = [
        {
            'query': 'SELECT * FROM your_table_name LIMIT 100',  # Update with your actual table
            'type': 'line',
            'title': 'Your Data Visualization'
        }
    ]
    
    dashboard_html = viz_service.create_dashboard_html(sample_charts)
    return dashboard_html

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    import os
    if not os.path.exists('templates'):
        os.makedirs('templates')
    
    print("🚀 Starting ANARIX AI Agent Web Server...")
    print("📊 Dashboard will be available at: http://localhost:5000")
    print("🔍 API endpoints available at: http://localhost:5000/api/data")
    
    app.run(debug=True, host='0.0.0.0', port=5000)