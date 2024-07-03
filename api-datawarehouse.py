from flask import Flask, jsonify, request
import duckdb
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)

# Create a new Flask application
app = Flask(__name__)

def execute_query(query, params=()):
    try:
        # Connect to the DuckDB database
        duck_conn = duckdb.connect('/home/ngocthang/Documents/Code/Stock-Company-Analysis/warehouse/datawarehouse.duckdb')
        # Execute the query and fetch all results
        result = duck_conn.execute(query, params).fetchall()
        # Get the column names from the query result
        columns = [desc[0] for desc in duck_conn.description]
        # Combine column names and row data into a list of dictionaries
        data = [dict(zip(columns, row)) for row in result]
        # Close the database connection
        duck_conn.close()
        return data
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        return None

@app.route('/dim_time', methods=['GET'])
def get_dim_time():
    query = "SELECT * FROM dim_time;"
    data = execute_query(query)
    if data is None:
        return jsonify({'error': 'Error retrieving data'}), 500
    return jsonify(data)

@app.route('/dim_companies', methods=['GET'])
def get_dim_companies():
    query = '''
        SELECT company_id, company_name, company_time_stamp, company_ticket, 
            company_is_delisted, company_exchange_name, company_industry_name, 
            company_industry_sector, company_sic_industry, company_sic_sector 
        FROM dim_companies;
    '''
    data = execute_query(query)
    if data is None:
        return jsonify({'error': 'Error retrieving data'}), 500
    return jsonify(data)

@app.route('/dim_topics', methods=['GET'])
def get_dim_topics():
    query = 'SELECT * FROM dim_topics;'
    data = execute_query(query)
    if data is None:
        return jsonify({'error': 'Error retrieving data'}), 500
    return jsonify(data)

@app.route('/dim_news', methods=['GET'])
def get_dim_news():
    query = '''
        SELECT new_id, new_title, new_url, new_time_published, 
                new_overall_sentiment_score, new_overall_sentiment_label,
                news_time_id
        FROM dim_news;
    '''
    data = execute_query(query)
    if data is None:
        return jsonify({'error': 'Error retrieving data'}), 500
    return jsonify(data)

@app.route('/fact_news_companies', methods=['GET'])
def get_fact_news_companies():
    query = 'SELECT * FROM fact_news_companies;'
    data = execute_query(query)
    if data is None:
        return jsonify({'error': 'Error retrieving data'}), 500
    return jsonify(data)

@app.route('/fact_news_topics', methods=['GET'])
def get_fact_news_topics():
    query = 'SELECT * FROM fact_news_topics;'
    data = execute_query(query)
    if data is None:
        return jsonify({'error': 'Error retrieving data'}), 500
    return jsonify(data)

@app.route('/fact_candles', methods=['GET'])
def get_fact_candles():
    candles_time_id = request.args.get('candles_time_id')
    
    if not candles_time_id:
        return jsonify({'error': 'candles_time_id parameter is required'}), 400

    try:
        candles_time_id = int(candles_time_id)
    except ValueError:
        return jsonify({'error': 'candles_time_id parameter must be an integer'}), 400

    query = 'SELECT * FROM fact_candles WHERE candles_time_id = ?;'
    data = execute_query(query, (candles_time_id,))
    if data is None:
        return jsonify({'error': 'Error retrieving data'}), 500
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
