from flask import Flask, jsonify, request
import happybase

app = Flask(__name__)

# HBase connection settings
hbase_host = 'localhost'
hbase_table_name = 'hbase_table_name'

def get_hbase_connection():
    connection = happybase.Connection(hbase_host)
    return connection

def fetch_data_with_filter(column_family, filter_condition):
    connection = get_hbase_connection()
    table = connection.table(hbase_table_name)

    # Apply filter condition to the HBase scan
    scan = table.scan(filter=filter_condition)

    # Fetch and format the results
    data = [{key.decode(): value.decode() for key, value in row.items()} for row in scan]

    connection.close()
    return data

@app.route('/api/data', methods=['GET'])
def get_filtered_data():
    # Get filter condition from request parameters
    filter_condition = request.args.get('filter')

    if not filter_condition:
        return jsonify({'error': 'Filter condition is required'}), 400

    # Specify the column family to scan and the filter condition
    column_family = 'cf'
    
    data = fetch_data_with_filter(column_family, filter_condition)
    
    if data:
        return jsonify(data)
    else:
        return jsonify({'error': 'Data not found'}), 404

if __name__ == '__main__':
    app.run(debug=True)


#To test
#curl http://host:port/api/data?filter=example_filter_condition
