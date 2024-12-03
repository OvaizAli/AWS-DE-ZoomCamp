import functions_framework
import base64
import csv
from google.cloud import bigquery
from flask import jsonify, make_response

@functions_framework.http
def fetch_and_load_csv_to_bq(request):
    """
    HTTP Cloud Function to fetch CSV data from the request payload and load into BigQuery.
    """
    # Initialize BigQuery client
    client = bigquery.Client()

    # Define BigQuery dataset and table names
    project_id = 'csci-5408-data-management'
    dataset_id = 'nycTaxi'
    location_aggregated_fact_table_id = 'LocationAggregatedFact'
    total_aggregated_fact_table_id = 'TotalAggregatedFact'
    location_dimension_table_id = 'LocationDimension'
    time_dimension_table_id = 'TimeDimension'

    # Fetch files from the request payload
    request_json = request.get_json(silent=True)
    if not request_json or 'files' not in request_json:
        return make_response(jsonify({'error': 'No files provided in the request'}), 400)

    files = request_json['files']

    # Process each file in the payload
    for file_data in files:
        file_name = file_data['file_name']
        file_content = file_data['file_content']
        
        # Decode the Base64-encoded file content
        try:
            decoded_content = base64.b64decode(file_content)
            file_content_text = decoded_content.decode('utf-8')

            # Parse CSV content into rows
            rows_to_insert = parse_csv_content(file_content_text, file_name)

            if rows_to_insert:
                # Determine the table based on the file name
                if 'total_aggregated' in file_name:
                    table_ref = client.dataset(dataset_id).table(total_aggregated_fact_table_id)
                elif 'location_aggregated' in file_name:
                    table_ref = client.dataset(dataset_id).table(location_aggregated_fact_table_id)
                elif 'location_dimension' in file_name:
                    table_ref = client.dataset(dataset_id).table(location_dimension_table_id)
                elif 'time_dimension' in file_name:
                    table_ref = client.dataset(dataset_id).table(time_dimension_table_id)
                else:
                    return make_response(jsonify({'error': f"Unknown file type: {file_name}"}), 400)

                # Insert rows into the appropriate BigQuery table (this appends by default)
                errors = client.insert_rows_json(table_ref, rows_to_insert)

                if errors:
                    return make_response(jsonify({'error': f"Error inserting rows into BigQuery: {errors}"}), 500)
            else:
                return make_response(jsonify({'error': f'No valid data found in the CSV file {file_name}'}), 400)

        except Exception as e:
            return make_response(jsonify({'error': f"Error processing file {file_name}: {str(e)}"}), 500)

    return make_response(jsonify({'message': 'Data loaded successfully.'}), 200)


def parse_csv_content(file_content, file_name):
    """
    Parse CSV content into rows for BigQuery, mapping CSV columns to BigQuery columns.
    """
    rows = []
    csv_reader = csv.DictReader(file_content.splitlines())

    # Define mappings for CSV columns to BigQuery columns
    column_mappings = {
        'total_aggregated': {
            'tpep_pickup_datetime': 'year',
            'month': 'month',
            'total_trips': 'total_trips',
            'total_revenue': 'total_revenue',
            'total_fare': 'total_fare',
            'total_tips': 'total_tips',
            'total_tolls': 'total_tolls',
            'total_congestion_surcharge': 'total_congestion_surcharge',
            'total_airport_fee': 'total_airport_fee',
            'avg_trip_distance': 'avg_trip_distance',
            'avg_fare_per_trip': 'avg_fare_per_trip'
        },
        'location_aggregated': {
            'PULocationID': 'PULocationID',
            'total_trips': 'total_trips',
            'total_revenue': 'total_revenue',
            'total_fare': 'total_fare',
            'avg_trip_distance': 'avg_trip_distance',
            'avg_fare_per_trip': 'avg_fare_per_trip'
        },
        'location_dimension': {
            'location_id': 'location_id',
            'PULocationID': 'PULocationID',
            'DOLocationID': 'DOLocationID'
        },
        'time_dimension': {
            'date': 'date',
            'day_of_week': 'day_of_week',
            'month': 'month',
            'year': 'year',
            'holiday_flag': 'holiday_flag'
        }
    }

    # Get the appropriate mapping based on the file name
    mapping = None
    for key in column_mappings:
        if key in file_name.lower():
            mapping = column_mappings[key]
            break

    if not mapping:
        return None

    # Map CSV columns to BigQuery columns
    for row in csv_reader:
        mapped_row = {}
        for csv_column, bq_column in mapping.items():
            if csv_column in row:
                mapped_row[bq_column] = row[csv_column]
        rows.append(mapped_row)

    return rows
