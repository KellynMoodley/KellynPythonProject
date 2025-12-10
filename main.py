from flask import Flask, render_template, request, redirect, url_for
import csv
import sys
import os
import logging
import chardet
from werkzeug.utils import secure_filename
from werkzeug.exceptions import RequestEntityTooLarge
import io
import math
import pandas as pd

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src import DataCleaner

app = Flask(__name__)
app.config['DATA_FOLDER'] = 'data'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max file size

# Create data folder if it doesn't exist
os.makedirs(app.config['DATA_FOLDER'], exist_ok=True)

# Store CSV data in memory
csv_data = []
included_data = []
excluded_data = []
summary_stats = None
current_filepath = None

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Allowed file type
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() == 'csv'

# Error handler for files too large
@app.errorhandler(RequestEntityTooLarge)
def handle_file_too_large(e):
    return "File is too large. Maximum allowed size is 50MB.", 413

# Index route with pagination
@app.route('/')
def index():
    page = request.args.get('page', 1, type=int)
    included_page = request.args.get('included_page', 1, type=int)
    excluded_page = request.args.get('excluded_page', 1, type=int)
    per_page = 50  # rows per page

    # Original data pagination
    total_pages = math.ceil(len(csv_data) / per_page) if csv_data else 1
    start = (page - 1) * per_page
    end = start + per_page
    page_data = csv_data[start:end]

    # Included data pagination
    included_total_pages = math.ceil(len(included_data) / per_page) if included_data else 1
    included_start = (included_page - 1) * per_page
    included_end = included_start + per_page
    included_page_data = included_data[included_start:included_end]

    # Excluded data pagination
    excluded_total_pages = math.ceil(len(excluded_data) / per_page) if excluded_data else 1
    excluded_start = (excluded_page - 1) * per_page
    excluded_end = excluded_start + per_page
    excluded_page_data = excluded_data[excluded_start:excluded_end]

    return render_template('index.html',
                           data=page_data,
                           page=page,
                           total_pages=total_pages,
                           included_data=included_page_data,
                           excluded_data=excluded_page_data,
                           included_page=included_page,
                           excluded_page=excluded_page,
                           included_total_pages=included_total_pages,
                           excluded_total_pages=excluded_total_pages,
                           total_included=len(included_data),
                           total_excluded=len(excluded_data),
                           summary_stats=summary_stats)

# Upload route
@app.route('/upload', methods=['POST'])
def upload_file():
    global csv_data, included_data, excluded_data, summary_stats, current_filepath

    # Reset cleaning data when new file is uploaded
    included_data = []
    excluded_data = []
    summary_stats = None

    if 'file' not in request.files:
        return redirect(url_for('index'))

    file = request.files['file']

    if file.filename == '':
        return redirect(url_for('index'))

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['DATA_FOLDER'], filename)

        # Save the file to the data folder
        file.save(filepath)
        current_filepath = filepath
        
        # Detect encoding using first 4KB
        with open(filepath, 'rb') as f:
            raw_bytes = f.read(4096)
            result = chardet.detect(raw_bytes)
            encoding = result['encoding'] or 'latin-1'
            logging.info(f"Detected encoding: {encoding}")

        # Read CSV for display
        csv_data = []
        try:
            with open(filepath, 'r', encoding=encoding, errors='replace') as f:
                csv_reader = csv.DictReader(f)
                for row in csv_reader:
                    csv_row = {
                        'FirstName': row.get('FirstName', '') or '-',
                        'BirthDay': row.get('BirthDay', ''),
                        'BirthMonth': row.get('BirthMonth', ''),
                        'BirthYear': row.get('BirthYear', '')
                    }
                    csv_data.append(csv_row)
        except Exception as e:
            logging.error(f"Error reading CSV: {e}")

    return redirect(url_for('index'))

# Clean data route
@app.route('/clean', methods=['POST'])
def clean_data():
    global included_data, excluded_data, summary_stats, current_filepath

    if not current_filepath or not os.path.exists(current_filepath):
        logging.error("No file available for cleaning")
        return redirect(url_for('index'))

    try:
        # Load the CSV into a DataFrame with proper column mapping
        df = pd.read_csv(current_filepath)
        
        # Rename columns to match datacleaning.py expectations
        column_mapping = {
            'FirstName': 'name',
            'BirthDay': 'birth_day',
            'BirthMonth': 'birth_month',
            'BirthYear': 'birth_year'
        }
        df = df.rename(columns=column_mapping)
        
        # Initialize cleaner and clean data
        cleaner = DataCleaner()
        included_df, excluded_df = cleaner.clean_data(df)
        
        # Get summary statistics
        summary_stats = cleaner.get_summary_stats(included_df, excluded_df)
        
        # Convert to list of dicts for template rendering
        included_data = included_df.to_dict('records')
        excluded_data = excluded_df.to_dict('records')
        
        logging.info(f"Data cleaning completed: {len(included_data)} included, {len(excluded_data)} excluded")
        
    except Exception as e:
        logging.error(f"Error during data cleaning: {e}")
        import traceback
        traceback.print_exc()

    return redirect(url_for('index'))

# Clear CSV data route
@app.route('/clear', methods=['POST'])
def clear_data():
    global csv_data, included_data, excluded_data, summary_stats, current_filepath
    csv_data = []
    included_data = []
    excluded_data = []
    summary_stats = None
    current_filepath = None
    
    # Clean up saved files
    if os.path.exists(app.config['DATA_FOLDER']):
        for filename in os.listdir(app.config['DATA_FOLDER']):
            filepath = os.path.join(app.config['DATA_FOLDER'], filename)
            try:
                if os.path.isfile(filepath):
                    os.unlink(filepath)
            except Exception as e:
                logging.error(f"Error deleting file {filepath}: {e}")
    
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)