from flask import Flask, render_template, request, redirect, url_for
import csv
import os
import logging
import chardet
from werkzeug.utils import secure_filename
from werkzeug.exceptions import RequestEntityTooLarge
import io
import math

app = Flask(__name__)
app.config['DATA_FOLDER'] = 'data'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max file size

# Create data folder if it doesn't exist
os.makedirs(app.config['DATA_FOLDER'], exist_ok=True)

# Store CSV data in memory
csv_data = []

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
    per_page = 50  # rows per page

    total_pages = math.ceil(len(csv_data) / per_page)
    start = (page - 1) * per_page
    end = start + per_page
    page_data = csv_data[start:end]

    return render_template('index.html',
                           data=page_data,
                           page=page,
                           total_pages=total_pages)

# Upload route
@app.route('/upload', methods=['POST'])
def upload_file():
    global csv_data

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
        
        # Detect encoding using first 4KB
        raw_bytes = file.read(4096)
        result = chardet.detect(raw_bytes)
        encoding = result['encoding'] or 'latin-1'
        logging.info(f"Detected encoding: {encoding}")

        # Reset file stream to beginning
        file.seek(0)

        # Read CSV directly from memory
        csv_data = []
        try:
            file_stream = io.TextIOWrapper(file.stream, encoding=encoding, errors='replace')
            csv_reader = csv.DictReader(file_stream)
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

# Clear CSV data route
@app.route('/clear', methods=['POST'])
def clear_data():
    global csv_data
    csv_data = []
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
