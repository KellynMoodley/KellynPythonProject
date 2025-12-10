from flask import Flask, render_template, request, redirect, url_for, jsonify, send_file, make_response, session
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
import json
from datetime import datetime
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, A4, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
import pickle

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src import DataCleaner

app = Flask(__name__)
app.config['DATA_FOLDER'] = 'data'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max file size
app.config['SECRET_KEY'] = 'secretkey'  # Required for sessions

# Create data folder if it doesn't exist
os.makedirs(app.config['DATA_FOLDER'], exist_ok=True)

# Cache directory for storing dataset metadata
CACHE_DIR = 'dataset_cache'
os.makedirs(CACHE_DIR, exist_ok=True)

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Helper functions for dataset management
def get_dataset_list():
    """Get list of all dataset IDs from session"""
    return session.get('dataset_list', [])

def set_dataset_list(dataset_list):
    """Save dataset list to session"""
    session['dataset_list'] = dataset_list

def get_current_dataset_id():
    """Get current dataset ID from session"""
    return session.get('current_dataset_id')

def set_current_dataset_id(dataset_id):
    """Set current dataset ID in session"""
    session['current_dataset_id'] = dataset_id

def save_dataset_metadata(dataset_id, metadata):
    """Save dataset metadata to file"""
    filepath = os.path.join(CACHE_DIR, f'{dataset_id}_meta.pkl')
    with open(filepath, 'wb') as f:
        pickle.dump(metadata, f)

def load_dataset_metadata(dataset_id):
    """Load dataset metadata from file"""
    filepath = os.path.join(CACHE_DIR, f'{dataset_id}_meta.pkl')
    if os.path.exists(filepath):
        with open(filepath, 'rb') as f:
            return pickle.load(f)
    return None

def get_all_datasets():
    """Get all datasets metadata as a dictionary"""
    dataset_list = get_dataset_list()
    datasets = {}
    for dataset_id in dataset_list:
        metadata = load_dataset_metadata(dataset_id)
        if metadata:
            datasets[dataset_id] = metadata
    return datasets

def get_current_dataset():
    """Get current dataset metadata"""
    dataset_id = get_current_dataset_id()
    if dataset_id:
        return load_dataset_metadata(dataset_id)
    return None

# Allowed file type
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() == 'csv'

# Error handler for files too large
@app.errorhandler(RequestEntityTooLarge)
def handle_file_too_large(e):
    return "File is too large. Maximum allowed size is 50MB.", 413

# Index route with pagination, filtering, and sorting
@app.route('/')
def index():
    # Get dataset_id from query params or use current
    dataset_id = request.args.get('dataset_id', get_current_dataset_id())
    if dataset_id:
        set_current_dataset_id(dataset_id)
    
    datasets = get_all_datasets()
    dataset = get_current_dataset()
    
    if not dataset:
        return render_template('index.html',
                             data=[],
                             datasets=datasets,
                             current_dataset_id=get_current_dataset_id())
    
    page = request.args.get('page', 1, type=int)
    included_page = request.args.get('included_page', 1, type=int)
    excluded_page = request.args.get('excluded_page', 1, type=int)
    per_page = 50  # rows per page
    
    # Get filter and sort parameters for included data
    name_filter = request.args.get('name_filter', '').strip()
    month_filter = request.args.get('month_filter', '').strip()
    year_filter = request.args.get('year_filter', '').strip()
    day_filter = request.args.get('day_filter', '').strip()
    sort_by = request.args.get('sort_by', '')
    sort_order = request.args.get('sort_order', 'asc')
    
    csv_data = dataset.get('csv_data', [])
    included_data = dataset.get('included_data', [])
    excluded_data = dataset.get('excluded_data', [])
    summary_stats = dataset.get('summary_stats')
    
    # Apply filters to included data
    filtered_included = included_data.copy() if included_data else []
    if name_filter:
        filtered_included = [row for row in filtered_included if name_filter.lower() in row['name'].lower()]
    if month_filter:
        try:
            month_int = int(month_filter)
            filtered_included = [row for row in filtered_included if row['birth_month'] == month_int]
        except ValueError:
            pass
    if year_filter:
        try:
            year_int = int(year_filter)
            filtered_included = [row for row in filtered_included if row['birth_year'] == year_int]
        except ValueError:
            pass
    if day_filter:
        try:
            day_int = int(day_filter)
            filtered_included = [row for row in filtered_included if row['birth_day'] == day_int]
        except ValueError:
            pass
    
    # Apply sorting to included data
    if sort_by and filtered_included:
        reverse = (sort_order == 'desc')
        try:
            filtered_included = sorted(filtered_included, key=lambda x: x.get(sort_by, ''), reverse=reverse)
        except Exception as e:
            logging.error(f"Error sorting: {e}")

    # Original data pagination
    total_pages = math.ceil(len(csv_data) / per_page) if csv_data else 1
    start = (page - 1) * per_page
    end = start + per_page
    page_data = csv_data[start:end]

    # Included data pagination (after filters)
    included_total_pages = math.ceil(len(filtered_included) / per_page) if filtered_included else 1
    included_start = (included_page - 1) * per_page
    included_end = included_start + per_page
    included_page_data = filtered_included[included_start:included_end]

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
                           total_included=len(filtered_included),
                           total_excluded=len(excluded_data),
                           summary_stats=summary_stats,
                           name_filter=name_filter,
                           month_filter=month_filter,
                           year_filter=year_filter,
                           day_filter=day_filter,
                           sort_by=sort_by,
                           sort_order=sort_order,
                           datasets=datasets,
                           current_dataset_id=get_current_dataset_id())

# Upload route
@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return redirect(url_for('index'))

    file = request.files['file']

    if file.filename == '':
        return redirect(url_for('index'))

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        
        # Get existing dataset list
        dataset_list = get_dataset_list()
        
        # Generate unique dataset ID
        dataset_id = f"dataset_{len(dataset_list) + 1}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        filepath = os.path.join(app.config['DATA_FOLDER'], f"{dataset_id}_{filename}")

        # Save the file to the data folder
        file.save(filepath)
        
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
        
        # Create dataset metadata
        metadata = {
            'csv_data': csv_data,
            'included_data': [],
            'excluded_data': [],
            'summary_stats': None,
            'included_df': None,
            'excluded_df': None,
            'filename': filename,
            'filepath': filepath
        }
        
        # Save metadata
        save_dataset_metadata(dataset_id, metadata)
        
        # Update dataset list
        dataset_list.append(dataset_id)
        set_dataset_list(dataset_list)
        
        # Set as current dataset
        set_current_dataset_id(dataset_id)
        
        logging.info(f"Uploaded dataset {dataset_id}: {filename}")

    return redirect(url_for('index', dataset_id=get_current_dataset_id()))

# Clean data route
@app.route('/clean', methods=['POST'])
def clean_data():
    dataset_id = request.form.get('dataset_id', get_current_dataset_id())
    if not dataset_id:
        logging.error("No dataset ID provided")
        return redirect(url_for('index'))
    
    dataset = load_dataset_metadata(dataset_id)
    if not dataset:
        logging.error("Dataset not found")
        return redirect(url_for('index'))
    
    filepath = dataset['filepath']

    if not os.path.exists(filepath):
        logging.error("File not found for cleaning")
        return redirect(url_for('index'))

    try:
        # Load the CSV into a DataFrame with proper column mapping
        df = pd.read_csv(filepath)
        
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
        
        # Update dataset
        dataset['included_df'] = included_df
        dataset['excluded_df'] = excluded_df
        dataset['included_data'] = included_df.to_dict('records')
        dataset['excluded_data'] = excluded_df.to_dict('records')
        dataset['summary_stats'] = summary_stats
        
        # Save updated metadata
        save_dataset_metadata(dataset_id, dataset)
        
        logging.info(f"Data cleaning completed for {dataset_id}: {len(dataset['included_data'])} included, {len(dataset['excluded_data'])} excluded")
        
    except Exception as e:
        logging.error(f"Error during data cleaning: {e}")
        import traceback
        traceback.print_exc()

    return redirect(url_for('index', dataset_id=dataset_id))

# Clear specific dataset
@app.route('/clear/<dataset_id>', methods=['POST'])
def clear_dataset(dataset_id):
    dataset_list = get_dataset_list()
    
    if dataset_id in dataset_list:
        # Load and delete file
        dataset = load_dataset_metadata(dataset_id)
        if dataset:
            filepath = dataset.get('filepath')
            if filepath and os.path.exists(filepath):
                try:
                    os.unlink(filepath)
                except Exception as e:
                    logging.error(f"Error deleting file {filepath}: {e}")
        
        # Delete metadata file
        meta_filepath = os.path.join(CACHE_DIR, f'{dataset_id}_meta.pkl')
        if os.path.exists(meta_filepath):
            try:
                os.unlink(meta_filepath)
            except Exception as e:
                logging.error(f"Error deleting metadata {meta_filepath}: {e}")
        
        # Remove from dataset list
        dataset_list.remove(dataset_id)
        set_dataset_list(dataset_list)
        logging.info(f"Cleared dataset {dataset_id}")
        
        # Update current dataset ID
        current_id = get_current_dataset_id()
        if current_id == dataset_id:
            new_current = dataset_list[0] if dataset_list else None
            set_current_dataset_id(new_current)
    
    return redirect(url_for('index'))

# Clear all data
@app.route('/clear', methods=['POST'])
def clear_data():
    dataset_list = get_dataset_list()
    
    # Clean up all saved files
    for dataset_id in dataset_list:
        dataset = load_dataset_metadata(dataset_id)
        if dataset:
            filepath = dataset.get('filepath')
            if filepath and os.path.exists(filepath):
                try:
                    os.unlink(filepath)
                except Exception as e:
                    logging.error(f"Error deleting file {filepath}: {e}")
        
        # Delete metadata file
        meta_filepath = os.path.join(CACHE_DIR, f'{dataset_id}_meta.pkl')
        if os.path.exists(meta_filepath):
            try:
                os.unlink(meta_filepath)
            except Exception as e:
                logging.error(f"Error deleting metadata {meta_filepath}: {e}")
    
    # Clear session
    set_dataset_list([])
    set_current_dataset_id(None)
    
    return redirect(url_for('index'))

# Download included data as CSV
@app.route('/download/included/csv')
def download_included_csv():
    dataset = get_current_dataset()
    if dataset and dataset.get('included_df') is not None and not dataset['included_df'].empty:
        output = io.StringIO()
        dataset['included_df'].to_csv(output, index=False)
        output.seek(0)
        
        response = make_response(output.getvalue())
        filename = dataset.get('filename', 'data').replace('.csv', '')
        response.headers['Content-Disposition'] = f'attachment; filename={filename}_included.csv'
        response.headers['Content-Type'] = 'text/csv'
        return response
    return "No data available", 404

# Download included data as PDF
@app.route('/download/included/pdf')
def download_included_pdf():
    dataset = get_current_dataset()
    if not dataset or dataset.get('included_df') is None or dataset['included_df'].empty:
        return "No data available", 404
    
    included_df = dataset['included_df']
    summary_stats = dataset.get('summary_stats')
    
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(letter))
    elements = []
    styles = getSampleStyleSheet()
    
    # Title
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#667eea'),
        spaceAfter=30,
        alignment=1  # Center
    )
    elements.append(Paragraph(f"Data Included Report - {dataset.get('filename', 'Unknown')}", title_style))
    elements.append(Spacer(1, 0.3*inch))
    
    # Summary
    summary_text = f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br/>"
    summary_text += f"Total Records: {len(included_df)}<br/>"
    if summary_stats:
        summary_text += f"Unique Names: {summary_stats['uniqueness']['total_unique_names']}<br/>"
    elements.append(Paragraph(summary_text, styles['Normal']))
    elements.append(Spacer(1, 0.3*inch))
    
    # Table with full row IDs
    data = [['Row ID', 'Name', 'Birth Day', 'Birth Month', 'Birth Year']]
    for idx, row in included_df.iterrows():
        data.append([
            str(row['row_id']),  # Full row ID
            str(row['name']),
            str(row['birth_day']),
            str(row['birth_month']),
            str(row['birth_year'])
        ])
    
    table = Table(data, colWidths=[2.5*inch, 2*inch, 1*inch, 1.2*inch, 1*inch])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#667eea')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTSIZE', (0, 1), (-1, -1), 8)
    ]))
    
    elements.append(table)
    doc.build(elements)
    
    buffer.seek(0)
    filename = dataset.get('filename', 'data').replace('.csv', '')
    return send_file(buffer, as_attachment=True, download_name=f'{filename}_included_report.pdf', mimetype='application/pdf')

# Download excluded data as CSV
@app.route('/download/excluded/csv')
def download_excluded_csv():
    dataset = get_current_dataset()
    if dataset and dataset.get('excluded_df') is not None and not dataset['excluded_df'].empty:
        output = io.StringIO()
        dataset['excluded_df'].to_csv(output, index=False)
        output.seek(0)
        
        response = make_response(output.getvalue())
        filename = dataset.get('filename', 'data').replace('.csv', '')
        response.headers['Content-Disposition'] = f'attachment; filename={filename}_excluded.csv'
        response.headers['Content-Type'] = 'text/csv'
        return response
    return "No data available", 404

# Download excluded data as PDF
@app.route('/download/excluded/pdf')
def download_excluded_pdf():
    dataset = get_current_dataset()
    if not dataset or dataset.get('excluded_df') is None or dataset['excluded_df'].empty:
        return "No data available", 404
    
    excluded_df = dataset['excluded_df']
    
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(letter))
    elements = []
    styles = getSampleStyleSheet()
    
    # Title
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#dc3545'),
        spaceAfter=30,
        alignment=1
    )
    elements.append(Paragraph(f"Data Exclusion Report - {dataset.get('filename', 'Unknown')}", title_style))
    elements.append(Spacer(1, 0.3*inch))
    
    # Summary
    summary_text = f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br/>"
    summary_text += f"Total Excluded Records: {len(excluded_df)}<br/>"
    elements.append(Paragraph(summary_text, styles['Normal']))
    elements.append(Spacer(1, 0.3*inch))
    
    # Table with full row IDs
    data = [['Row ID', 'Name', 'Birth Day', 'Birth Month', 'Birth Year', 'Exclusion Reason']]
    for idx, row in excluded_df.iterrows():
        data.append([
            str(row['row_id']),  # Full row ID
            str(row['name']) if row['name'] else '-',
            str(row['birth_day']) if row['birth_day'] else '-',
            str(row['birth_month']) if row['birth_month'] else '-',
            str(row['birth_year']) if row['birth_year'] else '-',
            str(row['exclusion_reason'])
        ])
    
    table = Table(data, colWidths=[2.5*inch, 1.5*inch, 0.8*inch, 1*inch, 0.8*inch, 2.5*inch])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#dc3545')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTSIZE', (0, 1), (-1, -1), 7),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE')
    ]))
    
    elements.append(table)
    doc.build(elements)
    
    buffer.seek(0)
    filename = dataset.get('filename', 'data').replace('.csv', '')
    return send_file(buffer, as_attachment=True, download_name=f'{filename}_excluded_report.pdf', mimetype='application/pdf')

# Download top 80% names as CSV
@app.route('/download/top80/csv')
def download_top80_csv():
    dataset = get_current_dataset()
    if not dataset or not dataset.get('summary_stats') or 'top_80_names' not in dataset['summary_stats']:
        return "No data available", 404
    
    top_80_data = dataset['summary_stats']['top_80_names']
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Name', 'Frequency', 'Percentage'])
    
    for name_info in top_80_data['top_names']:
        writer.writerow([name_info['name'], name_info['frequency'], name_info['percentage']])
    
    output.seek(0)
    response = make_response(output.getvalue())
    filename = dataset.get('filename', 'data').replace('.csv', '')
    response.headers['Content-Disposition'] = f'attachment; filename={filename}_top_80_percent_names.csv'
    response.headers['Content-Type'] = 'text/csv'
    return response

# Download top 80% names as JSON
@app.route('/download/top80/json')
def download_top80_json():
    dataset = get_current_dataset()
    if not dataset or not dataset.get('summary_stats') or 'top_80_names' not in dataset['summary_stats']:
        return "No data available", 404
    
    top_80_data = dataset['summary_stats']['top_80_names']
    
    output = json.dumps(top_80_data, indent=2)
    response = make_response(output)
    filename = dataset.get('filename', 'data').replace('.csv', '')
    response.headers['Content-Disposition'] = f'attachment; filename={filename}_top_80_percent_names.json'
    response.headers['Content-Type'] = 'application/json'
    return response

# API endpoint for chart data
@app.route('/api/chart-data')
def get_chart_data():
    dataset = get_current_dataset()
    if dataset and dataset.get('included_df') is not None and not dataset['included_df'].empty:
        included_df = dataset['included_df']
        
        # Birth year distribution
        year_counts = included_df['birth_year'].value_counts().sort_index()
        year_data = {
            'labels': [str(year) for year in year_counts.index.tolist()],
            'values': year_counts.values.tolist()
        }
        
        # Birth month distribution
        month_counts = included_df['birth_month'].value_counts().sort_index()
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        month_data = {
            'labels': [month_names[int(m)-1] for m in month_counts.index.tolist()],
            'values': month_counts.values.tolist()
        }
        
        return jsonify({
            'year_distribution': year_data,
            'month_distribution': month_data
        })
    
    return jsonify({'error': 'No data available'}), 404

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)