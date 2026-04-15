from flask import Flask, request, render_template, jsonify
import pandas as pd
import io
import json
import os
from werkzeug.utils import secure_filename

app = Flask(__name__)

# Configuration for production
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-key-change-in-production')
app.config['ALLOWED_EXTENSIONS'] = {
    'csv', 'xlsx', 'xls', 'json', 'txt', 'tsv', 'parquet', 'xml', 'html'
}

# Get port from environment variable (for cloud platforms)
port = int(os.environ.get('PORT', 5000))

def get_file_extension(filename):
    """Get file extension safely"""
    return filename.rsplit('.', 1)[1].lower() if '.' in filename else ''

def read_file(file, filename):
    """Read various file formats and return DataFrame"""
    ext = get_file_extension(filename)
    
    try:
        if ext == 'csv':
            return pd.read_csv(file)
        elif ext in ['xlsx', 'xls', 'xlsm', 'xlsb']:
            return pd.read_excel(file, engine='openpyxl' if ext in ['xlsx', 'xlsm'] else 'xlrd')
        elif ext == 'json':
            return pd.read_json(file)
        elif ext == 'tsv':
            return pd.read_csv(file, sep='\t')
        elif ext == 'txt':
            content = file.read().decode('utf-8')
            lines = content.split('\n')
            if ',' in lines[0]:
                return pd.read_csv(io.StringIO(content))
            elif '\t' in lines[0]:
                return pd.read_csv(io.StringIO(content), sep='\t')
            else:
                data = [line.strip() for line in lines if line.strip()]
                return pd.DataFrame({'data': data})
        elif ext == 'xml':
            return pd.read_xml(file)
        elif ext == 'html':
            tables = pd.read_html(file)
            if tables:
                return tables[0]
            else:
                raise ValueError("No tables found in HTML file")
        else:
            raise ValueError(f"Unsupported file format: {ext}")
    except Exception as e:
        raise Exception(f"Error reading {ext.upper()} file: {str(e)}")

def calculate_quality_metrics(df, original_rows):
    """Calculate comprehensive data quality and fragmentation metrics"""
    total_records = len(df)
    total_columns = len(df.columns)
    total_cells = total_records * total_columns
    
    non_null_cells = df.notna().sum().sum()
    completeness = (non_null_cells / total_cells) * 100 if total_cells > 0 else 0
    
    type_consistency = 0
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            type_consistency += df[col].apply(lambda x: isinstance(x, (int, float))).sum()
        elif pd.api.types.is_datetime64_dtype(df[col]):
            type_consistency += df[col].notna().sum()
        else:
            type_consistency += df[col].apply(lambda x: isinstance(x, str)).sum()
    consistency = (type_consistency / non_null_cells) * 100 if non_null_cells > 0 else 0
    
    unique_records = len(df.drop_duplicates())
    uniqueness = (unique_records / total_records) * 100 if total_records > 0 else 0
    
    missing_ratio = (total_cells - non_null_cells) / total_cells if total_cells > 0 else 0
    duplicate_ratio = (original_rows - total_records) / original_rows if original_rows > 0 else 0
    
    sparse_columns = 0
    for col in df.columns:
        missing_pct = (df[col].isna().sum() / total_records) * 100 if total_records > 0 else 0
        if missing_pct > 50:
            sparse_columns += 1
    sparse_ratio = sparse_columns / total_columns if total_columns > 0 else 0
    
    fragmentation_index = (missing_ratio * 40 + duplicate_ratio * 30 + sparse_ratio * 30)
    
    column_quality = []
    for col in df.columns:
        missing_count = df[col].isna().sum()
        missing_pct = (missing_count / total_records) * 100 if total_records > 0 else 0
        unique_count = df[col].nunique()
        unique_pct = (unique_count / total_records) * 100 if total_records > 0 else 0
        
        if pd.api.types.is_numeric_dtype(df[col]):
            col_type = 'numeric'
            outliers = 0
        elif pd.api.types.is_datetime64_dtype(df[col]):
            col_type = 'datetime'
            outliers = 0
        else:
            col_type = 'text'
            outliers = 0
        
        if missing_pct < 5 and unique_pct > 80:
            grade = 'A'
        elif missing_pct < 15 and unique_pct > 50:
            grade = 'B'
        elif missing_pct < 30:
            grade = 'C'
        else:
            grade = 'D'
        
        column_quality.append({
            'name': col,
            'type': col_type,
            'missing_count': int(missing_count),
            'missing_pct': missing_pct,
            'unique_count': int(unique_count),
            'unique_pct': unique_pct,
            'outliers': outliers,
            'grade': grade
        })
    
    quality_score = (completeness * 0.4 + consistency * 0.3 + uniqueness * 0.3)
    
    return {
        'total_records': total_records,
        'total_columns': total_columns,
        'duplicates_removed': original_rows - total_records,
        'missing_values': int(total_cells - non_null_cells),
        'completeness': completeness,
        'consistency': consistency,
        'uniqueness': uniqueness,
        'fragmentation_index': fragmentation_index,
        'quality_score': quality_score,
        'unique_records': unique_records,
        'inconsistent_types': int(total_cells - type_consistency),
        'fragmented_columns': sparse_columns,
        'column_quality': column_quality,
        'quality_level': 'Excellent' if quality_score >= 80 else 'Good' if quality_score >= 60 else 'Moderate' if quality_score >= 40 else 'Poor',
        'needs_improvement': quality_score < 60
    }

@app.route('/', methods=['GET', 'POST'])
def upload():
    if request.method == 'POST':
        try:
            file = request.files['file']
            
            if file.filename == '':
                return jsonify({'error': 'No file selected'}), 400
            
            filename = file.filename
            ext = get_file_extension(filename)
            
            if ext not in app.config['ALLOWED_EXTENSIONS']:
                return jsonify({'error': f'Unsupported file type: {ext}'}), 400
            
            df = read_file(file, filename)
            original_rows = len(df)
            
            df = df.drop_duplicates()
            df = df.fillna("Unknown")
            
            table_html = df.to_html(classes='data-table', border=0, index=False, escape=False)
            
            quality_metrics = calculate_quality_metrics(df, original_rows)
            
            stats = {
                'file_type': ext.upper(),
                'original_rows': original_rows,
                'cleaned_rows': len(df),
                'duplicates_removed': original_rows - len(df),
                'columns': len(df.columns),
                'numeric_columns': len(df.select_dtypes(include=['number']).columns),
                'text_columns': len(df.select_dtypes(include=['object']).columns),
                'datetime_columns': len(df.select_dtypes(include=['datetime64']).columns),
                'quality_score': quality_metrics['quality_score'],
                'quality_level': quality_metrics['quality_level'],
                'completeness': quality_metrics['completeness'],
                'consistency': quality_metrics['consistency'],
                'uniqueness': quality_metrics['uniqueness'],
                'fragmentation_index': quality_metrics['fragmentation_index'],
                'missing_values_filled': int(quality_metrics['missing_values']),
                'inconsistent_types': quality_metrics['inconsistent_types'],
                'fragmented_columns': quality_metrics['fragmented_columns'],
                'column_quality': quality_metrics['column_quality'],
                'needs_improvement': quality_metrics['needs_improvement']
            }
            
            return render_template('result.html', table_html=table_html, stats=stats, filename=filename)
            
        except Exception as e:
            return jsonify({'error': f'Error processing file: {str(e)}'}), 500
    
    return render_template('index.html')

@app.route('/health')
def health():
    """Health check endpoint for cloud platforms"""
    return jsonify({'status': 'healthy'}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=False)