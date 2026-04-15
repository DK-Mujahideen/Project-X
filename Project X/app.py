from flask import Flask, request, render_template, jsonify, session, send_file
import pandas as pd
import io
import json
import os
import traceback
from werkzeug.utils import secure_filename
import hashlib
from datetime import datetime
import re
from difflib import SequenceMatcher
import uuid

app = Flask(__name__)
app.secret_key = 'your-secret-key-here-12345'

# Configuration
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB
app.config['ALLOWED_EXTENSIONS'] = {
    'csv', 'xlsx', 'xls', 'json', 'txt', 'tsv', 'parquet', 'xml', 'html'
}

# Store session data for editing
edit_sessions = {}

port = int(os.environ.get('PORT', 5000))

def get_file_extension(filename):
    return filename.rsplit('.', 1)[1].lower() if '.' in filename else ''

def normalize_string(s):
    """Normalize string for comparison"""
    if pd.isna(s) or s == "Unknown":
        return ""
    return str(s).lower().strip().replace(" ", "").replace(".", "")

def fuzzy_match(str1, str2):
    """Calculate similarity ratio between two strings"""
    if not str1 or not str2:
        return 0
    return SequenceMatcher(None, str1.lower(), str2.lower()).ratio()

def compare_dataframes(df_old, df_new, key_column='customer_id'):
    """Compare two dataframes and return changes"""
    
    changes = {
        'added': [],
        'removed': [],
        'modified': [],
        'summary': {}
    }
    
    if key_column not in df_old.columns or key_column not in df_new.columns:
        return changes
    
    # Convert to string for comparison
    df_old[key_column] = df_old[key_column].astype(str)
    df_new[key_column] = df_new[key_column].astype(str)
    
    # Find added records (in new but not in old)
    old_keys = set(df_old[key_column])
    new_keys = set(df_new[key_column])
    
    added_keys = new_keys - old_keys
    removed_keys = old_keys - new_keys
    common_keys = old_keys & new_keys
    
    # Added records
    for key in added_keys:
        record = df_new[df_new[key_column] == key].iloc[0].to_dict()
        changes['added'].append(record)
    
    # Removed records
    for key in removed_keys:
        record = df_old[df_old[key_column] == key].iloc[0].to_dict()
        changes['removed'].append(record)
    
    # Modified records
    for key in common_keys:
        old_row = df_old[df_old[key_column] == key].iloc[0]
        new_row = df_new[df_new[key_column] == key].iloc[0]
        
        modified_fields = {}
        for col in df_old.columns:
            if col in df_new.columns:
                old_val = str(old_row[col]) if pd.notna(old_row[col]) else "Unknown"
                new_val = str(new_row[col]) if pd.notna(new_row[col]) else "Unknown"
                if old_val != new_val:
                    modified_fields[col] = {
                        'old': old_val,
                        'new': new_val
                    }
        
        if modified_fields:
            changes['modified'].append({
                'key': key,
                'fields': modified_fields,
                'old_record': old_row.to_dict(),
                'new_record': new_row.to_dict()
            })
    
    changes['summary'] = {
        'total_added': len(changes['added']),
        'total_removed': len(changes['removed']),
        'total_modified': len(changes['modified']),
        'total_unchanged': len(common_keys) - len(changes['modified'])
    }
    
    return changes

def find_similar_customers(df):
    """Find customers with similar names and phones but different emails"""
    potential_duplicates = []
    
    if 'name' not in df.columns:
        return []
    
    df['_norm_name'] = df['name'].apply(normalize_string)
    processed = set()
    
    for i, row1 in df.iterrows():
        if i in processed:
            continue
            
        similar_indices = [i]
        name1 = row1['_norm_name']
        
        if not name1:
            continue
            
        for j, row2 in df.iterrows():
            if j <= i or j in processed:
                continue
            
            name2 = row2['_norm_name']
            if not name2:
                continue
            
            name_similarity = fuzzy_match(name1, name2)
            
            if name_similarity > 0.8:
                similar_indices.append(j)
        
        if len(similar_indices) > 1:
            group = df.loc[similar_indices]
            
            phones = group['phone'].tolist() if 'phone' in df.columns else []
            unique_phones = [p for p in set(phones) if p != 'Unknown' and pd.notna(p)]
            
            emails = group['email'].tolist() if 'email' in df.columns else []
            unique_emails = [e for e in set(emails) if e != 'Unknown' and pd.notna(e)]
            
            confidence = "Low"
            if len(unique_phones) == 1 and len(unique_emails) > 1:
                confidence = "High (Same phone, different emails)"
            elif len(unique_emails) == 1 and len(unique_phones) > 1:
                confidence = "High (Same email, different phones)"
            elif len(unique_phones) > 1 and len(unique_emails) > 1:
                confidence = "Medium (Different contacts)"
            
            potential_duplicates.append({
                'name': group['name'].iloc[0],
                'phones': unique_phones,
                'emails': unique_emails,
                'customer_ids': group['customer_id'].tolist() if 'customer_id' in df.columns else [],
                'count': len(group),
                'confidence': confidence,
                'indices': similar_indices
            })
            
            processed.update(similar_indices)
    
    if '_norm_name' in df.columns:
        df.drop(columns=['_norm_name'], inplace=True)
    
    return potential_duplicates

def read_file(file, filename):
    """Read various file formats and return DataFrame"""
    ext = get_file_extension(filename)
    
    try:
        if ext == 'csv':
            df = pd.read_csv(file)
        elif ext in ['xlsx', 'xls', 'xlsm', 'xlsb']:
            if ext in ['xlsx', 'xlsm']:
                df = pd.read_excel(file, engine='openpyxl')
            else:
                df = pd.read_excel(file, engine='xlrd')
        elif ext == 'json':
            df = pd.read_json(file)
        elif ext == 'tsv':
            df = pd.read_csv(file, sep='\t')
        elif ext == 'txt':
            content = file.read().decode('utf-8')
            lines = content.split('\n')
            if ',' in lines[0]:
                df = pd.read_csv(io.StringIO(content))
            elif '\t' in lines[0]:
                df = pd.read_csv(io.StringIO(content), sep='\t')
            else:
                data = [line.strip() for line in lines if line.strip()]
                df = pd.DataFrame({'data': data})
        elif ext == 'xml':
            df = pd.read_xml(file)
        elif ext == 'html':
            tables = pd.read_html(file)
            if tables:
                df = tables[0]
            else:
                raise ValueError("No tables found in HTML file")
        elif ext == 'parquet':
            df = pd.read_parquet(file)
        else:
            raise ValueError(f"Unsupported file format: {ext}")
        
        for col in df.columns:
            if 'id' in col.lower() or col.lower() in ['customer_id', 'order_id', 'product_id']:
                df[col] = df[col].astype(str)
        
        return df
        
    except Exception as e:
        raise Exception(f"Error reading {ext.upper()} file: {str(e)}")

def detect_merge_key(dfs):
    """Detect common column to use as merge key"""
    if len(dfs) < 2:
        return None
    
    all_columns = set()
    for df in dfs:
        all_columns.update(df.columns)
    
    common_columns = []
    for col in all_columns:
        count = sum(1 for df in dfs if col in df.columns)
        if count >= 2:
            common_columns.append(col)
    
    id_columns = [col for col in common_columns if 'id' in col.lower()]
    if id_columns:
        return id_columns[0]
    
    return common_columns[0] if common_columns else None

def merge_dataframes(dfs, names, merge_on=None):
    """Merge multiple dataframes with type conversion"""
    if len(dfs) == 1:
        return dfs[0], {'message': 'Only one file provided. No merging performed.'}
    
    if not merge_on:
        merge_on = detect_merge_key(dfs)
    
    if not merge_on:
        merged_df = pd.concat(dfs, ignore_index=True)
        merge_info = {
            'method': 'concatenated',
            'message': 'No common key found. Files were stacked vertically.',
            'total_rows': len(merged_df),
            'total_columns': len(merged_df.columns)
        }
        return merged_df, merge_info
    
    dfs_copy = [df.copy() for df in dfs]
    
    for i, df in enumerate(dfs_copy):
        if merge_on in df.columns:
            dfs_copy[i][merge_on] = dfs_copy[i][merge_on].astype(str)
    
    merged_df = dfs_copy[0]
    merge_steps = []
    
    for i, df in enumerate(dfs_copy[1:], 1):
        if merge_on not in merged_df.columns or merge_on not in df.columns:
            original_rows = len(merged_df)
            merged_df = pd.concat([merged_df, df], ignore_index=True)
            merge_steps.append({
                'with_file': names[i],
                'merge_key': f'{merge_on} (MISSING - concatenated)',
                'rows_before': original_rows,
                'rows_after': len(merged_df)
            })
            continue
            
        try:
            original_rows = len(merged_df)
            merged_df = pd.merge(merged_df, df, on=merge_on, how='outer', suffixes=('', f'_{names[i]}'))
            new_rows = len(merged_df)
            merge_steps.append({
                'with_file': names[i],
                'merge_key': merge_on,
                'rows_before': original_rows,
                'rows_after': new_rows,
                'new_columns': list(df.columns)
            })
        except Exception as e:
            print(f"Error merging {names[i]}: {e}")
            merged_df = pd.concat([merged_df, df], ignore_index=True)
            merge_steps.append({
                'with_file': names[i],
                'merge_key': f'{merge_on} (FAILED - concatenated)',
                'rows_before': original_rows,
                'rows_after': len(merged_df)
            })
    
    merge_info = {
        'method': 'merged',
        'merge_key': merge_on,
        'steps': merge_steps,
        'total_rows': len(merged_df),
        'total_columns': len(merged_df.columns)
    }
    
    return merged_df, merge_info

def calculate_quality_metrics(df, original_rows):
    """Calculate comprehensive data quality metrics"""
    total_records = len(df)
    total_columns = len(df.columns)
    total_cells = total_records * total_columns
    
    non_null_cells = df.notna().sum().sum()
    completeness = (non_null_cells / total_cells) * 100 if total_cells > 0 else 0
    
    type_consistency = 0
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            type_consistency += df[col].apply(lambda x: isinstance(x, (int, float))).sum()
        else:
            type_consistency += df[col].notna().sum()
    consistency = (type_consistency / non_null_cells) * 100 if non_null_cells > 0 else 0
    
    unique_records = df.drop_duplicates().shape[0]
    uniqueness = (unique_records / total_records) * 100 if total_records > 0 else 0
    
    column_quality = []
    for col in df.columns:
        missing_count = df[col].isna().sum()
        missing_pct = (missing_count / total_records) * 100 if total_records > 0 else 0
        unique_count = df[col].nunique()
        unique_pct = (unique_count / total_records) * 100 if total_records > 0 else 0
        
        if pd.api.types.is_numeric_dtype(df[col]):
            col_type = 'numeric'
        elif pd.api.types.is_datetime64_dtype(df[col]):
            col_type = 'datetime'
        else:
            col_type = 'text'
        
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
            'missing_pct': missing_pct,
            'unique_pct': unique_pct,
            'grade': grade
        })
    
    missing_ratio = (total_cells - non_null_cells) / total_cells if total_cells > 0 else 0
    duplicate_ratio = (original_rows - total_records) / original_rows if original_rows > 0 else 0
    
    fragmentation_index = (missing_ratio * 50 + duplicate_ratio * 50)
    quality_score = completeness
    
    return {
        'total_records': total_records,
        'total_columns': total_columns,
        'completeness': completeness,
        'consistency': consistency,
        'uniqueness': uniqueness,
        'fragmentation_index': fragmentation_index,
        'quality_score': quality_score,
        'quality_level': 'Excellent' if quality_score >= 80 else 'Good' if quality_score >= 60 else 'Moderate' if quality_score >= 40 else 'Poor',
        'column_quality': column_quality,
        'duplicates_removed': original_rows - total_records,
        'missing_values_filled': int(total_cells - non_null_cells),
        'inconsistent_types': int(total_cells - type_consistency),
        'needs_improvement': quality_score < 60
    }

# ============== ROUTES ==============

@app.route('/')
def landing():
    """Landing page"""
    return render_template('landing.html')

@app.route('/analyzer', methods=['GET', 'POST'])
def analyzer():
    """Main upload page with all functionality"""
    if request.method == 'POST':
        try:
            print("=" * 50)
            print("Received POST request")
            
            if 'files' not in request.files:
                return jsonify({'error': 'No files part in the request'}), 400
            
            files = request.files.getlist('files')
            files = [f for f in files if f and f.filename]
            
            if not files:
                return jsonify({'error': 'No files selected'}), 400
            
            dfs = []
            names = []
            file_types = []
            
            for file in files:
                filename = file.filename
                ext = get_file_extension(filename)
                
                if ext not in app.config['ALLOWED_EXTENSIONS']:
                    return jsonify({'error': f'Unsupported file type: {ext}'}), 400
                
                df = read_file(file, filename)
                dfs.append(df)
                names.append(filename)
                file_types.append(ext.upper())
                print(f"  -> Loaded {len(df)} rows, {len(df.columns)} columns")
            
            merge_files = request.form.get('merge_files') == 'true'
            merge_key = request.form.get('merge_key', '')
            
            if merge_files and len(dfs) > 1:
                merged_df, merge_info = merge_dataframes(dfs, names, merge_key if merge_key else None)
                processing_info = {
                    'type': 'merged',
                    'details': merge_info,
                    'files_processed': len(dfs),
                    'file_names': names
                }
            else:
                merged_df = pd.concat(dfs, ignore_index=True)
                processing_info = {
                    'type': 'concatenated',
                    'details': {
                        'total_rows': len(merged_df),
                        'total_columns': len(merged_df.columns)
                    },
                    'files_processed': len(dfs),
                    'file_names': names
                }
            
            original_rows_before_clean = len(merged_df)
            
            # Find duplicate customers
            duplicate_customers = find_similar_customers(merged_df)
            
            # Clean data
            merged_df = merged_df.drop_duplicates()
            
            if 'customer_id' in merged_df.columns:
                completeness_score = merged_df.notna().sum(axis=1)
                merged_df['_temp_score'] = completeness_score
                merged_df = merged_df.sort_values('_temp_score', ascending=False)
                merged_df = merged_df.drop_duplicates(subset=['customer_id'], keep='first')
                merged_df = merged_df.drop(columns=['_temp_score'])
            
            if 'order_id' in merged_df.columns:
                merged_df = merged_df.drop_duplicates(subset=['order_id'], keep='first')
            
            merged_df = merged_df.fillna("Unknown")
            
            # Store in edit session
            session_id = str(uuid.uuid4())
            edit_sessions[session_id] = {
                'data': merged_df.to_json(),
                'original_rows': original_rows_before_clean,
                'created_at': datetime.now().isoformat()
            }
            
            table_html = merged_df.to_html(classes='data-table', border=0, index=False, escape=False)
            quality_metrics = calculate_quality_metrics(merged_df, original_rows_before_clean)
            
            stats = {
                'files_processed': len(dfs),
                'file_names': names,
                'file_types': file_types,
                'original_rows': original_rows_before_clean,
                'cleaned_rows': len(merged_df),
                'duplicates_removed': original_rows_before_clean - len(merged_df),
                'columns': len(merged_df.columns),
                'processing_info': processing_info,
                'quality_score': quality_metrics['quality_score'],
                'quality_level': quality_metrics['quality_level'],
                'completeness': quality_metrics['completeness'],
                'consistency': quality_metrics['consistency'],
                'uniqueness': quality_metrics['uniqueness'],
                'fragmentation_index': quality_metrics['fragmentation_index'],
                'column_quality': quality_metrics['column_quality'],
                'missing_values_filled': quality_metrics['missing_values_filled'],
                'inconsistent_types': quality_metrics['inconsistent_types'],
                'needs_improvement': quality_metrics['needs_improvement'],
                'duplicate_customers_found': len(duplicate_customers),
                'session_id': session_id
            }
            
            session['current_session_id'] = session_id
            
            return render_template('result.html', 
                                 table_html=table_html, 
                                 stats=stats,
                                 filename=f"{len(dfs)} files merged")
            
        except Exception as e:
            error_details = traceback.format_exc()
            print(f"Error: {str(e)}")
            print(error_details)
            return jsonify({'error': f'Error processing files: {str(e)}'}), 500
    
    return render_template('index.html')

@app.route('/edit-data', methods=['POST'])
def edit_data():
    """Edit specific cell in the data"""
    try:
        data = request.get_json()
        session_id = data.get('session_id')
        row_index = data.get('row_index')
        column = data.get('column')
        new_value = data.get('new_value')
        
        if session_id not in edit_sessions:
            return jsonify({'error': 'Session not found'}), 404
        
        df = pd.read_json(edit_sessions[session_id]['data'])
        
        if row_index >= len(df):
            return jsonify({'error': 'Row index out of range'}), 400
        
        if column not in df.columns:
            return jsonify({'error': 'Column not found'}), 400
        
        df.at[row_index, column] = new_value
        edit_sessions[session_id]['data'] = df.to_json()
        
        return jsonify({'success': True, 'message': 'Data updated successfully'})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/download-edited/<session_id>')
def download_edited(session_id):
    """Download the edited data as CSV"""
    try:
        if session_id not in edit_sessions:
            return jsonify({'error': 'Session not found'}), 404
        
        df = pd.read_json(edit_sessions[session_id]['data'])
        output = io.StringIO()
        df.to_csv(output, index=False)
        output.seek(0)
        
        return send_file(
            io.BytesIO(output.getvalue().encode()),
            mimetype='text/csv',
            as_attachment=True,
            download_name=f'edited_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get-data/<session_id>')
def get_data(session_id):
    """Get current data as JSON for editing interface"""
    try:
        if session_id not in edit_sessions:
            return jsonify({'error': 'Session not found'}), 404
        
        df = pd.read_json(edit_sessions[session_id]['data'])
        return jsonify({
            'columns': df.columns.tolist(),
            'data': df.replace('Unknown', '').fillna('').to_dict(orient='records'),
            'row_count': len(df)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/download-merged')
def download_merged():
    """Download the merged data as CSV"""
    try:
        session_id = session.get('current_session_id')
        if session_id and session_id in edit_sessions:
            df = pd.read_json(edit_sessions[session_id]['data'])
            output = io.StringIO()
            df.to_csv(output, index=False)
            output.seek(0)
            
            return send_file(
                io.BytesIO(output.getvalue().encode()),
                mimetype='text/csv',
                as_attachment=True,
                download_name=f'merged_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            )
        return jsonify({'error': 'No merged data available'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/health')
def health():
    return jsonify({'status': 'healthy'}), 200

if __name__ == '__main__':
    print("=" * 50)
    print("📊 Data Quality Analyzer - Started")
    print("=" * 50)
    print(f"📍 Landing Page: http://localhost:{port}/")
    print(f"📍 Analyzer Page: http://localhost:{port}/analyzer")
    print("✅ Supports file upload, merging, and editing")
    print("✅ Compare old vs new files with highlights")
    print("✅ Edit data before downloading")
    print("=" * 50)
    app.run(host='0.0.0.0', port=port, debug=True, threaded=True)
