from flask import Flask, render_template, request, jsonify, send_file, session
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import io
import os
import uuid
from werkzeug.utils import secure_filename
import json

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'  # Change this in production
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Create uploads directory if it doesn't exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

def excel_to_datetime(excel_date):
    """Convert Excel serial date to datetime"""
    if pd.isna(excel_date):
        return None
    try:
        if isinstance(excel_date, (int, float)):
            base_date = datetime(1899, 12, 30)
            return base_date + timedelta(days=excel_date)
        return excel_date
    except:
        return excel_date

def create_date_columns(df, date_col):
    """Create Month, Week, Day, Hours columns from a date column"""
    if date_col not in df.columns:
        return df
    
    df[date_col] = df[date_col].apply(excel_to_datetime)
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    
    df['Month'] = df[date_col].dt.strftime('%B %Y')
    df['Week'] = df[date_col].dt.isocalendar().week
    df['Day'] = df[date_col].dt.strftime('%A')
    df['Hours'] = df[date_col].dt.hour
    
    return df

def get_sheet_names(file_path):
    """Get all sheet names from an Excel file"""
    try:
        xl_file = pd.ExcelFile(file_path)
        return xl_file.sheet_names
    except:
        return []

def detect_data_type(df, filename=""):
    """Robustly detect data type with confidence scoring"""
    columns = [col.lower().strip() for col in df.columns]
    
    detection_rules = {
        'line_chat': {
            'required': ['chat transcript id', 'social account: name'],
            'strong_indicators': ['started by', 'open chat unique id', 'ready to assign (line)'],
            'channel_indicators': ['line'],
            'min_confidence': 0.7
        },
        'wechat_chat': {
            'required': ['wechat agent', 'follower name'],
            'strong_indicators': ['agent assigned time', 'agent first response time (seconds)'],
            'channel_indicators': ['wechat'],
            'min_confidence': 0.7
        },
        'live_chat': {
            'required': ['chat key', 'agent', 'start time'],
            'strong_indicators': ['visitor ip address', 'browser language', 'chat origin url'],
            'channel_indicators': ['sf', 'live'],
            'min_confidence': 0.6
        },
        'case_data': {
            'required': ['case number', 'case owner'],
            'strong_indicators': ['case reason', 'case status', 'case: created date/time'],
            'channel_indicators': ['case'],
            'min_confidence': 0.8
        }
    }
    
    results = {}
    
    for data_type, rules in detection_rules.items():
        confidence = 0.0
        matched_indicators = []
        
        required_matches = 0
        for req in rules['required']:
            if any(req in col for col in columns):
                required_matches += 1
                matched_indicators.append(f"Required: {req}")
        
        if required_matches == 0:
            results[data_type] = {'confidence': 0.0, 'indicators': []}
            continue
            
        confidence = (required_matches / len(rules['required'])) * 0.6
        
        strong_matches = 0
        for indicator in rules['strong_indicators']:
            if any(indicator in col for col in columns):
                strong_matches += 1
                matched_indicators.append(f"Strong: {indicator}")
        
        confidence += (strong_matches / len(rules['strong_indicators'])) * 0.3
        
        filename_lower = filename.lower()
        for channel in rules['channel_indicators']:
            if channel in filename_lower or any(channel in col for col in columns):
                confidence += 0.1
                matched_indicators.append(f"Channel: {channel}")
                break
        
        results[data_type] = {
            'confidence': min(confidence, 1.0),
            'indicators': matched_indicators
        }
    
    best_type = max(results.keys(), key=lambda x: results[x]['confidence'])
    best_confidence = results[best_type]['confidence']
    
    if best_confidence < detection_rules[best_type]['min_confidence']:
        return 'unknown', results
    
    return best_type, results

def smart_column_mapping(df, data_type):
    """Intelligently map columns with multiple fallback patterns"""
    columns = df.columns.tolist()
    column_mapping = {}
    
    mapping_patterns = {
        'Agent': [
            'Owner: Full Name', 'owner: full name',
            'WeChat Agent: Agent Nickname', 'wechat agent: agent nickname',
            'Agent', 'agent', 'Owner Name', 'owner name', 'Agent Name', 'agent name'
        ],
        'Chat Key': [
            'Chat Key', 'chat key', 'Number', 'number',
            'Chat Transcript ID', 'chat transcript id', 'ID', 'id'
        ],
        'Contact Name': [
            'Contact Name: Full Name', 'contact name: full name',
            'Follower Name', 'follower name', 'Contact Name', 'contact name'
        ],
        'Start Time': [
            'Start Time', 'start time', 'Request Time', 'request time',
            'Created Date', 'created date', 'Chat Start Time', 'chat start time'
        ]
    }
    
    for target_col, patterns in mapping_patterns.items():
        for pattern in patterns:
            if pattern in columns:
                column_mapping[target_col] = pattern
                break
            for col in columns:
                if col.lower() == pattern.lower():
                    column_mapping[target_col] = col
                    break
            if target_col in column_mapping:
                break
    
    if data_type == 'live_chat':
        column_mapping['Channel'] = 'SF'
    elif data_type == 'line_chat':
        column_mapping['Channel'] = 'LINE'
    elif data_type == 'wechat_chat':
        column_mapping['Channel'] = 'WeChat'
    
    return column_mapping

def process_chat_files(file_data_list):
    """Process chat files and return master_chat dataframe"""
    all_chat_data = []
    
    for file_info in file_data_list:
        df = file_info['data']
        detected_type = file_info['detected_type']
        
        if detected_type not in ['live_chat', 'line_chat', 'wechat_chat']:
            continue
        
        # Apply column mappings
        mapping = smart_column_mapping(df, detected_type)
        transformed = df.copy()
        
        for new_col, old_col in mapping.items():
            if old_col in transformed.columns:
                transformed[new_col] = transformed[old_col]
            elif isinstance(old_col, str) and old_col in ['SF', 'LINE', 'WeChat']:
                transformed[new_col] = old_col
        
        # Ensure required columns exist
        required_columns = [
            'Agent', 'Chat Key', 'Channel', 'Start Time', 'Contact Name',
            'Chat Transcript Name', 'Status', 'Chat Reason', 'End Time', 
            'Chat Duration (sec)', 'Wait Time', 'Post-Chat Rating'
        ]
        
        for col in required_columns:
            if col not in transformed.columns:
                transformed[col] = None
        
        # Create date columns
        date_columns = [col for col in transformed.columns if 'time' in col.lower() or 'date' in col.lower()]
        if date_columns:
            primary_date_col = date_columns[0]
            if primary_date_col in transformed.columns:
                transformed = create_date_columns(transformed, primary_date_col)
        
        all_chat_data.append(transformed)
    
    if not all_chat_data:
        return None
    
    # Combine all chat data
    master_chat = pd.concat(all_chat_data, ignore_index=True, sort=False)
    
    # Use exact Master_chat column order
    master_chat_columns_order = [
        'Agent', 'Chat Key', 'Chat Transcript Name', 'Status', 'Chat Reason',
        'Contact Name', 'Start Time', 'End Time', 'Chat Duration (sec)',
        'Wait Time', 'Post-Chat Rating', 'Chat Start Time', 'Ended By',
        'Type', 'Detail', 'Time', 'Regulation', 'Location',
        'Visitor IP Address', 'Chat Origin URL', 'Agent Message Count',
        'Visitor Message Count', 'Agent Average Response Time',
        'Billing Country', 'Browser Language', 'Created Date',
        'Owner Dept', 'Agent handling', 'Agent Dept', 'Day', 'Week',
        'Hours', 'Month', 'Actual Start Time', 'Actual End Time',
        'Actual Chat Duration (sec)', 'Actual Chat Duration (min)',
        'Channel', 'Start Time2', 'End Time3', 'Social Account: Name',
        'Ready To Assign (LINE)', 'Started By', 'Open Chat Unique ID',
        'Last Modified By: Full Name', 'Last Modified Date', 'Week ',
        'Agent Assigned Time', 'Created By: Full Name',
        'Agent First Response Time (Seconds)', 'Closed', 'Agent Avg Response Time'
    ]
    
    for col in master_chat_columns_order:
        if col not in master_chat.columns:
            master_chat[col] = None
    
    master_chat = master_chat[master_chat_columns_order]
    return master_chat

def process_case_files(file_data_list):
    """Process multiple case files and return combined master_case dataframe"""
    all_case_data = []
    
    for file_info in file_data_list:
        df = file_info['data']
        detected_type = file_info['detected_type']
        
        if detected_type != 'case_data':
            continue
        
        # Preserve Case Number as string
        if 'Case Number' in df.columns:
            df['Case Number'] = df['Case Number'].astype(str)
        
        # Convert Excel date fields
        date_columns = ['Case: Created Date/Time', 'Created Date', 'case_created_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: excel_to_datetime(x) if isinstance(x, (int, float)) and not pd.isna(x) else x
                )
        
        all_case_data.append(df)
    
    if not all_case_data:
        return None
    
    # Combine all case data
    combined_case = pd.concat(all_case_data, ignore_index=True, sort=False)
    
    # Use exact cases_main column order
    cases_main_columns_order = [
        'Account Billing Country', 'Case Number', 'Case Reason', 'Case Owner',
        'Account Name', 'Case Subject', 'Premium Client Qualified', 'Created By',
        'Case Creator', 'Age', 'Closed Reason', 'Source Email', 'To Email',
        'Case Record Type', 'Case: Created Date/Time', 'First Response',
        'Days Since Last Response Time Stamp', 'Days Since Last Client Response',
        'Case Origin', 'Case Creator: Alias', 'Case Owner Profile', 'Case: Closed',
        'First contact resolution', 'Created Date', 'Case Status', 'Owner Dept',
        'Age group', 'Month', 'Week', 'Day', 'Hours only', 'Hours Range',
        'Working hours (Y/N)', 'First Response Time (min)', 'First Response Time (hours)',
        'First Response Time Met', 'case_created_date'
    ]
    
    for col in cases_main_columns_order:
        if col not in combined_case.columns:
            combined_case[col] = None
    
    combined_case = combined_case[cases_main_columns_order]
    return combined_case

def process_rating_files(chat_file_path, chat_sheet, case_file_path, case_sheet):
    """Process rating files and return master_rating dataframe"""
    try:
        dfs = []
        
        if chat_file_path and chat_sheet:
            chat_rating_df = pd.read_excel(chat_file_path, sheet_name=chat_sheet)
            chat_transformed = pd.DataFrame()
            
            # Map APAC Chat Ratings to master_rating structure
            chat_transformed['Feedback Created Date'] = chat_rating_df.get('GetFeedback Response: Created Date')
            chat_transformed['Owner Name'] = chat_rating_df.get('GetFeedback Response: Owner Name')
            chat_transformed['Outcome'] = chat_rating_df.get('Outcome')
            chat_transformed['Rating'] = chat_rating_df.get('Post-Chat Rating')
            chat_transformed['Account: Billing Country'] = chat_rating_df.get('Account: Billing Country')
            chat_transformed['chat_case_number'] = chat_rating_df.get('Chat Transcript Name')
            chat_transformed['chat_case_id'] = chat_rating_df.get('ChatKey')
            chat_transformed['Language'] = chat_rating_df.get('Language')
            chat_transformed['Reason'] = 'Chat Feedback'
            chat_transformed['Month'] = chat_rating_df.get('Month')
            chat_transformed['Week '] = chat_rating_df.get('Week ')
            chat_transformed['Day'] = chat_rating_df.get('Day')
            chat_transformed['Team'] = chat_rating_df.get('Team')
            chat_transformed['PositivePctHelper'] = chat_rating_df.get('PositivePctHelper')
            chat_transformed['Source'] = 'Chat'
            
            dfs.append(chat_transformed)
        
        if case_file_path and case_sheet:
            case_rating_df = pd.read_excel(case_file_path, sheet_name=case_sheet)
            case_transformed = pd.DataFrame()
            
            # Map APAC Case Ratings to master_rating structure
            case_transformed['Feedback Created Date'] = case_rating_df.get('GetFeedback Response: Created Date')
            case_transformed['Owner Name'] = case_rating_df.get('GetFeedback Response: Owner Name')
            case_transformed['Outcome'] = case_rating_df.get('Outcome')
            case_transformed['Rating'] = case_rating_df.get('Case Satisfaction')
            case_transformed['Account: Billing Country'] = case_rating_df.get('Case: Account Billing Country')
            case_transformed['chat_case_number'] = case_rating_df.get('Case: Case Number')
            case_transformed['chat_case_id'] = case_rating_df.get('Case: Case ID')
            case_transformed['Language'] = case_rating_df.get('Language')
            case_transformed['Reason'] = case_rating_df.get('Case: Case Reason')
            case_transformed['Month'] = case_rating_df.get('Month')
            case_transformed['Week '] = case_rating_df.get('Week ')
            case_transformed['Day'] = case_rating_df.get('Day')
            case_transformed['Team'] = case_rating_df.get('Team')
            case_transformed['PositivePctHelper'] = case_rating_df.get('PositivePctHelper')
            case_transformed['Source'] = 'Case'
            
            dfs.append(case_transformed)
        
        if not dfs:
            return None
        
        master_rating = pd.concat(dfs, ignore_index=True, sort=False)
        
        # Ensure exact column order
        required_columns_order = [
            'Feedback Created Date', 'Owner Name', 'Outcome', 'Rating',
            'Account: Billing Country', 'chat_case_number', 'Language', 'Reason',
            'chat_case_id', 'Month', 'Week ', 'Day', 'Team', 'PositivePctHelper', 'Source'
        ]
        
        master_rating = master_rating[required_columns_order]
        
        if 'Feedback Created Date' in master_rating.columns:
            master_rating['Feedback Created Date'] = master_rating['Feedback Created Date'].apply(
                lambda x: excel_to_datetime(x) if isinstance(x, (int, float)) and not pd.isna(x) else x
            )
        
        return master_rating
        
    except Exception as e:
        return None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload_files', methods=['POST'])
def upload_files():
    """Handle file uploads and return sheet information"""
    files = request.files.getlist('files[]')
    file_info = []
    
    for file in files:
        if file and file.filename:
            filename = secure_filename(file.filename)
            unique_filename = f"{uuid.uuid4()}_{filename}"
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], unique_filename)
            file.save(file_path)
            
            sheets = get_sheet_names(file_path)
            file_info.append({
                'original_name': filename,
                'unique_name': unique_filename,
                'file_path': file_path,
                'sheets': sheets
            })
    
    return jsonify({'files': file_info})

@app.route('/analyze_sheets', methods=['POST'])
def analyze_sheets():
    """Analyze selected sheets and detect data types"""
    data = request.json
    selected_sheets = data.get('selected_sheets', [])
    
    analysis_results = []
    
    for sheet_info in selected_sheets:
        try:
            df = pd.read_excel(sheet_info['file_path'], sheet_name=sheet_info['sheet_name'])
            detected_type, all_results = detect_data_type(df, sheet_info['file_name'])
            
            result = {
                'file_name': sheet_info['file_name'],
                'sheet_name': sheet_info['sheet_name'],
                'file_path': sheet_info['file_path'],
                'detected_type': detected_type,
                'confidence': all_results[detected_type]['confidence'] if detected_type != 'unknown' else 0,
                'indicators': all_results[detected_type]['indicators'] if detected_type != 'unknown' else [],
                'rows': len(df),
                'columns': len(df.columns)
            }
            
            analysis_results.append(result)
            
        except Exception as e:
            analysis_results.append({
                'file_name': sheet_info['file_name'],
                'sheet_name': sheet_info['sheet_name'],
                'error': str(e)
            })
    
    return jsonify({'results': analysis_results})

@app.route('/process_chat', methods=['POST'])
def process_chat():
    """Process chat files and generate master_chat"""
    data = request.json
    confirmed_files = data.get('confirmed_files', [])
    
    try:
        # Load data for processing
        file_data_list = []
        for file_info in confirmed_files:
            if file_info['detected_type'] in ['live_chat', 'line_chat', 'wechat_chat']:
                df = pd.read_excel(file_info['file_path'], sheet_name=file_info['sheet_name'])
                file_data_list.append({
                    'data': df,
                    'detected_type': file_info['detected_type']
                })
        
        master_chat = process_chat_files(file_data_list)
        
        if master_chat is not None:
            # Save to session for download
            output_filename = f"master_chat_{uuid.uuid4()}.xlsx"
            output_path = os.path.join(app.config['UPLOAD_FOLDER'], output_filename)
            master_chat.to_excel(output_path, index=False)
            
            session['master_chat_file'] = output_filename
            
            return jsonify({
                'success': True,
                'rows': len(master_chat),
                'columns': len(master_chat.columns),
                'download_url': f'/download/master_chat/{output_filename}'
            })
        else:
            return jsonify({'success': False, 'error': 'No valid chat data processed'})
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/process_case', methods=['POST'])
def process_case():
    """Process case files and generate master_case"""
    data = request.json
    confirmed_files = data.get('confirmed_files', [])
    
    try:
        # Load data for processing
        file_data_list = []
        for file_info in confirmed_files:
            if file_info['detected_type'] == 'case_data' or file_info['detected_type'] == 'skip':
                if file_info['detected_type'] == 'skip':
                    continue
                df = pd.read_excel(file_info['file_path'], sheet_name=file_info['sheet_name'])
                file_data_list.append({
                    'data': df,
                    'detected_type': file_info['detected_type']
                })
        
        master_case = process_case_files(file_data_list)
        
        if master_case is not None:
            output_filename = f"master_case_{uuid.uuid4()}.xlsx"
            output_path = os.path.join(app.config['UPLOAD_FOLDER'], output_filename)
            master_case.to_excel(output_path, index=False)
            
            session['master_case_file'] = output_filename
            
            return jsonify({
                'success': True,
                'rows': len(master_case),
                'columns': len(master_case.columns),
                'download_url': f'/download/master_case/{output_filename}'
            })
        else:
            return jsonify({'success': False, 'error': 'No valid case data processed'})
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/process_rating', methods=['POST'])
def process_rating():
    """Process rating files and generate master_rating"""
    data = request.json
    chat_file_path = data.get('chat_file_path')
    chat_sheet = data.get('chat_sheet')
    case_file_path = data.get('case_file_path')
    case_sheet = data.get('case_sheet')
    
    try:
        master_rating = process_rating_files(chat_file_path, chat_sheet, case_file_path, case_sheet)
        
        if master_rating is not None:
            output_filename = f"master_rating_{uuid.uuid4()}.xlsx"
            output_path = os.path.join(app.config['UPLOAD_FOLDER'], output_filename)
            master_rating.to_excel(output_path, index=False)
            
            session['master_rating_file'] = output_filename
            
            return jsonify({
                'success': True,
                'rows': len(master_rating),
                'columns': len(master_rating.columns),
                'download_url': f'/download/master_rating/{output_filename}'
            })
        else:
            return jsonify({'success': False, 'error': 'Failed to process rating files'})
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/download/<file_type>/<filename>')
def download_file(file_type, filename):
    """Download processed files"""
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True, download_name=f"{file_type}.xlsx")
    else:
        return "File not found", 404

if __name__ == '__main__':
    app.run(debug=True)