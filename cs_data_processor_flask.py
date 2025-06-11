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
    """Convert Excel serial date to datetime - SIMPLIFIED VERSION"""
    if pd.isna(excel_date):
        return None
    try:
        if isinstance(excel_date, (int, float)):
            base_date = datetime(1899, 12, 30)
            return base_date + timedelta(days=excel_date)
        elif isinstance(excel_date, str):
            # Try to parse string dates - handle both DD/MM/YYYY and MM/DD/YYYY formats
            try:
                # First try DD/MM/YYYY format (more common internationally)
                return pd.to_datetime(excel_date, format='%d/%m/%Y', errors='raise')
            except:
                try:
                    # Then try MM/DD/YYYY format
                    return pd.to_datetime(excel_date, format='%m/%d/%Y', errors='raise')
                except:
                    # Finally try pandas auto-detection with dayfirst=True
                    return pd.to_datetime(excel_date, dayfirst=True, errors='coerce')
        return excel_date
    except:
        return excel_date

def standardize_date_columns(df):
    """Standardize date and datetime columns - SIMPLIFIED VERSION"""
    # Define known date/datetime columns for each data type
    date_columns = [
        'Start Time', 'End Time', 'Chat Start Time', 'Actual Start Time', 
        'Actual End Time', 'Last Modified Date', 'Agent Assigned Time',
        'Created Date', 'Case: Created Date/Time', 'First Response',
        'Feedback Created Date', 'case_created_date'
    ]
    
    # Exclude numerical columns that contain time-related words but are actually numbers
    numerical_exclusions = [
        'First Response Time (min)', 'First Response Time (hours)', 
        'Agent Average Response Time', 'Wait Time', 'Chat Duration (sec)',
        'Days Since Last Response Time Stamp', 'Days Since Last Client Response',
        'Agent First Response Time (Seconds)', 'Agent Avg Response Time',
        'Age'
    ]
    
    # Exclude text columns that might contain time-related words
    text_exclusions = [
        'First Response Time Met', 'Working hours (Y/N)', 'First contact resolution',
        'Case: Closed'
    ]
    
    for col in df.columns:
        # Skip if this is a numerical column that shouldn't be treated as date
        if col in numerical_exclusions or col in text_exclusions:
            continue
            
        if col in date_columns or any(keyword in col.lower() for keyword in ['date', 'time', 'created', 'modified']):
            # Additional check: skip columns with 'time' that also have 'min', 'sec', 'hours' (these are durations)
            if 'time' in col.lower() and any(duration in col.lower() for duration in ['min)', 'sec)', 'hour)', 'response']):
                continue
                
            if col in df.columns and not df[col].isna().all():
                try:
                    # Convert to datetime first - use dayfirst=True for international date formats
                    df[col] = df[col].apply(excel_to_datetime)
                    # Use dayfirst=True to handle DD/MM/YYYY format correctly
                    df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
                except Exception as e:
                    print(f"Warning: Could not convert column {col} to datetime: {e}")
                    continue
    
    return df

def save_excel_with_proper_formatting(df, output_path):
    """Save DataFrame to Excel - SIMPLIFIED VERSION LIKE YOUR WORKING CODE"""
    try:
        # Standardize date columns before saving
        df_formatted = standardize_date_columns(df.copy())
        
        # Use ExcelWriter for better control over formatting
        with pd.ExcelWriter(output_path, engine='openpyxl', date_format='YYYY-MM-DD HH:MM:SS') as writer:
            df_formatted.to_excel(writer, index=False, sheet_name='Sheet1')
            
            # Get the workbook and worksheet
            workbook = writer.book
            worksheet = writer.sheets['Sheet1']
            
            # Define date/datetime columns for specific formatting
            date_columns = [
                'Start Time', 'End Time', 'Chat Start Time', 'Actual Start Time', 
                'Actual End Time', 'Last Modified Date', 'Agent Assigned Time',
                'Created Date', 'Case: Created Date/Time', 'First Response',
                'Feedback Created Date', 'case_created_date'
            ]
            
            # Define numerical columns that should stay as numbers
            numerical_columns = [
                'First Response Time (min)', 'First Response Time (hours)',
                'Wait Time', 'Chat Duration (sec)', 'Agent Average Response Time',
                'Age', 'Days Since Last Response Time Stamp', 'Days Since Last Client Response'
            ]
            
            # Apply formatting to date columns only
            for col_idx, col_name in enumerate(df_formatted.columns, 1):
                if any(date_col.lower() in col_name.lower() for date_col in date_columns):
                    col_letter = worksheet.cell(row=1, column=col_idx).column_letter
                    for row in range(2, len(df_formatted) + 2):  # Skip header row
                        cell = worksheet[f'{col_letter}{row}']
                        if cell.value is not None and not pd.isna(cell.value):
                            cell.number_format = 'MM/DD/YYYY HH:MM:SS'
                elif any(num_col.lower() in col_name.lower() for num_col in numerical_columns):
                    # Force numerical columns to be numbers, not dates
                    col_letter = worksheet.cell(row=1, column=col_idx).column_letter
                    for row in range(2, len(df_formatted) + 2):  # Skip header row
                        cell = worksheet[f'{col_letter}{row}']
                        if cell.value is not None and not pd.isna(cell.value):
                            cell.number_format = '0.00'  # Number format
                            
    except Exception as e:
        print(f"Warning: Formatting failed, using basic save: {e}")
        # Fallback to basic save if formatting fails
        df.to_excel(output_path, index=False)

def create_date_columns(df, date_col):
    """Create Month, Week, Day, Hours columns from a date column"""
    if date_col not in df.columns:
        return df
    
    df[date_col] = df[date_col].apply(excel_to_datetime)
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    
    # Create calculated columns with proper formatting
    df['Month'] = df[date_col].dt.strftime('%B %Y')
    df['Week'] = df[date_col].dt.isocalendar().week.astype('Int64')  # Use nullable integer
    df['Day'] = df[date_col].dt.strftime('%A')
    
    # Create Hours column as time range (e.g., "2 PM - 3 PM")
    start_hour = df[date_col].dt.strftime('%I %p').str.lstrip('0')
    end_hour = (df[date_col] + pd.Timedelta(hours=1)).dt.strftime('%I %p').str.lstrip('0')
    df['Hours'] = start_hour + ' - ' + end_hour
    
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
    
    print(f"Processing {data_type} with columns: {columns}")
    
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
                print(f"Mapped {target_col} -> {pattern}")
                break
            for col in columns:
                if col.lower() == pattern.lower():
                    column_mapping[target_col] = col
                    print(f"Mapped {target_col} -> {col}")
                    break
            if target_col in column_mapping:
                break
    
    if data_type == 'live_chat':
        column_mapping['Channel'] = 'SF'
    elif data_type == 'line_chat':
        column_mapping['Channel'] = 'LINE'
    elif data_type == 'wechat_chat':
        column_mapping['Channel'] = 'WeChat'
    
    print(f"Final mapping: {column_mapping}")
    return column_mapping

def process_chat_files(file_data_list):
    """Process chat files and return master_chat dataframe"""
    all_chat_data = []
    
    for file_info in file_data_list:
        df = file_info['data']
        detected_type = file_info['detected_type']
        
        if detected_type not in ['live_chat', 'line_chat', 'wechat_chat']:
            continue
        
        print(f"Processing {detected_type} file with {len(df)} rows")
        
        # Apply column mappings
        mapping = smart_column_mapping(df, detected_type)
        transformed = df.copy()
        
        for new_col, old_col in mapping.items():
            if old_col in transformed.columns:
                transformed[new_col] = transformed[old_col]
                print(f"Copied {old_col} -> {new_col}")
                
                # Check Agent specifically
                if new_col == 'Agent':
                    agent_count = transformed[new_col].notna().sum()
                    print(f"Agent column has {agent_count} non-null values")
                    
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
        
        # Clean numerical columns (ensure they stay as numbers)
        numerical_columns = [
            'Wait Time', 'Chat Duration (sec)', 'Agent Average Response Time',
            'Agent Message Count', 'Visitor Message Count', 'Post-Chat Rating',
            'Agent First Response Time (Seconds)', 'Agent Avg Response Time'
        ]
        
        for col in numerical_columns:
            if col in transformed.columns:
                # Convert to numeric, replace non-numeric with blank/NaN
                transformed[col] = pd.to_numeric(transformed[col], errors='coerce')
                print(f"Cleaned numerical column: {col}")
        
        # Create date columns
        date_columns = [col for col in transformed.columns if 'time' in col.lower() or 'date' in col.lower()]
        # Exclude numerical time columns from date processing
        date_columns = [col for col in date_columns if col not in numerical_columns]
        
        if date_columns:
            primary_date_col = date_columns[0]
            if primary_date_col in transformed.columns:
                transformed = create_date_columns(transformed, primary_date_col)
        
        all_chat_data.append(transformed)
    
    if not all_chat_data:
        return None
    
    # Combine all chat data
    master_chat = pd.concat(all_chat_data, ignore_index=True, sort=False)
    
    # Final check
    final_agent_count = master_chat['Agent'].notna().sum()
    print(f"Final master_chat has {final_agent_count} agents out of {len(master_chat)} rows")
    
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
        
        print(f"Processing case file with {len(df)} rows")
        
        # Preserve Case Number as string
        if 'Case Number' in df.columns:
            df['Case Number'] = df['Case Number'].astype(str)
        
        # Define which columns should be treated as dates vs numbers vs text
        actual_date_columns = [
            'Case: Created Date/Time', 'Created Date', 'First Response'
        ]
        
        numerical_columns = [
            'First Response Time (min)', 'First Response Time (hours)',
            'Days Since Last Response Time Stamp', 'Days Since Last Client Response',
            'Age'
        ]
        
        # Text/boolean columns that should NOT be treated as numerical or dates
        text_columns = [
            'First Response Time Met', 'Working hours (Y/N)', 'First contact resolution',
            'Premium Client Qualified', 'Case Status', 'Case: Closed'
        ]
        
        # Clean numerical columns (ensure they stay as numbers)
        for col in numerical_columns:
            if col in df.columns:
                # Convert to numeric, replace non-numeric with blank/NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')
                print(f"Cleaned numerical column: {col}")
        
        # Ensure text columns stay as text (don't convert to numeric or dates)
        for col in text_columns:
            if col in df.columns:
                # Keep as text, just clean any Excel artifacts
                df[col] = df[col].astype(str).replace(['nan', 'NaT', 'None'], '')
                df[col] = df[col].replace('', None)  # Convert empty strings back to None/NaN
                print(f"Preserved text column: {col}")
        
        # Convert only actual date fields to datetime objects
        for col in actual_date_columns:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: excel_to_datetime(x) if isinstance(x, (int, float)) and not pd.isna(x) else x
                )
                df[col] = pd.to_datetime(df[col], errors='coerce')
                print(f"Converted date column: {col}")
        
        # Create case_created_date as date-only version of Case: Created Date/Time
        if 'Case: Created Date/Time' in df.columns:
            df['case_created_date'] = df['Case: Created Date/Time'].dt.date
            print("Created case_created_date from Case: Created Date/Time")
        elif 'Created Date' in df.columns:
            df['case_created_date'] = df['Created Date'].dt.date
            print("Created case_created_date from Created Date")
        
        # Create date component columns if we have a valid date column
        primary_date_col = None
        if 'Case: Created Date/Time' in df.columns and df['Case: Created Date/Time'].notna().any():
            primary_date_col = 'Case: Created Date/Time'
        elif 'Created Date' in df.columns and df['Created Date'].notna().any():
            primary_date_col = 'Created Date'
            
        if primary_date_col:
            # Create calculated date columns
            df['Month'] = df[primary_date_col].dt.strftime('%B %Y')
            df['Week'] = df[primary_date_col].dt.isocalendar().week.astype('Int64')
            df['Day'] = df[primary_date_col].dt.strftime('%A')
            df['Hours only'] = df[primary_date_col].dt.hour.astype('Int64')
            
            # Create Hours Range using the same logic as chats
            start_hour = df[primary_date_col].dt.strftime('%I %p').str.lstrip('0')
            end_hour = (df[primary_date_col] + pd.Timedelta(hours=1)).dt.strftime('%I %p').str.lstrip('0')
            df['Hours Range'] = start_hour + ' - ' + end_hour
            print(f"Created date components from {primary_date_col}")
        
        # Re-clean numerical columns AFTER date processing to ensure they stay numeric
        for col in numerical_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                print(f"Re-cleaned numerical column after date processing: {col}")
        
        # Re-preserve text columns AFTER date processing
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).replace(['nan', 'NaT', 'None'], '')
                df[col] = df[col].replace('', None)
                print(f"Re-preserved text column after date processing: {col}")
        
        all_case_data.append(df)
    
    if not all_case_data:
        return None
    
    # Combine all case data
    combined_case = pd.concat(all_case_data, ignore_index=True, sort=False)
    
    print(f"Final combined_case has {len(combined_case)} rows")
    
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
            print(f"Processing chat rating file: {chat_file_path}, sheet: {chat_sheet}")
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
            
            # Count non-null dates before processing
            date_count = chat_transformed['Feedback Created Date'].notna().sum()
            total_count = len(chat_transformed)
            print(f"Chat ratings: {date_count} valid dates out of {total_count} rows")
            
            dfs.append(chat_transformed)
        
        if case_file_path and case_sheet:
            print(f"Processing case rating file: {case_file_path}, sheet: {case_sheet}")
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
            
            # Count non-null dates before processing
            date_count = case_transformed['Feedback Created Date'].notna().sum()
            total_count = len(case_transformed)
            print(f"Case ratings: {date_count} valid dates out of {total_count} rows")
            
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
        
        # Add missing columns
        for col in required_columns_order:
            if col not in master_rating.columns:
                master_rating[col] = None
        
        master_rating = master_rating[required_columns_order]
        
        # Apply the same date standardization as chat and case processing
        master_rating = standardize_date_columns(master_rating)
        
        # Final check on the combined data
        final_date_count = master_rating['Feedback Created Date'].notna().sum()
        final_total = len(master_rating)
        print(f"Final master_rating: {final_date_count} valid dates out of {final_total} total rows")
        
        return master_rating
        
    except Exception as e:
        print(f"Error in process_rating_files: {e}")
        return None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload_files', methods=['POST'])
def upload_files():
    """Handle file uploads and return sheet information"""
    try:
        files = request.files.getlist('files[]')
        file_info = []
        
        if not files or files[0].filename == '':
            return jsonify({'success': False, 'error': 'No files uploaded'})
        
        for file in files:
            if file and file.filename:
                filename = secure_filename(file.filename)
                unique_filename = f"{uuid.uuid4()}_{filename}"
                file_path = os.path.join(app.config['UPLOAD_FOLDER'], unique_filename)
                
                try:
                    file.save(file_path)
                    sheets = get_sheet_names(file_path)
                    file_info.append({
                        'original_name': filename,
                        'unique_name': unique_filename,
                        'file_path': file_path,
                        'sheets': sheets
                    })
                except Exception as e:
                    return jsonify({'success': False, 'error': f'Error saving file {filename}: {str(e)}'})
        
        return jsonify({'success': True, 'files': file_info})
    
    except Exception as e:
        return jsonify({'success': False, 'error': f'Server error: {str(e)}'})

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
            # Save to session for download with proper formatting
            output_filename = f"master_chat_{uuid.uuid4()}.xlsx"
            output_path = os.path.join(app.config['UPLOAD_FOLDER'], output_filename)
            save_excel_with_proper_formatting(master_chat, output_path)
            
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
            save_excel_with_proper_formatting(master_case, output_path)
            
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
            save_excel_with_proper_formatting(master_rating, output_path)
            
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
