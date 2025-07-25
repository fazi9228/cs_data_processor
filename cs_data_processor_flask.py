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
    """Save DataFrame to Excel - FIXED VERSION TO PRESERVE TEXT DATA"""
    try:
        # ✅ FIX: Don't call standardize_date_columns which corrupts text data
        # The data is already properly processed by process_case_files
        df_formatted = df.copy()  # Just copy, don't standardize again
        
        # ✅ DEBUG: Check Created By before saving
        if 'Created By' in df_formatted.columns:
            created_by_count = df_formatted['Created By'].notna().sum()
            print(f"🔍 EXCEL SAVE: 'Created By' has {created_by_count} non-null values before saving")
            if created_by_count > 0:
                sample_values = df_formatted['Created By'].dropna().head(3).tolist()
                print(f"🔍 EXCEL SAVE: Sample Created By values: {sample_values}")
        
        # Use ExcelWriter for better control over formatting
        with pd.ExcelWriter(output_path, engine='openpyxl', date_format='YYYY-MM-DD HH:MM:SS') as writer:
            df_formatted.to_excel(writer, index=False, sheet_name='Sheet1')
            
            # Get the workbook and worksheet
            workbook = writer.book
            worksheet = writer.sheets['Sheet1']
            
            # ✅ UPDATED: Only format actual date columns, not text columns
            actual_date_columns = [
                'Case: Created Date/Time', 'Created Date', 'First Response',
                'Start Time', 'End Time', 'Chat Start Time', 'Actual Start Time', 
                'Actual End Time', 'Last Modified Date', 'Agent Assigned Time',
                'Feedback Created Date', 'case_created_date'
            ]
            
            # Define numerical columns that should stay as numbers
            numerical_columns = [
                'First Response Time (min)', 'First Response Time (hours)',
                'Wait Time', 'Chat Duration (sec)', 'Agent Average Response Time',
                'Age', 'Days Since Last Response Time Stamp', 'Days Since Last Client Response'
            ]
            
            # ✅ CRITICAL: Only apply date formatting to confirmed date columns
            for col_idx, col_name in enumerate(df_formatted.columns, 1):
                # Only format if it's definitely a date column
                if col_name in actual_date_columns:
                    col_letter = worksheet.cell(row=1, column=col_idx).column_letter
                    for row in range(2, len(df_formatted) + 2):  # Skip header row
                        cell = worksheet[f'{col_letter}{row}']
                        if cell.value is not None and not pd.isna(cell.value):
                            cell.number_format = 'MM/DD/YYYY HH:MM:SS'
                # Force numerical columns to be numbers, not dates
                elif any(num_col.lower() in col_name.lower() for num_col in numerical_columns):
                    col_letter = worksheet.cell(row=1, column=col_idx).column_letter
                    for row in range(2, len(df_formatted) + 2):  # Skip header row
                        cell = worksheet[f'{col_letter}{row}']
                        if cell.value is not None and not pd.isna(cell.value):
                            cell.number_format = '0.00'  # Number format
                # ✅ NEW: Leave text columns alone (including Created By, Case Creator)
                else:
                    # Don't apply any special formatting to text columns
                    pass
        
        # ✅ DEBUG: Verify the saved file
        print(f"✅ Excel file saved to: {output_path}")
        
        # Quick verification by reading back
        try:
            verification_df = pd.read_excel(output_path)
            if 'Created By' in verification_df.columns:
                saved_created_by_count = verification_df['Created By'].notna().sum()
                print(f"🔍 VERIFICATION: Saved file has {saved_created_by_count} non-null 'Created By' values")
                if saved_created_by_count > 0:
                    saved_sample = verification_df['Created By'].dropna().head(3).tolist()
                    print(f"🔍 VERIFICATION: Sample values in saved file: {saved_sample}")
                else:
                    print(f"❌ CRITICAL: Excel file was saved with null Created By values!")
        except Exception as verify_error:
            print(f"Could not verify saved file: {verify_error}")
                            
    except Exception as e:
        print(f"Warning: Formatting failed, using basic save: {e}")
        # ✅ IMPROVED: Even the fallback preserves data better
        try:
            df.to_excel(output_path, index=False)
            print(f"Used fallback Excel save to: {output_path}")
        except Exception as fallback_error:
            print(f"Even fallback save failed: {fallback_error}")
            raise

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
        ],
        # ✅ NEW: Map Team to Owner Dept for LINE and WeChat
        'Owner Dept': [
            'Owner Dept', 'owner dept',  # For Live Chat (SF)
            'Team', 'team',              # For LINE and WeChat  
            'Agent Dept', 'agent dept'   # Fallback option
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
    
    # Set channel-specific values
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
    total_source_rows = 0  # ✅ Track total source rows
    
    for file_info in file_data_list:
        df = file_info['data']
        detected_type = file_info['detected_type']
        
        if detected_type not in ['live_chat', 'line_chat', 'wechat_chat']:
            continue
        
        # ✅ TRACK SOURCE ROWS
        source_rows = len(df)
        total_source_rows += source_rows
        print(f"Processing {detected_type} file with {source_rows} rows")
        print(f"📊 Source rows for this file: {source_rows}")
        
        # Apply column mappings
        mapping = smart_column_mapping(df, detected_type)
        transformed = df.copy()
        
        for new_col, old_col in mapping.items():
            if old_col in transformed.columns:
                transformed[new_col] = transformed[old_col]
                print(f"Copied {old_col} -> {new_col}")
                
                # ✅ Check Owner Dept mapping specifically
                if new_col == 'Owner Dept':
                    dept_count = transformed[new_col].notna().sum()
                    print(f"✅ Owner Dept column has {dept_count} non-null values")
                    if dept_count > 0:
                        sample_depts = transformed[new_col].dropna().head(3).tolist()
                        print(f"✅ Sample Owner Dept values: {sample_depts}")
                
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
        
        # ✅ VERIFY ROW COUNT HASN'T CHANGED
        processed_rows = len(transformed)
        if processed_rows != source_rows:
            print(f"⚠️ WARNING: Row count changed! Source: {source_rows} → Processed: {processed_rows}")
            print(f"   Difference: {processed_rows - source_rows} rows")
        else:
            print(f"✅ Row count preserved: {processed_rows} rows")
        
        all_chat_data.append(transformed)
    
    if not all_chat_data:
        return None
    
    # Combine all chat data
    master_chat = pd.concat(all_chat_data, ignore_index=True, sort=False)
    
    # ✅ CRITICAL ROW COUNT VERIFICATION
    final_rows = len(master_chat)
    print(f"\n🔢 CHAT ROW COUNT VERIFICATION:")
    print(f"===============================")
    print(f"Total source rows across all files: {total_source_rows}")
    print(f"Final master_chat rows: {final_rows}")
    
    if final_rows == total_source_rows:
        print(f"✅ SUCCESS: Row counts match exactly!")
    else:
        row_difference = final_rows - total_source_rows
        if row_difference > 0:
            print(f"❌ ERROR: {row_difference} EXTRA rows in master file!")
            print(f"   This suggests data duplication or concatenation issues")
        else:
            print(f"❌ ERROR: {abs(row_difference)} MISSING rows from master file!")
            print(f"   This suggests data loss during processing")
        
        # Additional debugging for row count mismatch
        print(f"\n🔍 DEBUGGING ROW COUNT ISSUE:")
        print(f"   Check for:")
        print(f"   - Duplicate data in source files")
        print(f"   - Empty rows being filtered out")
        print(f"   - Date processing removing rows")
        print(f"   - Concatenation issues")
    
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
    
    # ✅ FINAL ROW COUNT CHECK
    if final_rows != total_source_rows:
        print(f"\n❌ CRITICAL: Chat row count mismatch detected!")
        print(f"   Source: {total_source_rows} rows")
        print(f"   Master: {final_rows} rows")
        print(f"   Stakeholder complaint about extra rows is VALID!")
    else:
        print(f"\n✅ Chat row count verification passed: {final_rows} rows")
    
    return master_chat

def process_case_files(file_data_list):
    """Process multiple case files and return combined master_case dataframe"""
    all_case_data = []
    total_source_rows = 0  # ✅ Track total source rows
    
    for file_info in file_data_list:
        df = file_info['data']
        detected_type = file_info['detected_type']
        
        if detected_type != 'case_data':
            continue
        
        # ✅ TRACK SOURCE ROWS
        source_rows = len(df)
        total_source_rows += source_rows
        print(f"Processing case file with {source_rows} rows")
        print(f"📊 Source rows for this file: {source_rows}")
        
        # ✅ DEBUG STEP 1: Check Created By immediately after reading Excel
        print(f"\n🔍 STEP 1 - After reading Excel file:")
        print(f"   DataFrame shape: {df.shape}")
        print(f"   Columns: {list(df.columns)}")
        if 'Created By' in df.columns:
            created_by_count_1 = df['Created By'].notna().sum()
            print(f"   ✅ 'Created By' column found with {created_by_count_1} non-null values")
            sample_values_1 = df['Created By'].dropna().head(3).tolist()
            print(f"   ✅ Sample values: {sample_values_1}")
        else:
            print(f"   ❌ 'Created By' column NOT found!")
            print(f"   Available columns with 'Created': {[col for col in df.columns if 'created' in col.lower()]}")
        
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
        
        # ✅ UPDATED: Include ALL text/name columns that should be preserved
        text_columns = [
            'First Response Time Met', 'Working hours (Y/N)', 'First contact resolution',
            'Premium Client Qualified', 'Case Status', 'Case: Closed', 
            'Created By', 'Case Creator', 'Case Owner', 'Account Name', 
            'Case Subject', 'Case Reason', 'Closed Reason', 'Source Email', 
            'To Email', 'Case Record Type', 'Case Origin', 'Case Creator: Alias',
            'Case Owner Profile', 'Owner Dept'  # ✅ Added all text fields
        ]
        
        # ✅ DEBUG STEP 2: Check Created By before text processing
        print(f"\n🔍 STEP 2 - Before text column processing:")
        if 'Created By' in df.columns:
            created_by_count_2 = df['Created By'].notna().sum()
            print(f"   ✅ 'Created By' has {created_by_count_2} non-null values")
            print(f"   Data type: {df['Created By'].dtype}")
        else:
            print(f"   ❌ 'Created By' column missing before text processing!")
        
        # Clean numerical columns (ensure they stay as numbers)
        for col in numerical_columns:
            if col in df.columns:
                # Convert to numeric, replace non-numeric with blank/NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')
                print(f"Cleaned numerical column: {col}")
        
        # ✅ UPDATED: More careful text column preservation
        for col in text_columns:
            if col in df.columns:
                print(f"Processing text column: {col}")
                
                # ✅ Special handling for Created By
                if col == 'Created By':
                    print(f"   🎯 SPECIAL: Processing 'Created By' column")
                    before_count = df[col].notna().sum()
                    print(f"   Before processing: {before_count} non-null values")
                    
                    # Be extra careful with Created By
                    original_values = df[col].copy()
                    df[col] = df[col].astype(str)
                    
                    # Only replace actual pandas NaN strings, not real data
                    df[col] = df[col].replace(['nan', 'NaT', 'None'], '')
                    # Convert empty strings to None, but preserve actual text values
                    df[col] = df[col].apply(lambda x: None if x == '' else x)
                    
                    after_count = df[col].notna().sum()
                    print(f"   After processing: {after_count} non-null values")
                    
                    if after_count != before_count:
                        print(f"   ❌ WARNING: Lost {before_count - after_count} values during text processing!")
                        # Show what changed
                        lost_indices = original_values.notna() & df[col].isna()
                        if lost_indices.any():
                            lost_values = original_values[lost_indices].head(5).tolist()
                            print(f"   Lost values: {lost_values}")
                else:
                    # Normal text processing for other columns
                    df[col] = df[col].astype(str)
                    df[col] = df[col].replace(['nan', 'NaT', 'None'], '')
                    df[col] = df[col].apply(lambda x: None if x == '' else x)
                
                print(f"Preserved text column: {col}")
        
        # ✅ DEBUG STEP 3: Check Created By after text processing
        print(f"\n🔍 STEP 3 - After text column processing:")
        if 'Created By' in df.columns:
            created_by_count_3 = df['Created By'].notna().sum()
            print(f"   ✅ 'Created By' has {created_by_count_3} non-null values")
            if created_by_count_3 > 0:
                sample_values_3 = df['Created By'].dropna().head(3).tolist()
                print(f"   ✅ Sample values: {sample_values_3}")
        else:
            print(f"   ❌ 'Created By' column missing after text processing!")
        
        # Convert only actual date fields to datetime objects
        for col in actual_date_columns:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: excel_to_datetime(x) if isinstance(x, (int, float)) and not pd.isna(x) else x
                )
                df[col] = pd.to_datetime(df[col], errors='coerce')
                print(f"Converted date column: {col}")
        
        # ✅ DEBUG STEP 4: Check Created By after date processing
        print(f"\n🔍 STEP 4 - After date processing:")
        if 'Created By' in df.columns:
            created_by_count_4 = df['Created By'].notna().sum()
            print(f"   ✅ 'Created By' has {created_by_count_4} non-null values")
        else:
            print(f"   ❌ 'Created By' column missing after date processing!")
        
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
        
        # ✅ DEBUG STEP 5: Final check before adding to combined data
        print(f"\n🔍 STEP 5 - Before adding to combined data:")
        if 'Created By' in df.columns:
            final_created_by_count = df['Created By'].notna().sum()
            print(f"   ✅ 'Created By' has {final_created_by_count} non-null values")
            if final_created_by_count > 0:
                sample_values_final = df['Created By'].dropna().head(3).tolist()
                print(f"   ✅ Sample values: {sample_values_final}")
            else:
                print(f"   ❌ WARNING: 'Created By' column exists but all values are null!")
        else:
            print(f"   ❌ 'Created By' column completely missing!")
        
        # ✅ VERIFY ROW COUNT HASN'T CHANGED
        processed_rows = len(df)
        if processed_rows != source_rows:
            print(f"⚠️ WARNING: Row count changed! Source: {source_rows} → Processed: {processed_rows}")
            print(f"   Difference: {processed_rows - source_rows} rows")
        else:
            print(f"✅ Row count preserved: {processed_rows} rows")
        
        all_case_data.append(df)
    
    if not all_case_data:
        return None
    
    # ✅ DEBUG STEP 6: Check Created By before concatenation
    print(f"\n🔍 STEP 6 - Before pd.concat:")
    for i, df in enumerate(all_case_data):
        if 'Created By' in df.columns:
            count = df['Created By'].notna().sum()
            print(f"   DataFrame {i}: 'Created By' has {count} non-null values")
        else:
            print(f"   DataFrame {i}: 'Created By' column missing!")
    
    # Combine all case data
    combined_case = pd.concat(all_case_data, ignore_index=True, sort=False)
    
    # ✅ DEBUG STEP 7: Check Created By after concatenation
    print(f"\n🔍 STEP 7 - After pd.concat:")
    if 'Created By' in combined_case.columns:
        concat_created_by_count = combined_case['Created By'].notna().sum()
        print(f"   ✅ Combined 'Created By' has {concat_created_by_count} non-null values")
    else:
        print(f"   ❌ 'Created By' column missing after concat!")
        print(f"   Available columns: {list(combined_case.columns)}")
    
    # ✅ CRITICAL ROW COUNT VERIFICATION
    final_rows = len(combined_case)
    print(f"\n🔢 ROW COUNT VERIFICATION:")
    print(f"===========================")
    print(f"Total source rows across all files: {total_source_rows}")
    print(f"Final master_case rows: {final_rows}")
    
    if final_rows == total_source_rows:
        print(f"✅ SUCCESS: Row counts match exactly!")
    else:
        row_difference = final_rows - total_source_rows
        if row_difference > 0:
            print(f"❌ ERROR: {row_difference} EXTRA rows in master file!")
            print(f"   This suggests data duplication or concatenation issues")
        else:
            print(f"❌ ERROR: {abs(row_difference)} MISSING rows from master file!")
            print(f"   This suggests data loss during processing")
        
        # Additional debugging for row count mismatch
        print(f"\n🔍 DEBUGGING ROW COUNT ISSUE:")
        print(f"   Check for:")
        print(f"   - Duplicate data in source files")
        print(f"   - Empty rows being filtered out")
        print(f"   - Date processing removing rows")
        print(f"   - Concatenation issues")
    
    print(f"Final combined_case has {len(combined_case)} rows")
    
    # ✅ UPDATED: More careful column addition - only add if truly missing
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
    
    # ✅ DEBUG STEP 8: Check Created By before column ordering
    print(f"\n🔍 STEP 8 - Before column ordering:")
    if 'Created By' in combined_case.columns:
        pre_order_count = combined_case['Created By'].notna().sum()
        print(f"   ✅ 'Created By' has {pre_order_count} non-null values before ordering")
    else:
        print(f"   ❌ 'Created By' column missing before ordering!")
    
    # ✅ CAREFUL: Only add columns that are truly missing, don't overwrite existing data
    for col in cases_main_columns_order:
        if col not in combined_case.columns:
            combined_case[col] = None
            print(f"Added missing column: {col}")
        elif col == 'Created By':
            # Extra check for Created By
            existing_count = combined_case[col].notna().sum()
            print(f"'Created By' already exists with {existing_count} non-null values - NOT overwriting")
    
    # ✅ FINAL VERIFICATION: Check preserved data
    if 'Case Creator' in combined_case.columns:
        creator_count = combined_case['Case Creator'].notna().sum()
        created_by_count = combined_case['Created By'].notna().sum()
        total_rows = len(combined_case)
        print(f"✅ Final verification:")
        print(f"   Created By: {created_by_count} non-null values")
        print(f"   Case Creator: {creator_count} non-null values")
        print(f"   Total rows: {total_rows}")
        
        # Show sample values for debugging
        if creator_count > 0:
            sample_creators = combined_case['Case Creator'].dropna().head(3).tolist()
            print(f"✅ Sample Case Creator values: {sample_creators}")
        if created_by_count > 0:
            sample_created_by = combined_case['Created By'].dropna().head(3).tolist()
            print(f"✅ Sample Created By values: {sample_created_by}")
        else:
            print(f"❌ CRITICAL: Created By has NO values - this is the bug!")
        
        # ✅ FINAL ROW COUNT CHECK
        if final_rows != total_source_rows:
            print(f"\n❌ CRITICAL: Row count mismatch detected!")
            print(f"   Source: {total_source_rows} rows")
            print(f"   Master: {final_rows} rows")
            print(f"   Stakeholder complaint about extra rows is VALID!")
        else:
            print(f"\n✅ Row count verification passed: {final_rows} rows")
    
    # ✅ DEBUG STEP 9: Check Created By after column ordering
    print(f"\n🔍 STEP 9 - After column ordering:")
    combined_case = combined_case[cases_main_columns_order]
    
    if 'Created By' in combined_case.columns:
        final_created_by_count = combined_case['Created By'].notna().sum()
        print(f"   ✅ Final 'Created By' has {final_created_by_count} non-null values")
        if final_created_by_count == 0:
            print(f"   ❌ FOUND THE BUG: Created By column exists but all values are null after ordering!")
    else:
        print(f"   ❌ 'Created By' column missing after final ordering!")
    
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
