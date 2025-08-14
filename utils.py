import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

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
        'Feedback Created Date', 'case_created_date', 'Accept Time'  # Added Accept Time
    ]
    
    # Exclude numerical columns that contain time-related words but are actually numbers
    numerical_exclusions = [
        'First Response Time (min)', 'First Response Time (hours)', 
        'Agent Average Response Time', 'Wait Time', 'Chat Duration (sec)',
        'Days Since Last Response Time Stamp', 'Days Since Last Client Response',
        'Agent First Response Time (Seconds)', 'Agent Avg Response Time',
        'Age', 'Duration (Minutes)', 'AHT (End - Accept) (min)'  # Added messaging duration fields
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
                'Feedback Created Date', 'case_created_date', 'Accept Time'  # Added Accept Time
            ]
            
            # Define numerical columns that should stay as numbers
            numerical_columns = [
                'First Response Time (min)', 'First Response Time (hours)',
                'Wait Time', 'Chat Duration (sec)', 'Agent Average Response Time',
                'Age', 'Days Since Last Response Time Stamp', 'Days Since Last Client Response',
                'Duration (Minutes)', 'AHT (End - Accept) (min)'  # Added messaging duration fields
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
