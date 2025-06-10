import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import io
from typing import Dict, List, Optional

# Set page configuration
st.set_page_config(
    page_title="CS Dashboard Data Processor",
    page_icon="📊",
    layout="wide"
)

def excel_to_datetime(excel_date):
    """Convert Excel serial date to datetime"""
    if pd.isna(excel_date):
        return None
    try:
        # Excel epoch starts from 1900-01-01, but has a leap year bug
        # So we use 1899-12-30 as the base
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
    
    # Convert to datetime if needed
    df[date_col] = df[date_col].apply(excel_to_datetime)
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    
    # Create calculated columns
    df['Month'] = df[date_col].dt.strftime('%B %Y')
    df['Week'] = df[date_col].dt.isocalendar().week
    df['Day'] = df[date_col].dt.strftime('%A')
    df['Hours'] = df[date_col].dt.hour
    
    return df

def get_sheet_names(file):
    """Get all sheet names from an Excel file"""
    try:
        xl_file = pd.ExcelFile(file)
        return xl_file.sheet_names
    except:
        return []

def detect_data_type(df, filename=""):
    """Robustly detect data type with confidence scoring"""
    columns = [col.lower().strip() for col in df.columns]
    column_set = set(columns)
    
    # Define very specific indicators for each type
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
        },
        'chat_rating': {
            'required': ['rating', 'chatkey'],
            'strong_indicators': ['post-chat rating', 'getfeedback'],
            'channel_indicators': ['chat'],
            'min_confidence': 0.7
        },
        'case_rating': {
            'required': ['rating', 'case'],
            'strong_indicators': ['feedback created date', 'outcome'],
            'channel_indicators': ['case'],
            'min_confidence': 0.7
        }
    }
    
    results = {}
    
    for data_type, rules in detection_rules.items():
        confidence = 0.0
        matched_indicators = []
        
        # Check required fields (must have at least one)
        required_matches = 0
        for req in rules['required']:
            if any(req in col for col in columns):
                required_matches += 1
                matched_indicators.append(f"Required: {req}")
        
        if required_matches == 0:
            results[data_type] = {'confidence': 0.0, 'indicators': []}
            continue
            
        # Base confidence from required matches
        confidence = (required_matches / len(rules['required'])) * 0.6
        
        # Bonus from strong indicators
        strong_matches = 0
        for indicator in rules['strong_indicators']:
            if any(indicator in col for col in columns):
                strong_matches += 1
                matched_indicators.append(f"Strong: {indicator}")
        
        confidence += (strong_matches / len(rules['strong_indicators'])) * 0.3
        
        # Bonus from channel indicators in filename or columns
        filename_lower = filename.lower()
        channel_bonus = 0
        for channel in rules['channel_indicators']:
            if channel in filename_lower or any(channel in col for col in columns):
                channel_bonus = 0.1
                matched_indicators.append(f"Channel: {channel}")
                break
        
        confidence += channel_bonus
        
        results[data_type] = {
            'confidence': min(confidence, 1.0),
            'indicators': matched_indicators
        }
    
    # Find best match
    best_type = max(results.keys(), key=lambda x: results[x]['confidence'])
    best_confidence = results[best_type]['confidence']
    
    if best_confidence < detection_rules[best_type]['min_confidence']:
        return 'unknown', results
    
    return best_type, results

def validate_column_mapping(df, data_type, proposed_mapping):
    """Validate that the proposed column mapping makes sense"""
    issues = []
    
    # Check for critical missing mappings
    if data_type in ['line_chat', 'wechat_chat', 'live_chat']:
        if 'Agent' not in proposed_mapping or proposed_mapping['Agent'] not in df.columns:
            issues.append("❌ Could not find Agent/Owner column")
        if 'Chat Key' not in proposed_mapping or proposed_mapping['Chat Key'] not in df.columns:
            issues.append("❌ Could not find Chat Key/Number column")
    
    # Check data quality in mapped columns
    for new_col, old_col in proposed_mapping.items():
        if old_col in df.columns:
            null_pct = df[old_col].isna().sum() / len(df) * 100
            if null_pct > 90:
                issues.append(f"⚠️ Column '{old_col}' is mostly empty ({null_pct:.1f}% null)")
    
    return issues

def smart_column_mapping(df, data_type):
    """Intelligently map columns with multiple fallback patterns"""
    columns = df.columns.tolist()
    column_mapping = {}
    
    # Define mapping patterns with priority order
    mapping_patterns = {
        'Agent': [
            'Owner: Full Name', 'owner: full name',
            'WeChat Agent: Agent Nickname', 'wechat agent: agent nickname',
            'Agent', 'agent',
            'Owner Name', 'owner name',
            'Agent Name', 'agent name'
        ],
        'Chat Key': [
            'Chat Key', 'chat key',
            'Number', 'number',
            'Chat Transcript ID', 'chat transcript id',
            'ID', 'id'
        ],
        'Contact Name': [
            'Contact Name: Full Name', 'contact name: full name',
            'Follower Name', 'follower name',
            'Contact Name', 'contact name',
            'Customer Name', 'customer name'
        ],
        'Start Time': [
            'Start Time', 'start time',
            'Request Time', 'request time',
            'Created Date', 'created date',
            'Chat Start Time', 'chat start time'
        ]
    }
    
    # Find best matches
    for target_col, patterns in mapping_patterns.items():
        for pattern in patterns:
            # Try exact match first
            if pattern in columns:
                column_mapping[target_col] = pattern
                break
            # Try case-insensitive match
            for col in columns:
                if col.lower() == pattern.lower():
                    column_mapping[target_col] = col
                    break
            if target_col in column_mapping:
                break
    
    # Add channel identifier
    if data_type == 'live_chat':
        column_mapping['Channel'] = 'SF'
    elif data_type == 'line_chat':
        column_mapping['Channel'] = 'LINE'
    elif data_type == 'wechat_chat':
        column_mapping['Channel'] = 'WeChat'
    
    return column_mapping

def preview_detection_results(files_and_sheets_data):
    """Show detection results for user confirmation before processing"""
    st.subheader("🔍 Data Detection Results")
    st.markdown("**Please review and confirm the detected data types before processing:**")
    
    detection_results = []
    
    for item in files_and_sheets_data:
        filename = item['name']
        df = item['data']
        
        # Detect data type
        detected_type, all_results = detect_data_type(df, filename)
        
        # Get column mapping
        if detected_type != 'unknown':
            column_mapping = smart_column_mapping(df, detected_type)
            validation_issues = validate_column_mapping(df, detected_type, column_mapping)
        else:
            column_mapping = {}
            validation_issues = ["❓ Could not reliably detect data type"]
        
        detection_results.append({
            'filename': filename,
            'detected_type': detected_type,
            'confidence': all_results[detected_type]['confidence'] if detected_type != 'unknown' else 0,
            'indicators': all_results[detected_type]['indicators'] if detected_type != 'unknown' else [],
            'column_mapping': column_mapping,
            'issues': validation_issues,
            'data': df
        })
    
    # Display results in a table format
    for i, result in enumerate(detection_results):
        with st.expander(f"📄 {result['filename']} - {result['detected_type'].replace('_', ' ').title()}", expanded=True):
            col1, col2 = st.columns([1, 1])
            
            with col1:
                st.write(f"**Confidence:** {result['confidence']:.1%}")
                st.write(f"**Rows:** {len(result['data']):,}")
                st.write(f"**Columns:** {len(result['data'].columns)}")
                
                if result['indicators']:
                    st.write("**Detection reasons:**")
                    for indicator in result['indicators']:
                        st.write(f"• {indicator}")
            
            with col2:
                if result['column_mapping']:
                    st.write("**Column mappings:**")
                    for new_col, old_col in result['column_mapping'].items():
                        if old_col in result['data'].columns:
                            st.write(f"• {new_col} ← `{old_col}`")
                        elif isinstance(old_col, str):
                            st.write(f"• {new_col} ← `{old_col}` (constant)")
            
            # Show issues if any
            if result['issues']:
                st.write("**Issues found:**")
                for issue in result['issues']:
                    st.write(issue)
            
            # Allow manual override
            manual_type = st.selectbox(
                f"Override data type for {result['filename']}:",
                ['auto-detected', 'live_chat', 'line_chat', 'wechat_chat', 'case_data', 'chat_rating', 'case_rating', 'skip'],
                key=f"manual_type_{i}"
            )
            
            if manual_type != 'auto-detected':
                result['detected_type'] = manual_type
    
    return detection_results

def flexible_transform_chat(df, data_type):
    """Flexible chat transformation that adapts to different column structures"""
    transformed = df.copy()
    
    # Get intelligent column mapping
    mapping = smart_column_mapping(df, data_type)
    
    # Apply column mappings
    for new_col, old_col in mapping.items():
        if old_col in transformed.columns:
            transformed[new_col] = transformed[old_col]
        elif isinstance(old_col, str) and old_col in ['SF', 'LINE', 'WeChat']:
            # This is a constant value for Channel
            transformed[new_col] = old_col
    
    # Ensure we have basic required columns
    required_columns = [
        'Agent', 'Chat Key', 'Channel', 'Start Time', 'Contact Name',
        'Chat Transcript Name', 'Status', 'Chat Reason', 'End Time', 
        'Chat Duration (sec)', 'Wait Time', 'Post-Chat Rating'
    ]
    
    for col in required_columns:
        if col not in transformed.columns:
            transformed[col] = None
    
    # Create date columns from any date field we can find
    date_columns = [col for col in transformed.columns if 'time' in col.lower() or 'date' in col.lower()]
    if date_columns:
        primary_date_col = date_columns[0]  # Use first available date column
        if primary_date_col in transformed.columns:
            transformed = create_date_columns(transformed, primary_date_col)
    
    return transformed

def flexible_process_chat_data_confirmed(detection_results):
    """Process chat data using confirmed detection results with exact Master_chat column order"""
    all_chat_data = []
    
    for result in detection_results:
        if result['detected_type'] == 'skip':
            st.info(f"⏭️ Skipping {result['filename']} as requested")
            continue
            
        if result['detected_type'] not in ['live_chat', 'line_chat', 'wechat_chat']:
            st.warning(f"⚠️ {result['filename']} is not chat data ({result['detected_type']}). Skipping...")
            continue
        
        if result['issues']:
            st.warning(f"⚠️ Processing {result['filename']} with known issues:")
            for issue in result['issues']:
                st.write(f"  {issue}")
        
        # Transform the data using confirmed mappings
        transformed = flexible_transform_chat(result['data'], result['detected_type'])
        all_chat_data.append(transformed)
        
        st.success(f"✅ {result['detected_type'].replace('_', ' ').title()} processed: {len(result['data'])} rows from {result['filename']}")
    
    if not all_chat_data:
        st.error("❌ No valid chat data found after confirmation!")
        return None
    
    # Combine all chat data
    master_chat = pd.concat(all_chat_data, ignore_index=True, sort=False)
    
    # Use EXACT Master_chat column order from your original file
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
    
    # Ensure all required columns exist and are in exact order
    for col in master_chat_columns_order:
        if col not in master_chat.columns:
            master_chat[col] = None
    
    # Reorder to match exact Master_chat structure
    master_chat = master_chat[master_chat_columns_order]
    
    st.success(f"🎉 Master Chat created successfully: {len(master_chat)} total rows")
    return master_chat

def process_case_file(case_file, case_sheet):
    """Process case file with exact cases_main column order"""
    try:
        case_df = pd.read_excel(case_file, sheet_name=case_sheet)
        
        st.info(f"📊 Loaded case file: {len(case_df)} rows, {len(case_df.columns)} columns")
        
        # Check if we have data
        if case_df.empty:
            st.error("❌ The selected sheet appears to be empty!")
            return None
        
        # Show first few columns for debugging
        st.write("📋 First few columns detected:", list(case_df.columns[:5]))
        
        # Preserve Case Number as string to maintain leading zeros
        if 'Case Number' in case_df.columns:
            case_df['Case Number'] = case_df['Case Number'].astype(str)
        
        # Convert Excel date fields to proper datetime while preserving existing calculated fields
        date_columns = ['Case: Created Date/Time', 'Created Date', 'case_created_date']
        for col in date_columns:
            if col in case_df.columns:
                # Only convert if it's an Excel serial number
                case_df[col] = case_df[col].apply(
                    lambda x: excel_to_datetime(x) if isinstance(x, (int, float)) and not pd.isna(x) else x
                )
        
        # Use EXACT cases_main column order from your original file
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
        
        # Check which columns exist vs required
        existing_cols = set(case_df.columns)
        required_cols = set(cases_main_columns_order)
        
        missing_cols = required_cols - existing_cols
        extra_cols = existing_cols - required_cols
        
        if missing_cols:
            st.warning(f"⚠️ Missing columns (will be added as empty): {missing_cols}")
        if extra_cols:
            st.info(f"ℹ️ Extra columns (will be removed): {extra_cols}")
        
        # Ensure all required columns exist
        for col in cases_main_columns_order:
            if col not in case_df.columns:
                case_df[col] = None
        
        # Reorder to match exact cases_main structure - only keep required columns
        case_df = case_df[cases_main_columns_order]
        
        st.success(f"✅ Cases processed: {len(case_df)} rows from sheet '{case_sheet}'")
        st.info(f"📊 Output structure: {len(case_df.columns)} columns, {len(case_df)} rows")
        
        return case_df
        
    except Exception as e:
        st.error(f"❌ Error processing case file: {str(e)}")
        st.error(f"Full error details: {repr(e)}")
        return None

def process_rating_files(chat_rating_file, case_rating_file, 
                        chat_rating_sheet, case_rating_sheet):
    """Process and combine rating files with exact master_rating structure"""
    try:
        dfs = []
        
        if chat_rating_file is not None and chat_rating_sheet:
            chat_rating_df = pd.read_excel(chat_rating_file, sheet_name=chat_rating_sheet)
            
            # Map APAC Chat Ratings to master_rating structure
            chat_transformed = pd.DataFrame()
            
            # Column mappings for APAC Chat Ratings
            chat_transformed['Feedback Created Date'] = chat_rating_df.get('GetFeedback Response: Created Date')
            chat_transformed['Owner Name'] = chat_rating_df.get('GetFeedback Response: Owner Name')
            chat_transformed['Outcome'] = chat_rating_df.get('Outcome')
            chat_transformed['Rating'] = chat_rating_df.get('Post-Chat Rating')
            chat_transformed['Account: Billing Country'] = chat_rating_df.get('Account: Billing Country')
            
            # Key mappings as specified:
            # chat_case_number from Chat Transcript Name
            # chat_case_id from ChatKey
            chat_transformed['chat_case_number'] = chat_rating_df.get('Chat Transcript Name')
            chat_transformed['chat_case_id'] = chat_rating_df.get('ChatKey')
            
            chat_transformed['Language'] = chat_rating_df.get('Language')
            chat_transformed['Reason'] = 'Chat Feedback'  # Default for chat ratings
            chat_transformed['Month'] = chat_rating_df.get('Month')
            chat_transformed['Week '] = chat_rating_df.get('Week ')  # Note the space in 'Week '
            chat_transformed['Day'] = chat_rating_df.get('Day')
            chat_transformed['Team'] = chat_rating_df.get('Team')
            chat_transformed['PositivePctHelper'] = chat_rating_df.get('PositivePctHelper')
            chat_transformed['Source'] = 'Chat'
            
            dfs.append(chat_transformed)
            st.success(f"✅ Chat Rating processed: {len(chat_transformed)} rows from sheet '{chat_rating_sheet}'")
        
        if case_rating_file is not None and case_rating_sheet:
            case_rating_df = pd.read_excel(case_rating_file, sheet_name=case_rating_sheet)
            
            # Map APAC Case Ratings to master_rating structure
            case_transformed = pd.DataFrame()
            
            # Column mappings for APAC Case Ratings
            case_transformed['Feedback Created Date'] = case_rating_df.get('GetFeedback Response: Created Date')
            case_transformed['Owner Name'] = case_rating_df.get('GetFeedback Response: Owner Name')
            case_transformed['Outcome'] = case_rating_df.get('Outcome')
            case_transformed['Rating'] = case_rating_df.get('Case Satisfaction')  # Note: Case Satisfaction, not Rating
            case_transformed['Account: Billing Country'] = case_rating_df.get('Case: Account Billing Country')
            
            # Key mappings as specified:
            # chat_case_number from Case Number
            # chat_case_id from Case ID
            case_transformed['chat_case_number'] = case_rating_df.get('Case: Case Number')
            case_transformed['chat_case_id'] = case_rating_df.get('Case: Case ID')
            
            case_transformed['Language'] = case_rating_df.get('Language')
            case_transformed['Reason'] = case_rating_df.get('Case: Case Reason')
            case_transformed['Month'] = case_rating_df.get('Month')
            case_transformed['Week '] = case_rating_df.get('Week ')  # Note the space in 'Week '
            case_transformed['Day'] = case_rating_df.get('Day')
            case_transformed['Team'] = case_rating_df.get('Team')
            case_transformed['PositivePctHelper'] = case_rating_df.get('PositivePctHelper')
            case_transformed['Source'] = 'Case'
            
            dfs.append(case_transformed)
            st.success(f"✅ Case Rating processed: {len(case_transformed)} rows from sheet '{case_rating_sheet}'")
        
        if not dfs:
            st.error("❌ No rating files uploaded!")
            return None
        
        # Combine rating files
        master_rating = pd.concat(dfs, ignore_index=True, sort=False)
        
        # Ensure exact column order matches master_rating
        required_columns_order = [
            'Feedback Created Date',
            'Owner Name', 
            'Outcome',
            'Rating',
            'Account: Billing Country',
            'chat_case_number',
            'Language',
            'Reason',
            'chat_case_id',
            'Month',
            'Week ',  # Note the space
            'Day',
            'Team',
            'PositivePctHelper',
            'Source'
        ]
        
        # Reorder columns to match exact master_rating structure
        master_rating = master_rating[required_columns_order]
        
        # Convert Excel date fields to proper datetime
        if 'Feedback Created Date' in master_rating.columns:
            master_rating['Feedback Created Date'] = master_rating['Feedback Created Date'].apply(
                lambda x: excel_to_datetime(x) if isinstance(x, (int, float)) and not pd.isna(x) else x
            )
        
        st.success(f"🎉 Master Rating created successfully: {len(master_rating)} total rows")
        return master_rating
        
    except Exception as e:
        st.error(f"❌ Error processing rating files: {str(e)}")
        return None

def download_excel(df, filename):
    """Create download button for Excel file"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Data')
    
    output.seek(0)
    return output.getvalue()

# Main App
def main():
    st.title("📊 CS Dashboard Data Processor")
    st.markdown("Transform your raw data files into master files ready for Google Sheets upload")
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs(["💬 Chat Processing", "📋 Case Processing", "⭐ CSAT Processing"])
    
    with tab1:
        st.header("Chat Data Processing")
        st.markdown("""
        **Upload your chat files - the app will automatically detect the data type:**
        - Can handle single file with multiple sheets or separate files
        - Automatically detects Live Chat, LINE, or WeChat data
        - Intelligently maps columns regardless of naming variations
        """)
        
        # Allow multiple file uploads
        uploaded_files = st.file_uploader(
            "Upload Chat Files (Excel format)",
            type=['xlsx', 'xls'],
            accept_multiple_files=True,
            key="chat_files"
        )
        
        # Sheet selection for each uploaded file
        files_and_sheets = []
        if uploaded_files:
            st.subheader("Select Sheets for Processing")
            for i, uploaded_file in enumerate(uploaded_files):
                col1, col2 = st.columns([1, 1])
                with col1:
                    st.write(f"**{uploaded_file.name}**")
                with col2:
                    sheets = get_sheet_names(uploaded_file)
                    if sheets:
                        selected_sheets = st.multiselect(
                            f"Select sheets from {uploaded_file.name}:",
                            sheets,
                            key=f"sheets_{i}"
                        )
                        for sheet in selected_sheets:
                            files_and_sheets.append({
                                'file': uploaded_file,
                                'sheet': sheet,
                                'name': f"{uploaded_file.name} - {sheet}"
                            })
        
        # Preview and validate detection
        if files_and_sheets and st.button("🔍 Preview Data Detection", key="preview_detection"):
            with st.spinner("Analyzing uploaded data..."):
                # Load all selected sheets for preview
                uploaded_data = []
                for item in files_and_sheets:
                    try:
                        df = pd.read_excel(item['file'], sheet_name=item['sheet'])
                        uploaded_data.append({
                            'name': item['name'],
                            'data': df
                        })
                    except Exception as e:
                        st.error(f"❌ Error reading {item['name']}: {str(e)}")
                
                if uploaded_data:
                    # Show detection results for user confirmation
                    detection_results = preview_detection_results(uploaded_data)
                    st.session_state['detection_results'] = detection_results
        
        # Process files after user confirmation
        if st.button("🔄 Process Chat Files", key="process_chat"):
            if 'detection_results' in st.session_state:
                with st.spinner("Processing chat files..."):
                    master_chat = flexible_process_chat_data_confirmed(st.session_state['detection_results'])
                    
                    if master_chat is not None:
                        st.session_state['master_chat'] = master_chat
                        
                        # Show preview
                        st.subheader("📋 Master Chat Preview")
                        st.dataframe(master_chat.head())
                        st.info(f"Total columns: {len(master_chat.columns)} | Total rows: {len(master_chat)}")
                        
                        # Show data type summary
                        if 'Channel' in master_chat.columns:
                            channel_counts = master_chat['Channel'].value_counts()
                            st.subheader("📊 Data Summary by Channel")
                            for channel, count in channel_counts.items():
                                st.write(f"• {channel}: {count} records")
            else:
                st.warning("⚠️ Please preview data detection first to confirm data types")
        
        # Download button for master_chat
        if 'master_chat' in st.session_state:
            excel_data = download_excel(st.session_state['master_chat'], 'master_chat.xlsx')
            st.download_button(
                label="📥 Download Master Chat",
                data=excel_data,
                file_name="master_chat.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_chat"
            )
    
    with tab2:
        st.header("Case Data Processing")
        st.markdown("""
        **Upload your case source file to create master_case:**
        - Upload: Updated APAC Case volume file
        """)
        
        case_file = st.file_uploader(
            "Upload Case file",
            type=['xlsx', 'xls'],
            key="case_file"
        )
        
        case_sheet = None
        if case_file is not None:
            case_sheets = get_sheet_names(case_file)
            if case_sheets:
                case_sheet = st.selectbox(
                    "Select Case sheet:",
                    case_sheets,
                    key="case_sheet"
                )
        
        if st.button("🔄 Process Case File", key="process_case"):
            if case_file is not None:
                if case_sheet:
                    with st.spinner("Processing case file..."):
                        master_case = process_case_file(case_file, case_sheet)
                        
                        if master_case is not None:
                            st.session_state['master_case'] = master_case
                            
                            # Show preview
                            st.subheader("📋 Master Case Preview")
                            st.dataframe(master_case.head())
                            st.info(f"Total columns: {len(master_case.columns)} | Total rows: {len(master_case)}")
                else:
                    st.error("❌ Please select a sheet for the Case file")
            else:
                st.warning("⚠️ Please upload a case file")
        
        # Download button for master_case
        if 'master_case' in st.session_state:
            excel_data = download_excel(st.session_state['master_case'], 'master_case.xlsx')
            st.download_button(
                label="📥 Download Master Case",
                data=excel_data,
                file_name="master_case.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_case"
            )
    
    with tab3:
        st.header("CSAT Rating Processing")
        st.markdown("""
        **Upload your 2 rating files to create master_rating:**
        - APAC Chat Ratings file
        - Case Rating file
        """)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Chat Ratings")
            chat_rating_file = st.file_uploader(
                "Upload Chat Rating file",
                type=['xlsx', 'xls'],
                key="chat_rating"
            )
            chat_rating_sheet = None
            if chat_rating_file is not None:
                chat_rating_sheets = get_sheet_names(chat_rating_file)
                if chat_rating_sheets:
                    chat_rating_sheet = st.selectbox(
                        "Select Chat Rating sheet:",
                        chat_rating_sheets,
                        key="chat_rating_sheet"
                    )
        
        with col2:
            st.subheader("Case Ratings")
            case_rating_file = st.file_uploader(
                "Upload Case Rating file",
                type=['xlsx', 'xls'],
                key="case_rating"
            )
            case_rating_sheet = None
            if case_rating_file is not None:
                case_rating_sheets = get_sheet_names(case_rating_file)
                if case_rating_sheets:
                    case_rating_sheet = st.selectbox(
                        "Select Case Rating sheet:",
                        case_rating_sheets,
                        key="case_rating_sheet"
                    )
        
        if st.button("🔄 Process Rating Files", key="process_rating"):
            if any([chat_rating_file, case_rating_file]):
                # Check if sheets are selected for uploaded files
                proceed = True
                if chat_rating_file and not chat_rating_sheet:
                    st.error("❌ Please select a sheet for Chat Rating file")
                    proceed = False
                if case_rating_file and not case_rating_sheet:
                    st.error("❌ Please select a sheet for Case Rating file")
                    proceed = False
                    
                if proceed:
                    with st.spinner("Processing rating files..."):
                        master_rating = process_rating_files(chat_rating_file, case_rating_file,
                                                           chat_rating_sheet, case_rating_sheet)
                        
                        if master_rating is not None:
                            st.session_state['master_rating'] = master_rating
                            
                            # Show preview
                            st.subheader("📋 Master Rating Preview")
                            st.dataframe(master_rating.head())
                            st.info(f"Total columns: {len(master_rating.columns)} | Total rows: {len(master_rating)}")
            else:
                st.warning("⚠️ Please upload at least one rating file")
        
        # Download button for master_rating
        if 'master_rating' in st.session_state:
            excel_data = download_excel(st.session_state['master_rating'], 'master_rating.xlsx')
            st.download_button(
                label="📥 Download Master Rating",
                data=excel_data,
                file_name="master_rating.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_rating"
            )
    
    # Instructions section at the bottom
    st.markdown("---")
    st.markdown("### 📝 Instructions for Google Sheets Upload")
    st.info("""
    1. Download the processed master files using the buttons above
    2. Open your Google Sheets dashboard
    3. Upload/import the Excel files to update your data source
    4. Refresh your Looker Studio dashboard
    """)

if __name__ == "__main__":
    main()