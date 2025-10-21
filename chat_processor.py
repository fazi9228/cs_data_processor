import pandas as pd
from utils import excel_to_datetime, create_date_columns

def detect_chat_data_type(df, filename=""):
    """Detect chat data type with confidence scoring"""
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
        'messaging': {
            'required': ['messaging session name', 'session owner: full name'],
            'strong_indicators': ['messaging user: contact: full name', 'messaging channel: channel name', 'accept time'],
            'channel_indicators': ['messaging', 'session'],
            'min_confidence': 0.7
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

def smart_chat_column_mapping(df, data_type):
    """Intelligently map chat columns with multiple fallback patterns"""
    columns = df.columns.tolist()
    columns_lower = {col: col.lower().strip() for col in columns}
    column_mapping = {}
    
    print(f"Processing {data_type} with columns: {columns}")
    
    mapping_patterns = {
        'Agent': [
            'owner: full name',
            'wechat agent: agent nickname',
            'session owner: full name',
            'agent',
            'owner name',
            'agent name'
        ],
        'Chat Key': [
            'chat key',
            'number',
            'chat transcript id',
            'messaging session name',
            'id'
        ],
        'Contact Name': [
            'contact name: full name',
            'follower name',
            'messaging user: contact: full name',
            'contact name'
        ],
        'Start Time': [
            'actual start time',
            'start time',
            'request time',
            'created date',
            'chat start time'
        ],
        'End Time': [
            'actual end time',
            'end time'
        ],
        'Accept Time': [
            'accept date',
            'accept time'
        ],
        'Owner Dept': [
            'owner dept',
            'team',
            'agent dept',
            'department'
        ],
        'Actual Chat Duration (min)': [
            'actual chat duration (min)',
            'actual duration (min)',
            'duration (minutes)',
            'aht'
        ],
        'Request Date': [
            'request date',
            'request time'
        ],
        'Close Date': [
            'close date',
            'closed date',
            'end date'
        ]
    }
    
    # Single pass matching - more efficient
    for target_col, patterns in mapping_patterns.items():
        for pattern in patterns:
            # Find matching column (case-insensitive)
            matched_col = next(
                (col for col, col_lower in columns_lower.items() if pattern in col_lower),
                None
            )
            if matched_col:
                column_mapping[target_col] = matched_col
                print(f"Mapped {target_col} -> {matched_col}")
                break
    
    # Set channel based on data type
    channel_map = {
        'live_chat': 'SF',
        'line_chat': 'LINE',
        'wechat_chat': 'WeChat',
        'messaging': 'Messaging'
    }
    if data_type in channel_map:
        column_mapping['Channel'] = channel_map[data_type]
    
    print(f"Final mapping: {column_mapping}")
    return column_mapping

def process_chat_files(file_data_list):
    """Process chat files and return master_chat dataframe"""
    all_chat_data = []
    total_source_rows = 0  # ✅ Track total source rows
    
    for file_info in file_data_list:
        df = file_info['data']
        detected_type = file_info['detected_type']
        
        if detected_type not in ['live_chat', 'line_chat', 'wechat_chat', 'messaging']:  # Added messaging
            continue
        
        # ✅ TRACK SOURCE ROWS
        source_rows = len(df)
        total_source_rows += source_rows
        print(f"Processing {detected_type} file with {source_rows} rows")
        print(f"📊 Source rows for this file: {source_rows}")
        
        # Apply column mappings
        mapping = smart_chat_column_mapping(df, detected_type)
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
                    
            elif isinstance(old_col, str) and old_col in ['SF', 'LINE', 'WeChat', 'Messaging']:  # Added Messaging
                transformed[new_col] = old_col
        
        # Special handling for messaging data
        if detected_type == 'messaging':
            # Preserve Actual Chat Duration (min) for messaging
            if 'Actual Chat Duration (min)' in transformed.columns:
                print("✅ Actual Chat Duration (min) preserved for Messaging")
            
            # Preserve Request Date for messaging
            if 'Request Date' in transformed.columns:
                print("✅ Request Date preserved for Messaging")
            
            # Preserve Close Date for messaging
            if 'Close Date' in transformed.columns:
                print("✅ Close Date preserved for Messaging")
            
            # Convert Duration (Minutes) to Chat Duration (sec)
            if 'Duration (Minutes)' in transformed.columns:
                transformed['Chat Duration (sec)'] = transformed['Duration (Minutes)'] * 60
                print("Converted Duration (Minutes) to Chat Duration (sec)")
            
            # Leave Wait Time blank for messaging (as requested)
            transformed['Wait Time'] = None
            print("Set Wait Time to blank for messaging data")
        else:
            # For LINE, WeChat, and Live Chat (SF) - set Request Date and Close Date to blank
            transformed['Request Date'] = None
            transformed['Close Date'] = None
            print(f"Set Request Date and Close Date to blank for {detected_type}")
        
        # Ensure required columns exist
        required_columns = [
            'Agent', 'Chat Key', 'Channel', 'Start Time', 'Contact Name',
            'Chat Transcript Name', 'Status', 'Chat Reason', 'End Time', 
            'Chat Duration (sec)', 'Wait Time', 'Post-Chat Rating', 'Accept Time'  # Added Accept Time
        ]
        
        for col in required_columns:
            if col not in transformed.columns:
                transformed[col] = None

        # Clean numerical columns (ensure they stay as numbers)
        numerical_columns = [
            'Wait Time', 'Chat Duration (sec)', 'Agent Average Response Time',
            'Agent Message Count', 'Visitor Message Count', 'Post-Chat Rating',
            'Agent First Response Time (Seconds)', 'Agent Avg Response Time',
            'Duration (Minutes)', 'Actual Chat Duration (min)'
        ]

        for col in numerical_columns:
            if col in transformed.columns:
                transformed[col] = pd.to_numeric(transformed[col], errors='coerce')
                print(f"Cleaned numerical column: {col}")

        # ✅ Handle date columns - ALL sources use DD/MM/YYYY format
        date_columns_to_process = [col for col in transformed.columns 
                                if 'time' in col.lower() or 'date' in col.lower()]
        # Exclude numerical time columns from date processing
        date_columns_to_process = [col for col in date_columns_to_process 
                                if col not in numerical_columns]

        for date_col in date_columns_to_process:
            if date_col in transformed.columns:
                # Apply excel_to_datetime conversion first for Excel serial dates
                transformed[date_col] = transformed[date_col].apply(
                    lambda x: excel_to_datetime(x) if isinstance(x, (int, float)) and not pd.isna(x) else x
                )
                # Use dayfirst=True since ALL sources use DD/MM/YYYY format
                transformed[date_col] = pd.to_datetime(transformed[date_col], dayfirst=True, errors='coerce')
                print(f"Converted date column: {date_col} using DD/MM/YYYY format")

        # Create date component columns from the primary date column
        primary_date_col = None
        if 'Start Time' in transformed.columns and transformed['Start Time'].notna().any():
            primary_date_col = 'Start Time'
        elif 'Actual Start Time' in transformed.columns and transformed['Actual Start Time'].notna().any():
            primary_date_col = 'Actual Start Time'
        elif 'Created Date' in transformed.columns and transformed['Created Date'].notna().any():
            primary_date_col = 'Created Date'

        if primary_date_col:
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
    
    # Use exact Master_chat column order with Accept Time added at the end
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
        'Agent First Response Time (Seconds)', 'Closed', 'Agent Avg Response Time',
        'Accept Time', 'Request Date', 'Close Date'  # Added at the end
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
