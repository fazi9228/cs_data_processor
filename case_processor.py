import pandas as pd
from utils import excel_to_datetime

def detect_case_data_type(df, filename=""):
    """Detect case data type with confidence scoring"""
    columns = [col.lower().strip() for col in df.columns]
    
    detection_rules = {
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