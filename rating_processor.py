import pandas as pd
from utils import standardize_date_columns

def process_chat_ratings(chat_rating_df):
    """Process APAC Chat Ratings to master_rating structure"""
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
    
    return chat_transformed

def process_case_ratings(case_rating_df):
    """Process APAC Case Ratings to master_rating structure"""
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
    
    return case_transformed

def process_messaging_ratings(messaging_rating_df):
    """Process Messaging Rating to master_rating structure"""
    messaging_transformed = pd.DataFrame()
    
    # Map Messaging Rating to master_rating structure
    messaging_transformed['Feedback Created Date'] = messaging_rating_df.get('Messaging Session: Start Time')
    messaging_transformed['Owner Name'] = messaging_rating_df.get('Chat Agent Name')
    messaging_transformed['Outcome'] = messaging_rating_df.get('Outcome')
    messaging_transformed['Rating'] = messaging_rating_df.get('Post-Chat Rating')
    messaging_transformed['Account: Billing Country'] = None  # Not available in messaging rating
    messaging_transformed['chat_case_number'] = messaging_rating_df.get('Messaging Session: Messaging Session Name')
    messaging_transformed['chat_case_id'] = messaging_rating_df.get('Messaging Session: Messaging Session Name')  # Same as number
    messaging_transformed['Language'] = messaging_rating_df.get('Language')
    messaging_transformed['Reason'] = messaging_rating_df.get('Messaging Session: Chat Reason')
    messaging_transformed['Month'] = messaging_rating_df.get('Month')
    messaging_transformed['Week '] = messaging_rating_df.get('Week ')  # Map Week to Week 
    messaging_transformed['Day'] = messaging_rating_df.get('Day')
    messaging_transformed['Team'] = messaging_rating_df.get('Team')
    messaging_transformed['PositivePctHelper'] = messaging_rating_df.get('PositivePctHelper')
    messaging_transformed['Source'] = 'Messaging'
    
    return messaging_transformed

def process_wechat_ratings(wechat_rating_df):
    """Process WeChat Rating to master_rating structure"""
    wechat_transformed = pd.DataFrame()
    
    # Map WeChat Rating to master_rating structure
    wechat_transformed['Feedback Created Date'] = wechat_rating_df.get('Created Date')
    wechat_transformed['Owner Name'] = wechat_rating_df.get('WeChat Agent: Agent Nickname')
    wechat_transformed['Outcome'] = wechat_rating_df.get('Outcome')
    
    # Convert Outcome to Rating (Positive=5, Negative=1)
    outcome_to_rating = wechat_rating_df.get('Outcome').map({'Positive': 5, 'Negative': 1})
    wechat_transformed['Rating'] = outcome_to_rating
    
    wechat_transformed['Account: Billing Country'] = None  # Not available
    wechat_transformed['chat_case_number'] = wechat_rating_df.get('WeChat Transcript: Number')
    wechat_transformed['chat_case_id'] = wechat_rating_df.get('Survey Taken Number')
    wechat_transformed['Language'] = None  # Not available
    wechat_transformed['Reason'] = None  # Not available
    wechat_transformed['Month'] = wechat_rating_df.get('Month')
    wechat_transformed['Week '] = wechat_rating_df.get('Week ')  # ✅ UPDATED: Now source has space!
    wechat_transformed['Day'] = wechat_rating_df.get('Day')
    wechat_transformed['Team'] = None  # Not available
    
    # ✅ UPDATED: Direct mapping - no conversion needed!
    wechat_transformed['PositivePctHelper'] = wechat_rating_df.get('PositivePctHelper')
    
    wechat_transformed['Source'] = 'WeChat'
    
    return wechat_transformed

def process_line_ratings(line_rating_df):
    """Process LINE Rating to master_rating structure - PLACEHOLDER FOR FUTURE"""
    # TODO: Implement LINE rating processing when file structure is available
    line_transformed = pd.DataFrame()
    
    # This will be implemented when LINE rating file structure is provided
    line_transformed['Source'] = 'LINE'
    
    return line_transformed

def process_rating_files(chat_file_path, chat_sheet, case_file_path, case_sheet, 
                        messaging_file_path=None, messaging_sheet=None,
                        wechat_file_path=None, wechat_sheet=None,
                        line_file_path=None, line_sheet=None):
    """Process rating files and return master_rating dataframe"""
    try:
        dfs = []
        
        # Process existing chat ratings
        if chat_file_path and chat_sheet:
            print(f"Processing chat rating file: {chat_file_path}, sheet: {chat_sheet}")
            chat_rating_df = pd.read_excel(chat_file_path, sheet_name=chat_sheet)
            chat_transformed = process_chat_ratings(chat_rating_df)
            
            # Count non-null dates before processing
            date_count = chat_transformed['Feedback Created Date'].notna().sum()
            total_count = len(chat_transformed)
            print(f"Chat ratings: {date_count} valid dates out of {total_count} rows")
            
            dfs.append(chat_transformed)
        
        # Process existing case ratings
        if case_file_path and case_sheet:
            print(f"Processing case rating file: {case_file_path}, sheet: {case_sheet}")
            case_rating_df = pd.read_excel(case_file_path, sheet_name=case_sheet)
            case_transformed = process_case_ratings(case_rating_df)
            
            # Count non-null dates before processing
            date_count = case_transformed['Feedback Created Date'].notna().sum()
            total_count = len(case_transformed)
            print(f"Case ratings: {date_count} valid dates out of {total_count} rows")
            
            dfs.append(case_transformed)
        
        # Process new messaging ratings
        if messaging_file_path and messaging_sheet:
            print(f"Processing messaging rating file: {messaging_file_path}, sheet: {messaging_sheet}")
            messaging_rating_df = pd.read_excel(messaging_file_path, sheet_name=messaging_sheet)
            messaging_transformed = process_messaging_ratings(messaging_rating_df)
            
            # Count non-null dates before processing
            date_count = messaging_transformed['Feedback Created Date'].notna().sum()
            total_count = len(messaging_transformed)
            print(f"Messaging ratings: {date_count} valid dates out of {total_count} rows")
            
            dfs.append(messaging_transformed)
        
        # Process WeChat ratings
        if wechat_file_path and wechat_sheet:
            print(f"Processing WeChat rating file: {wechat_file_path}, sheet: {wechat_sheet}")
            wechat_rating_df = pd.read_excel(wechat_file_path, sheet_name=wechat_sheet)
            wechat_transformed = process_wechat_ratings(wechat_rating_df)
            
            # Count non-null dates before processing
            date_count = wechat_transformed['Feedback Created Date'].notna().sum()
            total_count = len(wechat_transformed)
            print(f"WeChat ratings: {date_count} valid dates out of {total_count} rows")
            
            dfs.append(wechat_transformed)
        
        # Process LINE ratings (future implementation)
        if line_file_path and line_sheet:
            print(f"Processing LINE rating file: {line_file_path}, sheet: {line_sheet}")
            line_rating_df = pd.read_excel(line_file_path, sheet_name=line_sheet)
            line_transformed = process_line_ratings(line_rating_df)
            
            # Count non-null dates before processing
            date_count = line_transformed['Feedback Created Date'].notna().sum() if 'Feedback Created Date' in line_transformed.columns else 0
            total_count = len(line_transformed)
            print(f"LINE ratings: {date_count} valid dates out of {total_count} rows")
            
            if not line_transformed.empty:
                dfs.append(line_transformed)
        
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