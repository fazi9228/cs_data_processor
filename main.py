from flask import Flask, render_template, request, jsonify, send_file, session
import pandas as pd
import os
import uuid
from werkzeug.utils import secure_filename

# Import our modular processors
from utils import get_sheet_names, save_excel_with_proper_formatting
from chat_processor import detect_chat_data_type, process_chat_files
from case_processor import detect_case_data_type, process_case_files
from rating_processor import process_rating_files

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'  # Change this in production
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Create uploads directory if it doesn't exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

def detect_data_type(df, filename=""):
    """Master detection function that routes to appropriate detector"""
    # Try chat detection first
    chat_type, chat_results = detect_chat_data_type(df, filename)
    if chat_type != 'unknown':
        return chat_type, chat_results
    
    # Try case detection
    case_type, case_results = detect_case_data_type(df, filename)
    if case_type != 'unknown':
        return case_type, case_results
    
    # If no detection worked, return unknown with all results
    all_results = {**chat_results, **case_results}
    return 'unknown', all_results

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
            if file_info['detected_type'] in ['live_chat', 'line_chat', 'wechat_chat', 'messaging']:  # Added messaging
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
    # New messaging rating parameters
    messaging_file_path = data.get('messaging_file_path')
    messaging_sheet = data.get('messaging_sheet')
    # Future WeChat and LINE rating parameters
    wechat_file_path = data.get('wechat_file_path')
    wechat_sheet = data.get('wechat_sheet')
    line_file_path = data.get('line_file_path')
    line_sheet = data.get('line_sheet')
    
    try:
        master_rating = process_rating_files(
            chat_file_path, chat_sheet, 
            case_file_path, case_sheet,
            messaging_file_path, messaging_sheet,
            wechat_file_path, wechat_sheet,
            line_file_path, line_sheet
        )
        
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