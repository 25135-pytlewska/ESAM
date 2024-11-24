from flask import Flask, request, send_file, render_template_string
from werkzeug.utils import secure_filename
from esam_processor import ESAMProcessor
import os
from pathlib import Path
import tempfile
import shutil

app = Flask(__name__)

# HTML template for the upload form
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>CSV File Processor</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
        }
        .upload-container {
            border: 2px dashed #ccc;
            padding: 20px;
            text-align: center;
            margin: 20px 0;
        }
        .upload-container:hover {
            border-color: #666;
        }
        .button {
            background-color: #4CAF50;
            border: none;
            color: white;
            padding: 15px 32px;
            text-align: center;
            text-decoration: none;
            display: inline-block;
            font-size: 16px;
            margin: 4px 2px;
            cursor: pointer;
            border-radius: 4px;
        }
        .error {
            color: red;
            margin: 10px 0;
        }
    </style>
</head>
<body>
    <h1>CSV File Processor</h1>
    <div class="upload-container">
        <form method="post" enctype="multipart/form-data">
            <input type="file" name="file" accept=".csv" required>
            <br><br>
            <input type="submit" value="Upload and Process" class="button">
        </form>
    </div>
    {% if error %}
    <div class="error">
        {{ error }}
    </div>
    {% endif %}
</body>
</html>
"""

def setup_temp_directories():
    """Create temporary input and excel directories."""
    temp_dir = Path(tempfile.mkdtemp())
    input_dir = temp_dir / "input"
    excel_dir = temp_dir / "excel"
    input_dir.mkdir()
    excel_dir.mkdir()
    return temp_dir, input_dir, excel_dir

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        if 'file' not in request.files:
            return render_template_string(HTML_TEMPLATE, error="No file part")
        
        file = request.files['file']
        if file.filename == '':
            return render_template_string(HTML_TEMPLATE, error="No selected file")
        
        if not file.filename.endswith('.csv'):
            return render_template_string(HTML_TEMPLATE, error="Please upload a CSV file")

        try:
            # Create temporary directories
            temp_dir, input_dir, excel_dir = setup_temp_directories()
            
            # Save uploaded file
            filename = secure_filename(file.filename)
            base_filename = filename.rsplit('.', 1)[0]  # Remove .csv extension
            input_file_path = input_dir / filename
            file.save(str(input_file_path))
            
            # Process the file
            processor = ESAMProcessor(input_dir=str(input_dir))
            results = processor.process_data(base_filename)
            processor.save_to_excel(base_filename, results, output_dir=str(excel_dir))
            
            # Prepare the processed file for download
            excel_file_path = excel_dir / f"{base_filename}.xlsx"
            
            # Clean up temporary directory after sending file
            @after_this_request
            def cleanup(response):
                shutil.rmtree(temp_dir)
                return response
            
            return send_file(
                excel_file_path,
                as_attachment=True,
                download_name=f"{base_filename}_processed.xlsx",
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
            
        except Exception as e:
            return render_template_string(HTML_TEMPLATE, error=f"Error processing file: {str(e)}")
    
    return render_template_string(HTML_TEMPLATE, error=None)

# Decorator to execute function after request
from functools import wraps
from flask import after_this_request

if __name__ == '__main__':
    app.run(debug=True) 