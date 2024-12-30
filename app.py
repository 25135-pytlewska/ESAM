from flask import Flask, request, send_file,render_template, render_template_string
from werkzeug.utils import secure_filename
from esam_processor import ESAMProcessor
import os
from pathlib import Path
import tempfile
import shutil

app = Flask(__name__)

UPLOAD_TEMPLATE = 'upload_form.html'
LIST_TEMPLATE = 'list.html'

def setup_temp_directories():
    """Create temporary input and excel directories."""
    temp_dir = Path(tempfile.mkdtemp())
    input_dir = temp_dir / "input"
    excel_dir = temp_dir / "excel"
    input_dir.mkdir()
    excel_dir.mkdir()
    return temp_dir, input_dir, excel_dir

@app.route('/list', methods=['GET'])
def list_files():
    input_dir = './input'

    try:
        files = Path(input_dir).iterdir()
        return [file.name for file in files if file.is_file()]

    except Exception as e:
        print(f"An error occurred while listing files: {e}")
        return []

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        if 'file' not in request.files:
            return render_template(UPLOAD_TEMPLATE, error="No file part")

        file = request.files['file']
        if file.filename == '':
            return render_template(UPLOAD_TEMPLATE, error="No selected file")

        if not file.filename.endswith('.csv'):
            return render_template(UPLOAD_TEMPLATE, error="Please upload a CSV file")

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
            # results.mpal_per_day
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
            return render_template(UPLOAD_TEMPLATE, error=f"Error processing file: {str(e)}")

    return render_template(UPLOAD_TEMPLATE, error=None)

# Decorator to execute function after request
from functools import wraps
from flask import after_this_request

if __name__ == '__main__':
    app.run(debug=True) 