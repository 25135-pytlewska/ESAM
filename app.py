from flask import Flask, render_template, request, send_file
import pandas as pd
import io
from processing import process_file

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        if 'file' not in request.files:
            return 'No file uploaded', 400
        
        file = request.files['file']
        if file.filename == '':
            return 'No file selected', 400

        # Read the file
        if file:
            # Process the file using the function from processing.py
            try:
                result_df = process_file(file)
                
                # Convert the results to CSV and prepare for download
                output = io.BytesIO()
                result_df.to_csv(output, index=False)
                output.seek(0)
                
                return send_file(
                    output,
                    mimetype='text/csv',
                    as_attachment=True,
                    download_name='processed_results.csv'
                )
            except Exception as e:
                return f'Error processing file: {str(e)}', 400

    return render_template('upload.html')

if __name__ == '__main__':
    app.run(debug=True)