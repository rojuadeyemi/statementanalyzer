from flask import Flask, request, render_template, redirect
import os
from analyzer.analyzer import Analyzer

# define flask app
app = Flask(__name__)

# Configure upload folder
app.config['UPLOAD_FOLDER'] = os.path.join(app.root_path, 'static', 'uploads')

# Create upload folder, if it does not exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Home endpoint
@app.route('/')
def main():
    return render_template('index.html')

# extract endpoint
@app.route('/extract', methods=['POST'])
def extract():

    if 'file' not in request.files:
        return redirect(request.url)
    
    file = request.files['file']
    
    if file.filename == '':
        return redirect(request.url)
    
    if file:

        # Save the uploaded image
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
        file.save(file_path)
        
        # Extract statement
        statement = Analyzer(file_path)
        
        # Save to Excel
        statement.save_excel_report()
        timestamp = statement.timestamp
        
        text = f"Your statement {file.filename} has been extracted and stored as {file.filename.split('.')[0]}_{timestamp}.xlsx in 'reports' folder"

        # Use relative path to 'uploads' for URL mapping
        return render_template('after.html', text=text)
    
if __name__ == '__main__':
    app.run(debug=True)