from flask import Flask, render_template, request, g
import pyodbc
import os
import pandas as pd
from werkzeug.utils import secure_filename
from datetime import datetime

# Additional information
filename = 'UID_data'
print(filename)
uploaded_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Current date and time
user_id = 'project_admin'  # Replace with the actual user ID

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config[
    'DATABASE'] = 'DRIVER={SQL Server};SERVER=192.168.0.99;PORT=1433;DATABASE=YSRCheyutha;UID=project_admin;PWD=4t7I6Y#'
app.config['ALLOWED_EXTENSIONS'] = {'csv', 'xlsx'}  # Allow CSV and Excel files


# Function to check if the file has an allowed extension
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']


# Function to initialize the database
def init_db():
    with app.app_context():
        db = get_db()
        with app.open_resource('schema.sql', mode='r') as f:
            db.cursor().execute(f.read())
        db.commit()


# Function to get the database connection
def get_db():
    if not hasattr(g, 'YSRCheyutha'):
        g.sql_server_db = pyodbc.connect(app.config['DATABASE'])
    return g.sql_server_db


# Function to close the database connection
@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'YSRCheyutha'):
        g.sql_server_db.close()


def table_exists(cursor, table_name):
    cursor.execute(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'{table_name}'")
    return cursor.fetchone()[0] > 0


# Route for the home page
@app.route('/')
def index():
    return render_template('index.html')

# Helper function to get existing column names in a table
def get_existing_columns(cursor, table_name):
    cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{table_name}'")
    return [column[0] for column in cursor.fetchall()]
@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return "No file part"

    file = request.files['file']
    table_name = request.form.get("table_name")
    print(table_name)

    if file.filename == '':
        return "No selected file"

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))

        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)

        db = get_db()
        cursor = db.cursor()

        if filename.endswith('.csv'):
            data = pd.read_csv(file_path)

        elif filename.endswith('.xlsx'):
            data = pd.read_excel(file_path, engine='openpyxl')

        else:
            return 'Unsupported file type'

        # Ensure column names are unique
        data.columns = [f'{col}_{i}' if data.columns.duplicated().any() else col for i, col in enumerate(data.columns)]

        column_names = list(data.columns)

        # Check if the table exists
        if table_exists(cursor, table_name):
            # Alter the existing table to add new columns if needed
            for col in column_names:
                if col not in get_existing_columns(cursor, table_name):
                    alter_table_query = f'''
                        ALTER TABLE {table_name}
                        ADD {col} NVARCHAR(MAX)
                    '''
                    cursor.execute(alter_table_query)

        else:
            # Create Table dynamically
            create_table_query = f'''
                CREATE TABLE {table_name} (
                    {', '.join([f'{col} NVARCHAR(MAX)' for col in column_names])},
                    filename NVARCHAR(MAX),
                    uploaded_date DATETIME,
                    user_id NVARCHAR(MAX)
                )
            '''
            cursor.execute(create_table_query)

        # Insert data into the table
        for _, row in data.iterrows():
            uploaded_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            insert_query = f'''
                INSERT INTO {table_name} ({', '.join(column_names + ['filename', 'uploaded_date', 'user_id'])})
                VALUES ({', '.join(['?'] * (len(column_names) + 3))})
            '''
            cursor.execute(insert_query, *row.tolist() + [filename, uploaded_date, user_id])

        db.commit()
        return 'File Uploaded and Data Imported Successfully'

    return 'Invalid file type'


if __name__ == '__main__':
    app.run(debug=True)
