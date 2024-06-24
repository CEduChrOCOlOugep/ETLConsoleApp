import pyodbc
import pandas as pd
from pathlib import Path
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Font

# Define the connection parameters
server = 'your_server_name'
database = 'your_database_name'
username = 'your_username'
password = 'your_password'
stored_procedure = 'schemaName.AnalyzeTemporalTableErrors'
output_file = Path('Project/ProjectNameReports/database_reports/output.xlsx')

# Function to execute a stored procedure and return the results as a DataFrame
def execute_stored_procedure():
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=' + server + ';'
        'DATABASE=' + database + ';'
        'UID=' + username + ';'
        'PWD=' + password
    )
    cursor = conn.cursor()
    cursor.execute(f"EXEC {stored_procedure}")
    columns = [column[0] for column in cursor.description]
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return pd.DataFrame.from_records(results, columns=columns)

# Fetch the data from the stored procedure
df_sp = execute_stored_procedure()

# Initialize a new workbook
workbook = Workbook()
sheet = workbook.active
sheet.title = "StoredProcedureResults"

# Write the DataFrame to the sheet
for r in dataframe_to_rows(df_sp, index=False, header=True):
    sheet.append(r)

# Apply bold font to the first row (header)
for cell in sheet[1]:
    cell.font = Font(bold=True)

# Save the workbook to the output file
workbook.save(output_file)
print(f"Report has been saved to '{output_file}'")
