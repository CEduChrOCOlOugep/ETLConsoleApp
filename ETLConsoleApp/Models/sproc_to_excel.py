import pyodbc
import pandas as pd

# Define the connection parameters
server = 'your_server_name'
database = 'your_database_name'
username = 'your_username'
password = 'your_password'
stored_procedure = 'schemaName.AnalyzeTemporalTableErrors'
output_file = 'output.xlsx'

# Establish the database connection
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=' + server + ';'
    'DATABASE=' + database + ';'
    'UID=' + username + ';'
    'PWD=' + password
)

# Create a cursor from the connection
cursor = conn.cursor()

# Execute the stored procedure
cursor.execute(f"EXEC {stored_procedure}")

# Fetch the results
columns = [column[0] for column in cursor.description]
results = cursor.fetchall()

# Close the cursor and connection
cursor.close()
conn.close()

# Convert the results to a DataFrame
df = pd.DataFrame.from_records(results, columns=columns)

# Save the DataFrame to an Excel file
df.to_excel(output_file, index=False)

print(f"Data saved to {output_file}")
