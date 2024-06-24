import pyodbc
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
import pandas as pd

# Database connection details
server = 'your_server'
database = 'your_database'
username = 'your_username'
password = 'your_password'

# SQL Server stored procedure details
stored_procedure = 'your_stored_procedure_name'

# Excel output file
output_file = 'output.xlsx'

def execute_stored_procedure(server, database, username, password, stored_procedure):
    """
    Execute the stored procedure and return the result as a DataFrame.
    """
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password}"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Execute the stored procedure
    sql_query = f"EXEC {stored_procedure}"
    cursor.execute(sql_query)

    # Fetch the results into a DataFrame
    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()
    df = pd.DataFrame.from_records(rows, columns=columns)

    cursor.close()
    conn.close()
    
    return df

def write_to_excel(dataframe, output_file):
    """
    Write the DataFrame to an Excel file.
    """
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = 'Result'

    for r in dataframe_to_rows(dataframe, index=False, header=True):
        ws.append(r)

    wb.save(output_file)

def main():
    df = execute_stored_procedure(server, database, username, password, stored_procedure)
    write_to_excel(df, output_file)
    print(f'Results have been written to {output_file}')

if __name__ == "__main__":
    main()