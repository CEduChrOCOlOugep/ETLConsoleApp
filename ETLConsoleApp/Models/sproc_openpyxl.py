import pyodbc
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
import pandas as pd

# Database connection details
server = 'your_server'
database = 'your_database'
stored_procedure = 'your_stored_procedure_name'

# Excel output file
output_file = 'output.xlsx'

def execute_stored_procedure(server, database, stored_procedure):
    """
    Execute the stored procedure and return the result as a DataFrame.
    """
    conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
    with pyodbc.connect(conn_str) as conn:
        df = pd.read_sql(f"EXEC {stored_procedure}", conn)
    
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
    df = execute_stored_procedure(server, database, stored_procedure)
    write_to_excel(df, output_file)
    print(f'Results have been written to {output_file}')

if __name__ == "__main__":
    main()