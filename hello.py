import pandas as pd
import mysql.connector
from mysql.connector import Error
import os

# File paths
csv_file = r'C:\Users\prasa\Documents\Dataload\insurance.csv'
row_file = r'C:\Users\prasa\Documents\Dataload\insurance_row.txt'
col_file = r'C:\Users\prasa\Documents\Dataload\insurance_column.txt'

# Extract table name from CSV filename (without extension)
table_name = os.path.splitext(os.path.basename(csv_file))[0]

def validate_csv(csv_path, row_path, col_path):
    csv_data = pd.read_csv(csv_path)
    with open(row_path, 'r') as f:
        expected_rows = int(f.read().strip())
    with open(col_path, 'r') as f:
        expected_cols = int(f.read().strip())

    if csv_data.shape[0] != expected_rows:
        print(f"Row validation failed: Expected {expected_rows}, Found {csv_data.shape[0]}")
        return None
    if csv_data.shape[1] != expected_cols:
        print(f"Column validation failed: Expected {expected_cols}, Found {csv_data.shape[1]}")
        return None

    print("CSV file validation passed.")
    return csv_data

def map_dtype_to_mysql(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    else:
        return 'VARCHAR(255)'

def create_table_if_not_exists(connection, dataframe, table_name):
    cursor = connection.cursor()
    cols_with_types = []
    for col in dataframe.columns:
        mysql_type = map_dtype_to_mysql(dataframe[col].dtype)
        safe_col_name = col.replace(' ', '_')
        cols_with_types.append(f"`{safe_col_name}` {mysql_type}")
    create_table_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(cols_with_types)})"
    cursor.execute(create_table_query)
    connection.commit()
    cursor.close()
    print(f"Table '{table_name}' is ready.")

def load_data_to_mysql(dataframe):
    try:
        connection = mysql.connector.connect(
            host='127.0.0.1',
            user='root',
            password='root',
            database='world'
        )
        if connection.is_connected():
            create_table_if_not_exists(connection, dataframe, table_name)
            cursor = connection.cursor()
            cols = ','.join([f"`{c.replace(' ', '_')}`" for c in dataframe.columns])
            placeholders = ','.join(['%s'] * len(dataframe.columns))
            insert_query = f"INSERT INTO `{table_name}` ({cols}) VALUES ({placeholders})"
            records = [tuple(x) for x in dataframe.to_numpy()]
            cursor.executemany(insert_query, records)
            connection.commit()
            print(f"{cursor.rowcount} records inserted successfully.")
            cursor.close()
    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            connection.close()
            print("MySQL connection is closed.")

if __name__ == "__main__":
    df = validate_csv(csv_file, row_file, col_file)
    if df is not None:
        load_data_to_mysql(df)
