import os
import csv
import snowflake.connector
from config import credentials

conn = snowflake.connector.connect(
        user = credentials["USER"],
        password = credentials["PASSWORD"],
        account = credentials["ACCOUNT"],
        warehouse = credentials["WAREHOUSE"],
        database = credentials["DATABASE"],
        schema = credentials["SCHEMA"]
        )

# Defining the folder where extracted CSV files will be saved
output_dir = 'D:/etl2/data'

# Creating a cursor object to execute SQL queries
cur = conn.cursor()
tables = []

class ExtractFiles:

    def extract_in_csv(self):

        # Storing the names of tables in a list
        for row in cur.execute("SHOW TABLES").fetchall():
            tables.append(row[1])

        for table in tables:
            # Extracting column names of the table
            columns_name = cur.execute(f"DESCRIBE TABLE {table}").fetchall()
            extracted_columns = [item[0] for item in columns_name]

            # SQL query to select all rows from the table
            sql_query = f'''SELECT * FROM {table}'''
                
            # Extracting all rows of a table
            rows = cur.execute(sql_query).fetchall()
            
            file = open(os.path.join(output_dir, f'{table}.csv'), 'w+', newline ='')
            with file:
                # Creating a CSV writer object    
                write = csv.writer(file)
                # Writing the column names as header
                write.writerow(extracted_columns)
                # Writing rows of data
                write.writerows(rows)
        
        print("Data Extraction completed")