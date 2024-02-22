import os
import pandas as pd
import snowflake.connector
from config import credentials

# Defining the current schema
schema = "STG"

conn = snowflake.connector.connect(
        user = credentials["USER"],
        password = credentials["PASSWORD"],
        account = credentials["ACCOUNT"],
        warehouse = credentials["WAREHOUSE"],
        database = credentials["DATABASE"],
        )
cur = conn.cursor()

class StageLoad:

    def load_data(self, df, table_name):
        # Removing data already present in the table
        cur.execute(f"Truncate table {table_name}")

        for row in df.itertuples():
            # SQL query to insert each row into the table
            sql_query = f'''
                Insert into {table_name} values {row[1:len(row)]}
            '''
            cur.execute(sql_query)


    def load_stage_table(self):
        # Using the staging schema as current schema
        cur.execute(f"USE SCHEMA {schema}")
        
        for csv_file in os.listdir("data"):
            # Reading each CSV file representing each table into a pandas dataframe
            df = pd.read_csv(f"data/{csv_file}")
            # Replacing the missing values with zeroes
            df.fillna(value = 0, inplace=True)
            # Extracting table name from the CSV file name
            table_name = csv_file.split('.')[0]

            # SQL query to create the table with the same structure as the tables in transactions schema
            sql_query = f'''
                create or replace table {schema}_{table_name} as
                select * from TRANSACTIONS.{table_name} where 1=2;
            '''
            cur.execute(sql_query)

            # Loading data into the newly created table in staging area
            self.load_data(df, f'{schema}_{table_name}')
        print(f"{schema} tables has been loaded")