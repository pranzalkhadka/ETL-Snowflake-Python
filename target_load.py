import os
import pandas as pd
import snowflake.connector
from config import credentials

conn = snowflake.connector.connect(
        user = credentials["USER"],
        password = credentials["PASSWORD"],
        account = credentials["ACCOUNT"],
        warehouse = credentials["WAREHOUSE"],
        database = credentials["DATABASE"],
        schema = credentials["TGT_SCHEMA"]
        )

# Creating a cursor object to execute SQL queries
cur = conn.cursor()

class TargetLoad:


    def load_data(self, df, table_name):
        # Removing data present in the table
        cur.execute(f"Truncate table DWH_Pranjal_{table_name}")
        # Handling different table insertion based on table_name
        match table_name:
            case "CATEGORY":
                for row in df.itertuples():
                    query = "Insert into DWH_PRANJAL_CATEGORY" \
                    "(ID, CATEGORY_DESC)"\
                    "VALUES ('%s','%s')" % (row.ID, row.CATEGORY_DESC)
                    cur.execute(query)

            case "COUNTRY":
                for row in df.itertuples():
                    query = "Insert into DWH_PRANJAL_COUNTRY" \
                    "(ID, COUNTRY_DESC)"\
                    "VALUES ('%s','%s')" % (row.ID, row.COUNTRY_DESC)
                    cur.execute(query)

            case "CUSTOMER":
                for row in df.itertuples():
                    query = "Insert into DWH_PRANJAL_CUSTOMER" \
                    "(ID, CUSTOMER_FIRST_NAME,CUSTOMER_MIDDLE_NAME,CUSTOMER_LAST_NAME,CUSTOMER_ADDRESS)"\
                    "VALUES ('%s','%s','%s','%s','%s')" % (row.ID, row.CUSTOMER_FIRST_NAME, row.CUSTOMER_MIDDLE_NAME, row.CUSTOMER_LAST_NAME, row.CUSTOMER_ADDRESS)
                    cur.execute(query)

            case "PRODUCT":
                for row in df.itertuples():
                    query = "Insert into DWH_PRANJAL_PRODUCT" \
                    "(ID, SUBCATEGORY_ID,PRODUCT_DESC)"\
                    "VALUES ('%s','%s','%s')" % (row.ID, row.SUBCATEGORY_ID, row.PRODUCT_DESC)
                    cur.execute(query)

            case "REGION":
                for row in df.itertuples():
                    query = "Insert into DWH_PRANJAL_REGION" \
                    "(ID, COUNTRY_ID,REGION_DESC)"\
                    "VALUES ('%s','%s','%s')" % (row.ID, row.COUNTRY_ID, row.REGION_DESC)
                    cur.execute(query)
                        
            case "SALES":
                for row in df.itertuples():
                    query = "Insert into DWH_PRANJAL_SALES" \
                    "(ID, STORE_ID,PRODUCT_ID,CUSTOMER_ID,TRANSACTION_TIME,QUANTITY, AMOUNT, DISCOUNT)"\
                    "VALUES ('%s','%s','%s','%s','%s','%s','%s','%s')" % (row.ID, row.STORE_ID, row.PRODUCT_ID, row.CUSTOMER_ID, row.TRANSACTION_TIME,row.QUANTITY,row.AMOUNT,row.DISCOUNT)
                    cur.execute(query)

            case "STORE":
                for row in df.itertuples():
                    query = "Insert into DWH_PRANJAL_STORE" \
                    "(ID, REGION_ID,STORE_DESC)"\
                    "VALUES ('%s','%s','%s')" % (row.ID, row.REGION_ID, row.STORE_DESC)
                    cur.execute(query)
            
            case "SUBCATEGORY":
                for row in df.itertuples():
                    query = "Insert into DWH_PRANJAL_SUBCATEGORY" \
                    "(ID, CATEGORY_ID,SUBCATEGORY_DESC)"\
                    "VALUES ('%s','%s','%s')" % (row.ID, row.CATEGORY_ID, row.SUBCATEGORY_DESC)
                    cur.execute(query)

    def load_target_table(self):
        for csv_file in os.listdir("data"):
            # Reading CSV file into a pandas dataframe
            df = pd.read_csv(f"data/{csv_file}")
            # Replacing NaN values by filling with zeroes
            df.fillna(value = 0, inplace=True)
            # Extracting table name from file name
            table_name_suffix = csv_file.split('.')[0]

            # Creating target table in Snowflake
            sql_query = f'''
                create or replace table DWH_Pranjal_{table_name_suffix} as
                select * from TRANSACTIONS.{table_name_suffix} where 1=2;
            '''
            cur.execute(sql_query)
            # Adding a surrogate key to the target table
            cur.execute(f"ALTER TABLE DWH_Pranjal_{table_name_suffix} ADD SURROGATE_KEY integer UNIQUE NOT NULL PRIMARY KEY AUTOINCREMENT(1,1)")
            # Loading data into target table
            self.load_data(df, table_name_suffix)
        print("Target tables has been loaded")
        cur.close()
        conn.close()