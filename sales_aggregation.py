import snowflake.connector
from config import credentials

conn = snowflake.connector.connect(
            user = credentials["USER"],
            password = credentials["PASSWORD"],
            account = credentials["ACCOUNT"],
            warehouse = credentials["WAREHOUSE"],
            database = credentials["DATABASE"],
            schema = credentials["TMP_SCHEMA"]
        )

# Creating a cursor object to execute SQL queries
cur = conn.cursor()

class SalesAggregation:

    def sales_aggregation(self):
        
        # SQL query to create or replace a table for aggregating sales data by year and month
        query = """
        CREATE or REPLACE TABLE TMP_SALES_AGG AS
        (SELECT year, month , sum(amount) as monthly_sale
        from
        (select ID,quantity,amount, year(transaction_time) as year , monthname(transaction_time) as month from TMP_SALES)
        group by year, month
        order by year desc, month asc)
        """

        cur.execute(query)
        print("Sales aggregation completed")
