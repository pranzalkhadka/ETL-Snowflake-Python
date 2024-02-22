from extract import ExtractFiles
extract = ExtractFiles()

from sales_aggregation import SalesAggregation
sales_aggregate = SalesAggregation()

from stage_load import StageLoad
stage_load = StageLoad()

from temp_load import TempLoad
temp_load = TempLoad()

from target_load import TargetLoad
target_load = TargetLoad()

def run_etl():
    # Extracting the csv files locally
    extract.extract_in_csv()
    # Creating tables and loading the data in the staging area
    stage_load.load_stage_table()
    # Creating tables and loading the data in the temporary storage for additional transformation
    temp_load.load_temp_table()
    # Peforming sales aggregation
    sales_aggregate.sales_aggregation()
    # Finally, loading the data in target area after required transformation are completed
    target_load.load_target_table()

run_etl()