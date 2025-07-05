import pandas as pd
import logging
from utils.gcs import upload_to_gcs, load_from_gcs
from ingestion.enrich_data import TechnicalIndicators, EnrichMacros
from utils.gcs import upload_to_gcs, load_from_gcs, blob_exists, load_all_gcs_data
logging.basicConfig(
    filename="ingestion/debug.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def transform__ticker_data(bucket_name: str, source_blob_name: str):
    """
    Transforms the data by enriching it with market cap, sector, and industry information.
    Args:
        bucket_name (str): The name of the GCS bucket.
        source_blob_name (str): The source path in the GCS bucket.
    """
    # Load the ticker data from GCS
    ticker_df = load_from_gcs(bucket_name, source_blob_name)
    if ticker_df.empty:
        print(f"No data found in {source_blob_name}. Please check the bucket and blob name.")
        return
    
    for df in ticker_df:
        if df.empty:
            print(f"No data found for {df}. Skipping enrichment.")
            continue
        
        # Enrich the data with market cap, sector, and industry information
        enrich_data = TechnicalIndicators(df)
        enrich_data.enrich_tickers()
        enrich_data.calculate_technical_indicators()

        if enrich_data.transformed_df.empty:
            print(f"No enriched data found for {df}. Skipping upload.")
            continue
        
        # Save the enriched data back to GCS
        destination_blob_name = f"enriched/tickers/enrich_data.transformed_{df['Ticker'].iloc[0]}.parquet"
        upload_to_gcs(enrich_data.transformed_df, bucket_name, destination_blob_name)
        logger.info(f"Enriched data for {df['Ticker'].iloc[0]} uploaded to {destination_blob_name}")

def transform_macro_data(bucket_name: str, source_blob_name: str):
    """
    Transforms the macroeconomic data by enriching it with additional indicators.
    Args:
        bucket_name (str): The name of the GCS bucket.
        source_blob_name (str): The source path in the GCS bucket.
    """
    # Load the macroeconomic data from GCS
    macro_df = load_from_gcs(bucket_name, source_blob_name)[0]
    if macro_df.empty:
        print(f"No data found in {source_blob_name}. Please check the bucket and blob name.")
        return

    # Enrich the macroeconomic data
    enrich_data = EnrichMacros(macro_df)
    enrich_data.transform_macro_data()

    if enrich_data.transformed_df.empty:
        print(f"No enriched data found for {macro_df}. Skipping upload.")
        return

    # Save the enriched data back to GCS
    destination_blob_name = f"enriched/macros/enrich_data.transformed.parquet"
    upload_to_gcs(enrich_data.transformed_df, bucket_name, destination_blob_name)
    logger.info(f"Enriched macro data uploaded to {destination_blob_name}")
    
def main():
    """
    Main function to run the data transformation process.
    """
    bucket_name = "candlethrob-candata"
    
    # Transform ticker data
    ticker_source_blob_name = "raw/tickers/"
    transform__ticker_data(bucket_name, ticker_source_blob_name)
    
    # Transform macroeconomic data
    macro_source_blob_name = "raw/macros"
    transform_macro_data(bucket_name, macro_source_blob_name)