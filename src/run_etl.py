"""
ETL Pipeline Script - Load transaction data from CSV to database
"""
import sys
import argparse
from pathlib import Path

# Setup Python path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.etl.extract import extract_data_from_csv
from src.etl.transform import transform_data
from src.etl.load import load_to_database, check_duplicates


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Run ETL pipeline")
    parser.add_argument('--file', default='data/raw/retail_15_01_2022.csv', help='CSV file path')
    parser.add_argument('--db', default='data/database/retail.db', help='Database path')
    return parser.parse_args()


def run_etl(csv_path: str, db_path: str) -> int:
    """Run ETL pipeline: Extract -> Transform -> Load"""
    try:
        # Extract
        #print(f"Extracting from {csv_path}...")
        df, date = extract_data_from_csv(csv_path)
        
        
        # Transform
  
        df = transform_data(df, date)
      
        
        # Load
        
        duplicates, df = check_duplicates(df, db_path)
        
        if duplicates > 0:
            print(f"Skipped {duplicates} duplicates")
        
        if len(df) == 0:
            print("No new data to load")
            return 0
        
        loaded = load_to_database(df, db_path)
        #print(f" Loaded {loaded} transactions")
        
        print("Pipeline completed!")
        return 0
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def main():
    """Main entry point"""
    args = parse_args()
    return run_etl(args.file, args.db)


if __name__ == "__main__":
    main()