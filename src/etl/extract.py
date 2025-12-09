import re
from datetime import datetime
from pathlib import Path
import pandas as pd


def extract_date_from_filename(file_path: Path) -> str:
    """
    Extract date from filename pattern: retail_DD_MM_YYYY.csv
        
    Raises:
        ValueError: If filename doesn't match expected pattern
    """
    filename = file_path.stem  # Get filename without extension
    
    # we assume that each file that arrives in raw has this pattern: retail_DD_MM_YYYY
    pattern = r'retail_(\d{2})_(\d{2})_(\d{4})'
    match = re.search(pattern, filename)
    
    if not match:
        raise ValueError(
            f"Filename '{filename}' doesn't match expected pattern 'retail_DD_MM_YYYY.csv'"
        )
    
    day, month, year = match.groups()
    
    # Validate date
    try:
        date_obj = datetime(int(year), int(month), int(day))
        return date_obj.strftime('%Y-%m-%d')
    except ValueError as e:
        raise ValueError(f"Invalid date extracted from filename: {day}/{month}/{year} - {e}")
    



def extract_data_from_csv(file_path:str) -> tuple[pd.DataFrame, str]:
    """ 
     Extract data from CSV file and date from filename
    """

    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"CSV file not found: {file_path}")
    
    # Extract date from filename
    transaction_date = extract_date_from_filename(file_path)
    print(f"Extracted date from filename: {transaction_date}")


    # Read CSV
    try:
        df = pd.read_csv(file_path)
        
        if df.empty:
            raise ValueError("CSV file is empty")
        
        # Validate required columns from settings
        required_columns  = [
                'id', 'category', 'description', 
                'quantity', 'amount_excl_tax', 'amount_inc_tax' 
                ]
        missing_columns = set(required_columns) - set(df.columns)
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        print(f"Extracted {len(df)} rows from CSV")
        print(f"Columns found: {df.columns.tolist()}")
        
        return df, transaction_date
    
    except Exception as e:
        raise ValueError(f"Error reading CSV: {str(e)}")

