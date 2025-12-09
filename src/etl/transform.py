import pandas as pd


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename columns to match database schema

    """
    df = df.rename(columns={
        'description': 'name'
    })
    return df


def add_transaction_date(df: pd.DataFrame, transaction_date: str) -> pd.DataFrame:
    """
    Add transaction_date column to DataFrame
    
    """
    df['transaction_date'] = transaction_date
    return df


def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reorder columns to match database schema
    
    Expected order: id, transaction_date, category, name, 
                   quantity, amount_excl_tax, amount_inc_tax
    """
    column_order = [
        'id', 
        'transaction_date', 
        'category', 
        'name', 
        'quantity', 
        'amount_excl_tax', 
        'amount_inc_tax'
    ]
    
    # Check if all columns exist
    missing = set(column_order) - set(df.columns)
    if missing:
        raise f"Missing columns when reordering: {missing}"
    
    # Reorder (only columns that exist)
    available_columns = [col for col in column_order if col in df.columns]
    df = df[available_columns]
    
    return df




def transform_data(df: pd.DataFrame, 
                   transaction_date: str ) -> pd.DataFrame:
    """
    Transform raw data into validated transactions ready for database insertion
    
    This is the main transformation pipeline that applies all cleaning and
    validation steps in the correct order.
    

        
    Returns:
        pd.DataFrame: Cleaned and validated data ready for DB insertion

    """
    print(f"Starting transformation of {len(df)} rows")
    
    # Make a copy to avoid modifying original
    df = df.copy()
    
    initial_count = len(df)
    
    # Apply all transformations in sequence

    df = rename_columns(df)
    df = add_transaction_date(df, transaction_date)
    df = reorder_columns(df)
    
    final_count = len(df)
    rejected_count = initial_count - final_count
    

    if final_count == 0:
        raise ValueError("All rows were rejected during transformation")
    
    print(f"Transformation complete: {final_count} valid transactions")
    
    return df