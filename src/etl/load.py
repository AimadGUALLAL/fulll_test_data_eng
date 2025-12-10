import pandas as pd
import sqlite3




def check_duplicates(df: pd.DataFrame, db_path: str) -> tuple[int , pd.DataFrame]:
    """
    Check for existing IDs in database to prevent duplicates
    """

    try:

        conn = sqlite3.connect(db_path)

        existing_ids = pd.read_sql_query(
            "SELECT DISTINCT id FROM transactions",
            conn
        )

        conn.close()

        if existing_ids.empty:

            print("No existing transactions in database")

            return 0, df
        

        existing_ids = set(existing_ids['id'].tolist())

        new_df = df[~df['id'].isin(existing_ids)]

        duplicates_count = len(df) - len(new_df)


        return duplicates_count, new_df


    except Exception as e:

        raise ValueError(f"Error checking duplicates : {str(e)}")

    


def load_to_database(df: pd.DataFrame, 
                     db_path: str ,
                     if_exists: str = 'append') -> int:
    """
    Load data into transactions table

        
    Returns:
        int: Number of rows successfully loaded
        
    Raises:
        ValueError: If the operation fails
    """
    if df.empty:
        print("DataFrame is empty - nothing to load")
        return 0
    

    
    try:
        conn = sqlite3.connect(db_path)

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM transactions")
        total_count_before = cursor.fetchone()[0]
        print(f"Total transactions in database before inserting is {total_count_before}")



        print(f"Inserting {len(df)} transactions into database...")
        
        # Insert into database
        df.to_sql(
            name='transactions',
            con=conn,
            if_exists=if_exists,
            index=False,
            dtype={
                'id': 'TEXT',
                'transaction_date': 'TEXT',
                'category': 'TEXT',
                'name': 'TEXT',
                'quantity': 'BIGINT',
                'amount_excl_tax': 'FLOAT',
                'amount_inc_tax': 'FLOAT'
            }
        )
        
        conn.commit()
        loaded_count = len(df)
        print(f"Successfully loaded {loaded_count} transactions to database")
        
        # Verify insertion
        
        cursor.execute("SELECT COUNT(*) FROM transactions")
        total_count_after = cursor.fetchone()[0]
        print(f"Total transactions in database: {total_count_after}")
        
        conn.close()
        return loaded_count
        
    except Exception as e:
        raise ValueError(f"Error loading data to database: {str(e)}")

