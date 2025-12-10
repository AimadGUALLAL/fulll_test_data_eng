import unittest
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd

from src.etl.extract import extract_date_from_filename, extract_data_from_csv
from src.etl.transform import transform_data
from src.etl.load import check_duplicates, load_to_database
from src.run_etl import run_etl

class TestETLFunctions(unittest.TestCase):
    
    def setUp(self):
        self.file_path = "data/raw/retail_15_01_2022.csv"
        self.db_path = "data/database/retail.db"
    
    # Testing the extract step 
    def test_extract_valid_date(self):
        
        
        csv_path = Path(self.file_path)
        expected_date = "2022-01-15"

        result = extract_date_from_filename(csv_path)

        self.assertEqual(result,expected_date)
    

    def test_extract_date_invalid_filename(self):
        file = Path("wrong_name.csv")
        with self.assertRaises(ValueError):
            extract_date_from_filename(file)
    

    def test_extract_data_from_csv(self):
        
        
        result_df, date = extract_data_from_csv(self.file_path)


        self.assertIsInstance(result_df, pd.DataFrame)

        self.assertEqual(len(result_df), 54)

        self.assertEqual(date, "2022-01-15")

    
    def test_extract_data_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            extract_data_from_csv("not_existing_file.csv")

    # testing the transformation step
    def test_transform_module(self):

        df = pd.read_csv(self.file_path)
        transformed_df = transform_data(df, "2022-01-15")

        expected_column_order = [
        'id', 
        'transaction_date', 
        'category', 
        'name', 
        'quantity', 
        'amount_excl_tax', 
        'amount_inc_tax']

        cols = list(transformed_df.columns)
        #check if description is renamed with name
        self.assertIn("name", cols)
        # check if transaction date column is added
        self.assertIn("transaction_date", cols)
        # check the same order
        self.assertEqual(cols, expected_column_order)
        # check if dataframe has the same length
        self.assertEqual(len(df), 54)


    # testing the loading step 

    # lets test the check_dupliaces function first and then load 

    def test_check_duplicates(self):

        # we test with the same data extracted from the csv file , at this level , the data is already in the database
        # we take into account that and we prepare the tests for

        df = pd.read_csv(self.file_path)

        duplicates , new_df = check_duplicates(df, self.db_path)

        self.assertIsInstance(duplicates, int)
        self.assertIsInstance(new_df, pd.DataFrame)

        # we know that all the 54 transactions of this file are  in the db
        self.assertEqual(duplicates, len(df))
        # the new df to insert is empty
        self.assertEqual(len(new_df), 0)


    def test_check_duplicates_with_new_data(self):
        #check for two new transactions
        df = pd.DataFrame({
            "id": ["1abc", "2abc"],
            "transaction_date" : ["2022-01-16", "2022-01-16"],
            "category": ["SELL", "BUY"],
            "name": ["Item A", "Item B"],
            "quantity": [1, 2],
            "amount_excl_tax": [10, 20],
            "amount_inc_tax": [12, 24] })

        duplicates , new_df = check_duplicates(df, self.db_path)

        self.assertIsInstance(duplicates, int)
        self.assertIsInstance(new_df, pd.DataFrame)

        self.assertEqual(duplicates,0)
        self.assertEqual(len(new_df), 2)

    
    def test_load_to_database(self):
        # testing with two new transactions 
        # will be deleted just after 
        df = pd.DataFrame({
            "id": ["1abc", "2abc"],
            "transaction_date" : ["2022-01-16", "2022-01-16"],
            "category": ["SELL", "BUY"],
            "name": ["Item A", "Item B"],
            "quantity": [1, 2],
            "amount_excl_tax": [10, 20],
            "amount_inc_tax": [12, 24] })

        count_loaded = load_to_database(df, self.db_path)

        self.assertIsInstance(count_loaded, int)

        self.assertEqual(count_loaded, 2)
    
    def test_load_to_database_with_empty_df(self):

        df = pd.DataFrame()

        count_loaded = load_to_database(df, self.db_path)

        self.assertEqual(count_loaded, 0)
    

    def test_load_to_database_with_invalid_df(self):
        df = pd.DataFrame({
            "id_changed": ["1abc", "2abc"],
            "transaction_date_changed" : ["2022-01-16", "2022-01-16"],
            "category_changed": ["SELL", "BUY"],
            "name_changed": ["Item A", "Item B"],
            "quantity": [1, 2],
            "amount_excl_tax": [10, 20],
            "amount_inc_tax": [12, 24] })

        with self.assertRaises(ValueError):
            load_to_database(df, self.db_path)

        
    # test also the function that run the etl pipeline 

    def test_etl_runner(self):

        status = run_etl(self.file_path , self.db_path)

        self.assertIsInstance(status, int)

        self.assertEqual(status, 0)










if __name__ == '__main__':
    unittest.main()



    
    
