import unittest
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.run_queries import (
    get_balance_by_date,
    get_total_amount_tax_inc_sell,
    get_transactions_count_by_date
)



class TransactionTest(unittest.TestCase):
    def setUp(self):
        self.db_path = "data/database/retail.db"


    def test_number_of_transactions_on_15_01_2022(self):
        expected_nb_transactions = 54
        # check the number of transactions in the database
        # after loading data from the CSV file

        real_nb_transactions = get_transactions_count_by_date(self.db_path, "2022-01-15")

        #verify firs if its a valid number
        self.assertIsInstance(real_nb_transactions, int)
        self.assertGreaterEqual(real_nb_transactions, 0)
        #verifiy the expected result
        self.assertEqual(real_nb_transactions, expected_nb_transactions)

    
    def test_number_of_transactions_on_14_01_2022(self):

        expected_nb_transactions = 47

        real_nb_transactions = get_transactions_count_by_date(self.db_path, "2022-01-14")

        self.assertIsInstance(real_nb_transactions, int)
        self.assertGreaterEqual(real_nb_transactions, 0)
        self.assertEqual(real_nb_transactions, expected_nb_transactions)

    def test_total_sell_ammount(self):
        
        expected_total = 360448.98
        real_total = get_total_amount_tax_inc_sell(self.db_path)
        
        self.assertIsNotNone(real_total)
        self.assertIsInstance(real_total, (int, float))
        self.assertEqual(real_total, expected_total)

    def test_balance_by_date_for_amazon_echo_dot(self):

        # i will test using the first 3 rows 

        first_expected_three_rows = [
            {"date" : "2022-01-01", "balance": -2},
            {"date" : "2022-01-02", "balance": 8},
            {"date": "2022-01-03", "balance":7 }            
            ]
        
        results = get_balance_by_date(self.db_path, "Amazon Echo Dot")


        self.assertIsInstance(results, list)

        for elt in results:
            self.assertIn("date", elt)
            self.assertIn("balance", elt)
            self.assertIsInstance(elt["balance"], (int,float))

        
        self.assertEqual(results[:3], first_expected_three_rows)

    def test_balance_by_date_another_product(self):

        product = "Fitbit Charge 5"

        expected_three_rows = [
            {"date":"2022-01-01", "balance":2},
            {"date": "2022-01-02", "balance":12},
            {"date":"2022-01-03", "balance":4}
        ]

        results = get_balance_by_date(self.db_path, product)


        self.assertIsInstance(results, list)

        for elt in results:
            self.assertIn("date", elt)
            self.assertIn("balance", elt)
            self.assertIsInstance(elt["balance"], (int,float))

        
        self.assertEqual(results[:3], expected_three_rows)

    
    def test_with_non_existing_product(self):

        results = get_balance_by_date(self.db_path, "Unknown product X")

        self.assertIsInstance(results, list)

        self.assertEqual(len(results), 0, "Should return empty list for non-existent product")


if __name__ == '__main__':
    unittest.main()