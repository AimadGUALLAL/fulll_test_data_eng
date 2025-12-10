"""
Business Queries - Execute SQL queries from files and display results
"""
import sqlite3
import pandas as pd
from pathlib import Path


# Paths
SQL_DIR = Path(__file__).parent / "queries"
DEFAULT_DB = "data/database/retail.db"


def load_query(name: str) -> str :
    """ Load the queries from the files of answers .sql """

    path = SQL_DIR / name

    if not path.exists():

        raise FileNotFoundError(f"Query file not found: {path}")
    
    return path.read_text()


def execute_query(db_path: str, query_file:str , params: tuple =()):

    """Return the result of the query"""

    query = load_query(query_file)

    conn = sqlite3.connect(db_path)

    cur = conn.cursor()

    cur.execute(query, params)

    return cur.fetchall()
    


# Business queries

# QST 1 : le nombre de transaction pour une date donnée
def get_transactions_count_by_date(db_path: str , date: str) -> int:

    rows = execute_query(db_path, "q1_total_transactions.sql", (date,))

    return rows[0][0] if rows else 0

# QST 2 : le total tax inclus des transactions type sell

def get_total_amount_tax_inc_sell(db_path: str):

    rows = execute_query(db_path, "q2_total_amount.sql")

    return rows[0][0]


# QST 3 : la balance entre sell et buy par date pour un produit spécifique

def get_balance_by_date(db_path : str , product: str):

    rows = execute_query(db_path, "q3_balance.sql", (product,))

    return [
        {"date" : r[0], "balance": r[1]}
        for r in rows
    ]





if __name__ == '__main__':

    qst1_res = get_transactions_count_by_date(DEFAULT_DB, "2022-01-15")

    print("le nombre total des transactions le 2022-01-15 :", qst1_res)

    qst2_res = get_total_amount_tax_inc_sell(DEFAULT_DB)

    print("le montant total avec taxe inluse des sell :", qst2_res)

    
    qst3_res = get_balance_by_date(DEFAULT_DB, "Amazon Echo Dot")

    data_table = pd.DataFrame(qst3_res)

    print("le résultat des balances par date sous format tabulaire : \n", data_table)