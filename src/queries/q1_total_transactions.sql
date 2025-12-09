SELECT count(*) AS nb_transactions
FROM transactions
WHERE transaction_date = ? 
;
