SELECT transaction_date ,
SUM(CASE WHEN category ='SELL' THEN quantity ELSE 0 END) - SUM(CASE WHEN category = 'BUY' THEN quantity ELSE 0 END) AS balance
FROM transactions
WHERE name = ? 
GROUP by transaction_date ;