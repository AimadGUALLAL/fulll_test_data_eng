SELECT round(sum(amount_inc_tax),2)
FROM transactions 
WHERE category = 'SELL' ;
