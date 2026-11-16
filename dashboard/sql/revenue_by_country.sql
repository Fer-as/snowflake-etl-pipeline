-- Regular-sale revenue by the customer's primary country.
-- ORDERS has no country column: country lives in CUSTOMERS.PRIMARY_COUNTRY
-- (the customer's most frequent country). Lines without a customer ID
-- cannot be attributed and are grouped as 'Unidentified customer'.
-- Parameters: start_date, end_date.
SELECT
    COALESCE(c.PRIMARY_COUNTRY, 'Unidentified customer') AS COUNTRY,
    SUM(o.QUANTITY * o.UNIT_PRICE)                       AS REVENUE,
    COUNT(DISTINCT o.CUSTOMER_ID)                        AS CUSTOMERS,
    COUNT(DISTINCT o.INVOICE_NUMBER)                     AS ORDERS
FROM ORDERS AS o
LEFT JOIN CUSTOMERS AS c
    ON c.CUSTOMER_ID = o.CUSTOMER_ID
WHERE o.IS_REGULAR_SALE
  AND o.INVOICE_DATE >= %(start_date)s
  AND o.INVOICE_DATE <  %(end_date)s
GROUP BY 1
ORDER BY REVENUE DESC, COUNTRY
