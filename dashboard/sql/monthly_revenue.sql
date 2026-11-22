-- Regular-sale revenue and order count per calendar month.
-- Parameters: start_date (inclusive), end_date (exclusive).
SELECT
    DATE_TRUNC('MONTH', INVOICE_DATE)::DATE AS MONTH,
    SUM(QUANTITY * UNIT_PRICE)              AS REVENUE,
    COUNT(DISTINCT INVOICE_NUMBER)          AS ORDERS
FROM ORDERS
WHERE IS_REGULAR_SALE
  AND INVOICE_DATE >= %(start_date)s
  AND INVOICE_DATE <  %(end_date)s
GROUP BY 1
ORDER BY 1
