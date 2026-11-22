-- One row per identified customer with their regular-sale activity.
-- Rows with IS_MISSING_CUSTOMER are excluded (no customer to attribute).
-- Parameters: start_date, end_date.
SELECT
    o.CUSTOMER_ID,
    COUNT(DISTINCT o.INVOICE_NUMBER) AS ORDERS,
    SUM(o.QUANTITY * o.UNIT_PRICE)   AS REVENUE,
    MIN(o.INVOICE_DATE)              AS FIRST_PURCHASE,
    MAX(o.INVOICE_DATE)              AS LAST_PURCHASE
FROM ORDERS AS o
WHERE o.IS_REGULAR_SALE
  AND NOT o.IS_MISSING_CUSTOMER
  AND o.INVOICE_DATE >= %(start_date)s
  AND o.INVOICE_DATE <  %(end_date)s
GROUP BY o.CUSTOMER_ID
