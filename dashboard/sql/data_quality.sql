-- Row counts per data-quality flag plus a deliberately naive net value
-- (sum over ALL rows) to show why revenue must filter on IS_REGULAR_SALE.
-- Flags overlap (e.g. every other negative adjustment also has price 0).
-- Parameters: start_date, end_date.
SELECT
    COUNT(*)                              AS TOTAL_LINES,
    COUNT_IF(IS_REGULAR_SALE)             AS REGULAR_SALE,
    COUNT_IF(IS_CANCELLATION)             AS CANCELLATION,
    COUNT_IF(IS_OTHER_NEGATIVE_ADJUSTMENT) AS OTHER_NEGATIVE_ADJUSTMENT,
    COUNT_IF(IS_NONPOSITIVE_PRICE)        AS NONPOSITIVE_PRICE,
    COUNT_IF(IS_MISSING_CUSTOMER)         AS MISSING_CUSTOMER,
    COALESCE(SUM(QUANTITY * UNIT_PRICE), 0) AS NAIVE_NET_VALUE
FROM ORDERS
WHERE INVOICE_DATE >= %(start_date)s
  AND INVOICE_DATE <  %(end_date)s
