-- Headline KPIs for the selected date range.
-- Revenue uses ONLY regular sales (positive quantity, positive price,
-- not a cancellation), so cancellations and adjustments are never netted
-- off or double counted. Cancelled value is reported separately.
-- Parameters: start_date (inclusive), end_date (exclusive).
SELECT
    COUNT(*)                                                        AS TOTAL_LINES,
    COUNT(DISTINCT INVOICE_NUMBER)                                  AS TOTAL_INVOICES,
    COUNT_IF(IS_REGULAR_SALE)                                       AS REGULAR_SALE_LINES,
    COUNT(DISTINCT IFF(IS_REGULAR_SALE, INVOICE_NUMBER, NULL))      AS SALES_ORDERS,
    COALESCE(SUM(IFF(IS_REGULAR_SALE, QUANTITY * UNIT_PRICE, 0)), 0) AS REVENUE,
    COALESCE(SUM(IFF(IS_REGULAR_SALE, QUANTITY, 0)), 0)             AS UNITS_SOLD,
    COUNT(DISTINCT IFF(IS_REGULAR_SALE, CUSTOMER_ID, NULL))         AS ACTIVE_CUSTOMERS,
    COUNT(DISTINCT IFF(IS_CANCELLATION, INVOICE_NUMBER, NULL))      AS CANCELLED_INVOICES,
    COALESCE(-SUM(IFF(IS_CANCELLATION, QUANTITY * UNIT_PRICE, 0)), 0) AS CANCELLED_VALUE,
    COALESCE(SUM(IFF(IS_REGULAR_SALE AND IS_MISSING_CUSTOMER, QUANTITY * UNIT_PRICE, 0)), 0)
                                                                    AS REVENUE_MISSING_CUSTOMER
FROM ORDERS
WHERE INVOICE_DATE >= %(start_date)s
  AND INVOICE_DATE <  %(end_date)s
