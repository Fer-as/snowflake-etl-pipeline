-- Top products by regular-sale revenue (gross) or net of cancellations.
-- REVENUE      = regular-sale value only (the dashboard's revenue rule).
-- NET_REVENUE  = regular-sale value + cancellation value (cancellations
--                have negative quantity, so they reduce the total).
-- Net ranking stops one huge order that was cancelled minutes later
-- (e.g. 80,995 x stock code 23843 on 2011-12-09) from topping the list.
-- Service/non-merchandise codes (postage, manual lines, fees, bad-debt
-- adjustment, gift vouchers) are excluded because they are not products.
-- The code list must match SERVICE_STOCK_CODES in dashboard/data_sources.py
-- (enforced by tests/test_dashboard.py).
-- Parameters: start_date, end_date, rank_by_net (boolean), limit.
SELECT
    o.STOCK_CODE,
    COALESCE(MAX(p.DESCRIPTION), '(no description)')                 AS DESCRIPTION,
    SUM(IFF(o.IS_REGULAR_SALE, o.QUANTITY * o.UNIT_PRICE, 0))        AS REVENUE,
    SUM(o.QUANTITY * o.UNIT_PRICE)                                   AS NET_REVENUE,
    SUM(IFF(o.IS_REGULAR_SALE, o.QUANTITY, 0))                       AS UNITS,
    COUNT(DISTINCT IFF(o.IS_REGULAR_SALE, o.INVOICE_NUMBER, NULL))   AS ORDERS
FROM ORDERS AS o
LEFT JOIN PRODUCTS AS p
    ON p.STOCK_CODE = o.STOCK_CODE
WHERE (o.IS_REGULAR_SALE OR o.IS_CANCELLATION)
  AND o.INVOICE_DATE >= %(start_date)s
  AND o.INVOICE_DATE <  %(end_date)s
  AND o.STOCK_CODE NOT IN ('AMAZONFEE', 'B', 'BANK CHARGES', 'C2', 'CRUK', 'D', 'DOT', 'M', 'PADS', 'POST', 'S', 'm')
  AND NOT STARTSWITH(o.STOCK_CODE, 'gift_')
GROUP BY o.STOCK_CODE
HAVING SUM(IFF(o.IS_REGULAR_SALE, 1, 0)) > 0
ORDER BY IFF(%(rank_by_net)s, NET_REVENUE, REVENUE) DESC, o.STOCK_CODE
LIMIT %(limit)s
