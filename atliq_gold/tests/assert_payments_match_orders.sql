-- Reconciliation: any paid order whose amount differs from its (de-duplicated) payment fails the build.
SELECT o.order_id, o.order_amount, SUM(p.amount) AS total_paid
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_payments') }} p ON o.order_id = p.order_id
GROUP BY o.order_id, o.order_amount
HAVING SUM(p.amount) <> o.order_amount
