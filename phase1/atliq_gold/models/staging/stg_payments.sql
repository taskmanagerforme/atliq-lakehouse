-- keep the first payment when the gateway retried; test-account orders excluded
SELECT p.payment_id, p.order_id, p.amount, p.method, p.paid_at
FROM {{ source('silver', 'payments') }} p
JOIN {{ ref('stg_orders') }} o ON p.order_id = o.order_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY p.order_id ORDER BY p.paid_at, p.payment_id) = 1
