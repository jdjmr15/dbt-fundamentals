WITH orders as (

    SELECT order_id,
           customer_id AS user_id,
            order_date
    FROM {{ ref('stg_jaffle_shop_orders') }}

),


payments as (

    SELECT * FROM {{ ref('stg_stripe__payments') }}

),

order_payments as (

    SELECT
        order_id,
        sum(CASE WHEN status = 'success' THEN amount END) AS amount
    FROM payments
    GROUP BY order_id

)



SELECT
    o.order_id AS order_id,
    o.user_id AS customer_id,
    o.order_date AS order_date,
    coalesce(op.amount, 0) AS amount
FROM orders o
LEFT JOIN order_payments op
ON o.order_id = op.order_id