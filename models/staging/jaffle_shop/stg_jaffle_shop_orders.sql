SELECT
    id AS order_id,
    user_id AS customer_id,
    order_date,
    status,
    _etl_loaded_at AS etl_loaded_at
FROM {{ source('jaffle_shop', 'orders') }}