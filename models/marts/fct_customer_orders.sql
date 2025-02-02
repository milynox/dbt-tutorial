with
    -- Import CTEs
    -- See all the tables used in this file at the top
    customers as (select * from {{ ref('stg_jaffle_shop__customers') }}),

    orders as (select * from {{ ref('stg_jaffle_shop__orders') }}),

    payments as (select * from {{ ref('stg_stripe__payments') }}),

    -- Logical CTEs
    completed_payments as (  -- total amount has been paid for an order
        select
            order_id,
            max(payment_created_at) as payment_finalized_date,
            sum(payment_amount) as total_amount_paid
        from payments
        where payment_status <> 'fail'
        group by 1
    ),

    paid_orders as (  -- detail info of these orders 
        select
            orders.order_id,
            orders.customer_id,
            orders.order_placed_at,
            orders.order_status,
            completed_payments.total_amount_paid,
            completed_payments.payment_finalized_date,
            customers.customer_first_name,
            customers.customer_last_name
        from orders
        left join completed_payments on orders.order_id = completed_payments.order_id
        left join customers on orders.customer_id = customers.customer_id
    ),

    -- Final CTEs
    final as (
        select
            paid_orders.*,
            row_number() over (order by paid_orders.order_id) as transaction_seq,
            row_number() over (
                partition by customer_id order by paid_orders.order_id
            ) as customer_sales_seq,

            -- new vs returning customer for each customer
            case
                when
                    (
                        row_number() over (
                            partition by paid_orders.customer_id
                            order by paid_orders.order_placed_at, paid_orders.order_id
                        )
                        = 1
                    )
                then 'new'
                else 'return'
            end as nvsr,

            -- Customer lifetime value for each order
            -- Với từng đơn hàng của từng khách, tính tổng doanh thu của các đơn trước đó.
            sum(paid_orders.total_amount_paid) over (
                partition by paid_orders.customer_id
                order by paid_orders.order_id asc
                rows between unbounded preceding and current row
            ) as customer_lifetime_value,

            -- first day of sale
            first_value(paid_orders.order_placed_at) over (
                partition by paid_orders.customer_id
                order by paid_orders.order_placed_at
            ) as fdos

        from paid_orders
        order by paid_orders.order_id
    )

-- Simple Select Statement
select *
from final
