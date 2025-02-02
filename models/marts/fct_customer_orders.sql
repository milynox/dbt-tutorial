with
    -- Import CTEs
    -- See all the tables used in this file at the top
    customers as (select * from {{ source("jaffle_shop", "customers") }}),

    orders as (select * from {{ source("jaffle_shop", "orders") }}),

    payments as (select * from {{ source("stripe", "payments") }}),

    -- Logical CTEs
    completed_payments as (  -- total amount has been paid for an order
        select
            orderid as order_id,
            max(created) as payment_finalized_date,
            sum(amount) / 100.0 as total_amount_paid
        from payments
        where status <> 'fail'
        group by 1
    ),

    paid_orders as (  -- detail info of these orders 
        select
            orders.id as order_id,
            orders.user_id as customer_id,
            orders.order_date as order_placed_at,
            orders.status as order_status,
            completed_payments.total_amount_paid,
            completed_payments.payment_finalized_date,
            customers.first_name as customer_first_name,
            customers.last_name as customer_last_name
        from orders
        left join completed_payments on orders.id = completed_payments.order_id
        left join customers on orders.user_id = customers.id
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
        -- left join customer_orders as c using (customer_id)
        -- left outer join x on x.order_id = p.order_id
        order by paid_orders.order_id
    )

-- Simple Select Statement
select *
from final
