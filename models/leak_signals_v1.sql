-- leak_signals_v1.sql
-- dbt-style metric layer for SMB Revenue Leak Program v1
-- Assumes canonical normalized models:
--   {{ ref('fact_orders') }}, {{ ref('fact_order_lines') }}, {{ ref('fact_refunds') }}
--   {{ ref('fact_payments') }}, {{ ref('fact_invoices') }}, {{ ref('fact_support_tickets') }}
--   {{ ref('dim_customer') }}

{% set window_days = var('window_days', 28) %}
{% set baseline_days = var('baseline_days', 84) %}

with date_bounds as (
  select
    max(order_ts) as max_order_ts,
    max(payment_ts) as max_payment_ts,
    max(refund_ts) as max_refund_ts
  from {{ ref('fact_orders') }}
  left join {{ ref('fact_payments') }} on 1=1
  left join {{ ref('fact_refunds') }} on 1=1
),
anchor as (
  select coalesce(greatest(max_order_ts, max_payment_ts, max_refund_ts), current_timestamp) as anchor_ts
  from date_bounds
),
windows as (
  select
    anchor_ts,
    anchor_ts - interval '{{ window_days }} day' as w_start,
    anchor_ts as w_end,
    anchor_ts - interval '{{ window_days + baseline_days }} day' as b_start,
    anchor_ts - interval '{{ window_days }} day' as b_end
  from anchor
),

orders_w as (
  select o.*
  from {{ ref('fact_orders') }} o
  cross join windows w
  where o.order_ts >= w.w_start and o.order_ts < w.w_end
),
orders_b as (
  select o.*
  from {{ ref('fact_orders') }} o
  cross join windows w
  where o.order_ts >= w.b_start and o.order_ts < w.b_end
),
refunds_w as (
  select r.*
  from {{ ref('fact_refunds') }} r
  cross join windows w
  where r.refund_ts >= w.w_start and r.refund_ts < w.w_end
),
refunds_b as (
  select r.*
  from {{ ref('fact_refunds') }} r
  cross join windows w
  where r.refund_ts >= w.b_start and r.refund_ts < w.b_end
),
payments_w as (
  select p.*
  from {{ ref('fact_payments') }} p
  cross join windows w
  where p.payment_ts >= w.w_start and p.payment_ts < w.w_end
),
payments_b as (
  select p.*
  from {{ ref('fact_payments') }} p
  cross join windows w
  where p.payment_ts >= w.b_start and p.payment_ts < w.b_end
),
tickets_w as (
  select t.*
  from {{ ref('fact_support_tickets') }} t
  cross join windows w
  where t.created_ts >= w.w_start and t.created_ts < w.w_end
),
tickets_b as (
  select t.*
  from {{ ref('fact_support_tickets') }} t
  cross join windows w
  where t.created_ts >= w.b_start and t.created_ts < w.b_end
),

base_metrics as (
  select
    -- Core revenue and cost metrics
    (select coalesce(sum(net_revenue),0) from orders_w) as net_revenue_w,
    (select coalesce(sum(net_revenue),0) from orders_b) as net_revenue_b,
    (select coalesce(sum(gross_revenue),0) from orders_w) as gross_revenue_w,
    (select coalesce(sum(gross_revenue),0) from orders_b) as gross_revenue_b,
    (select coalesce(sum(discount_amount),0) from orders_w) as discount_w,
    (select coalesce(sum(discount_amount),0) from orders_b) as discount_b,
    (select coalesce(sum(shipping_cost),0) from orders_w) as shipping_cost_w,
    (select coalesce(sum(shipping_cost),0) from orders_b) as shipping_cost_b,
    (select coalesce(sum(cogs_total),0) from orders_w) as cogs_w,
    (select coalesce(sum(cogs_total),0) from orders_b) as cogs_b,

    -- Refund metrics
    (select coalesce(sum(refund_amount),0) from refunds_w) as refund_amount_w,
    (select coalesce(sum(refund_amount),0) from refunds_b) as refund_amount_b,

    -- Payment metrics
    (select count(*) from payments_w) as payment_attempts_w,
    (select count(*) from payments_b) as payment_attempts_b,
    (select count(*) from payments_w where status='failed') as payment_failed_w,
    (select count(*) from payments_b where status='failed') as payment_failed_b,
    (select coalesce(sum(amount),0) from payments_w where status='failed') as payment_failed_amount_w,
    (select coalesce(sum(dispute_amount),0) from payments_w where status='disputed') as dispute_amount_w,
    (select count(*) from payments_w where status='disputed') as dispute_count_w,
    (select count(*) from payments_b where status='disputed') as dispute_count_b,

    -- Support metrics
    (select count(*) from tickets_w) as tickets_w,
    (select count(*) from tickets_b) as tickets_b
),

ratios as (
  select
    *,
    case when net_revenue_w > 0 then refund_amount_w / net_revenue_w else 0 end as refund_rate_w,
    case when net_revenue_b > 0 then refund_amount_b / net_revenue_b else 0 end as refund_rate_b,
    case when gross_revenue_w > 0 then discount_w / gross_revenue_w else 0 end as discount_rate_w,
    case when gross_revenue_b > 0 then discount_b / gross_revenue_b else 0 end as discount_rate_b,
    case when net_revenue_w > 0 then shipping_cost_w / net_revenue_w else 0 end as shipping_ratio_w,
    case when net_revenue_b > 0 then shipping_cost_b / net_revenue_b else 0 end as shipping_ratio_b,
    case when payment_attempts_w > 0 then payment_failed_w * 1.0 / payment_attempts_w else 0 end as failed_pay_rate_w,
    case when payment_attempts_b > 0 then payment_failed_b * 1.0 / payment_attempts_b else 0 end as failed_pay_rate_b,
    case when net_revenue_w > 0 then (net_revenue_w - cogs_w - shipping_cost_w) / net_revenue_w else 0 end as margin_w,
    case when net_revenue_b > 0 then (net_revenue_b - cogs_b - shipping_cost_b) / net_revenue_b else 0 end as margin_b
  from base_metrics
),

sku_refund_w as (
  select
    ol.sku_id,
    sum(coalesce(r.refund_amount,0)) as refund_amount,
    sum(coalesce(ol.line_net,0)) as line_net
  from {{ ref('fact_order_lines') }} ol
  join {{ ref('fact_orders') }} o on o.order_id = ol.order_id
  left join {{ ref('fact_refunds') }} r on r.order_id = o.order_id
  cross join windows w
  where o.order_ts >= w.w_start and o.order_ts < w.w_end
  group by 1
),

coupon_stats_w as (
  select
    coupon_code,
    count(*) as uses,
    count(distinct customer_id) as users,
    sum(discount_value) as discount_value
  from {{ ref('fact_discounts') }} d
  cross join windows w
  where d.order_id in (select order_id from orders_w)
  group by 1
),

repeat_customers as (
  select
    c.customer_id,
    sum(case when o.order_ts >= w.w_start and o.order_ts < w.w_end then 1 else 0 end) as orders_w,
    sum(case when o.order_ts >= w.b_start and o.order_ts < w.b_end then 1 else 0 end) as orders_b
  from {{ ref('dim_customer') }} c
  left join {{ ref('fact_orders') }} o on o.customer_id = c.customer_id
  cross join windows w
  group by 1
)

-- Final metric block: one row with deterministic formulas for top 10 signals
select
  -- Signal 1: Refund Spike Leak
  case when refund_rate_w > refund_rate_b * 1.20 and refund_amount_w >= 500
       then refund_amount_w - (refund_rate_b * net_revenue_w)
       else 0 end as leak_01_refund_spike_usd,

  -- Signal 2: SKU Refund Concentration Leak (top 5)
  (
    select coalesce(sum(refund_amount),0)
    from (
      select refund_amount
      from sku_refund_w
      order by refund_amount desc
      limit 5
    ) t
  ) as leak_02_sku_refund_concentration_usd,

  -- Signal 3: Discount Overuse Leak
  case when discount_rate_w > discount_rate_b + 0.03
       then (discount_rate_w - greatest(discount_rate_b, 0.10)) * gross_revenue_w
       else 0 end as leak_03_discount_overuse_usd,

  -- Signal 4: Coupon Abuse Leak
  (
    select coalesce(sum(case when users > 0 and (uses * 1.0 / users) > 3 then discount_value else 0 end),0)
    from coupon_stats_w
  ) as leak_04_coupon_abuse_usd,

  -- Signal 5: Shipping Cost Creep Leak
  case when shipping_ratio_w > shipping_ratio_b * 1.15
       then shipping_cost_w - (shipping_ratio_b * net_revenue_w)
       else 0 end as leak_05_shipping_creep_usd,

  -- Signal 6: Failed Payment Recovery Leak
  case when failed_pay_rate_w > failed_pay_rate_b + 0.02
       then payment_failed_amount_w
       else 0 end as leak_06_failed_payment_recovery_usd,

  -- Signal 7: Dispute/Chargeback Leak
  case when dispute_count_w > dispute_count_b * 1.20
       then dispute_amount_w + (dispute_count_w * 15)
       else 0 end as leak_07_dispute_usd,

  -- Signal 8: Margin Compression Leak
  case when margin_w < margin_b - 0.03
       then (margin_b - margin_w) * net_revenue_w
       else 0 end as leak_08_margin_compression_usd,

  -- Signal 9: Support-Linked Refund Leak
  case when tickets_w > tickets_b * 1.20 and refund_amount_w > refund_amount_b * 1.10
       then refund_amount_w - refund_amount_b * ({{ window_days }} * 1.0 / {{ baseline_days }})
       else 0 end as leak_09_support_linked_refund_usd,

  -- Signal 10: Repeat-Customer Churned Revenue Leak
  (
    with rep as (
      select
        sum(case when orders_w > 0 then 1 else 0 end) as repeaters_w,
        sum(case when orders_b > 0 then 1 else 0 end) as repeaters_b
      from repeat_customers
    )
    select case when repeaters_b > repeaters_w
           then (repeaters_b - repeaters_w) * (select case when count(*)>0 then sum(net_revenue)/count(*) else 0 end from orders_w)
           else 0 end
    from rep
  ) as leak_10_repeat_customer_churn_usd,

  -- Reusable ratios for downstream severity/confidence layers
  refund_rate_w,
  refund_rate_b,
  discount_rate_w,
  discount_rate_b,
  shipping_ratio_w,
  shipping_ratio_b,
  failed_pay_rate_w,
  failed_pay_rate_b,
  margin_w,
  margin_b,
  net_revenue_w,
  gross_revenue_w
from ratios;