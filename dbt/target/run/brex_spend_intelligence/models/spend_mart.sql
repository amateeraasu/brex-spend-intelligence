
  
    

  create  table "postgres"."public_mart"."spend_mart__dbt_tmp"
  
  
    as
  
  (
    

/*
  spend_mart.sql
  ──────────────
  Aggregates raw.transactions by department, category, and month.
  Adds month-over-month (MoM) spend change and AI/policy violation rates.

  Columns produced:
    dept_category_month   Surrogate grain key
    department
    category
    spend_month           First day of the month (DATE)
    total_spend           Sum of amount_usd
    transaction_count
    avg_transaction       Average spend per transaction
    policy_violation_cnt  Rule-based violations from generate_data.py
    ai_flagged_cnt        Violations flagged by Claude
    policy_violation_pct  % of transactions that violated policy
    ai_flagged_pct        % of transactions AI-flagged
    prev_month_spend      Spend in the prior calendar month
    mom_change_usd        Absolute MoM change
    mom_change_pct        Relative MoM change (NULL for first month in window)
*/

with

base as (

    select
        department,
        category,
        date_trunc('month', transaction_date)::date as spend_month,
        amount_usd,
        policy_violation,
        coalesce(ai_flagged, false)                 as ai_flagged

    from "postgres"."raw"."transactions"
    where transaction_date is not null

),

monthly as (

    select
        department,
        category,
        spend_month,

        round(sum(amount_usd)::numeric, 2)          as total_spend,
        count(*)                                     as transaction_count,
        round(avg(amount_usd)::numeric, 2)           as avg_transaction,

        sum(case when policy_violation then 1 else 0 end)
                                                     as policy_violation_cnt,
        sum(case when ai_flagged       then 1 else 0 end)
                                                     as ai_flagged_cnt,

        round(
            100.0 * sum(case when policy_violation then 1 else 0 end)
            / nullif(count(*), 0), 2
        )                                            as policy_violation_pct,

        round(
            100.0 * sum(case when ai_flagged then 1 else 0 end)
            / nullif(count(*), 0), 2
        )                                            as ai_flagged_pct

    from base
    group by 1, 2, 3

),

with_mom as (

    select
        m.*,

        lag(m.total_spend) over (
            partition by m.department, m.category
            order by m.spend_month
        )                                            as prev_month_spend,

        round(
            m.total_spend - lag(m.total_spend) over (
                partition by m.department, m.category
                order by m.spend_month
            ), 2
        )                                            as mom_change_usd,

        round(
            100.0 * (
                m.total_spend - lag(m.total_spend) over (
                    partition by m.department, m.category
                    order by m.spend_month
                )
            ) / nullif(lag(m.total_spend) over (
                partition by m.department, m.category
                order by m.spend_month
            ), 0), 2
        )                                            as mom_change_pct

    from monthly m

)

select
    department
        || '_' || category
        || '_' || to_char(spend_month, 'YYYY_MM')   as dept_category_month,
    department,
    category,
    spend_month,
    total_spend,
    transaction_count,
    avg_transaction,
    policy_violation_cnt,
    ai_flagged_cnt,
    policy_violation_pct,
    ai_flagged_pct,
    prev_month_spend,
    mom_change_usd,
    mom_change_pct

from with_mom
order by department, category, spend_month
  );
  