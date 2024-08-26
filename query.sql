WITH monthly_revenue as(
    SELECT
        date(date_trunc('month',payment_date)) AS payment_month,
        user_id,
        game_name,
        sum (revenue_amount_usd) AS total_revenue
    FROM project.games_payments gp 
    GROUP BY 1,2,3
    ),
revenue_lag_lead_months as(
    SELECT
        *,
        date(payment_month- INTERVAL '1'month) AS previous_calendar_month,
        date(payment_month + INTERVAL '1' month)AS next_calendar_month,
        lag(total_revenue)OVER (PARTITION BY user_id ORDER BY payment_month) 
           AS previous_paid_month_revenue,
        lag(payment_month)OVER (PARTITION BY user_id ORDER BY payment_month) 
           AS previous_paid_month,
        lead(payment_month)over(PARTITION BY user_id ORDER BY payment_month) 
           AS next_paid_month
    FROM monthly_revenue),
revenue_metrics AS (
    SELECT
        payment_month,
        user_id,
        game_name,
        total_revenue,
        CASE  WHEN previous_paid_month IS NULL  THEN total_revenue END  AS  new_mrr,
        CASE  WHEN previous_paid_month = previous_calendar_month 
              AND total_revenue > previous_paid_month_revenue
              THEN total_revenue - previous_paid_month_revenue END AS expansion_revenue,
        CASE  WHEN previous_paid_month = previous_calendar_month
              AND total_revenue < previous_paid_month_revenue
              THEN total_revenue - previous_paid_month_revenue END AS contraction_revenue,
       CASE  WHEN next_paid_month IS null OR next_paid_month != next_calendar_month
              THEN total_revenue  END AS churned_revenue
       FROM  revenue_lag_lead_months)    
    SELECT 
        rm.*,
        gpu.LANGUAGE,
        gpu.has_older_device_model,
        gpu.age
    FROM revenue_metrics rm
LEFT JOIN project.games_paid_users gpu using(user_id)  
