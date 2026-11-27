CREATE MATERIALIZED VIEW gold_banking.customer_by_segment
AS
select 
segment,
marketing_consent,
region,
COUNT(customer_id) as customer_count,
risk_rating 
from bank_platform.gold_banking.gold_customers
GROUP BY ALL