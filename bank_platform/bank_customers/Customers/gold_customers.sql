
CREATE MATERIALIZED VIEW gold_banking.gold_customers
AS
select 
customer_id,
national_id,
concat(first_name, last_name) as customer_full_name,
age,
case 
    when age < 18 then 'Under 18'
    when age < 25 then '18-24'
    when age < 35 then '25-34'
    when age < 45 then '35-44'
    when age < 55 then '45-54'
    when age < 65 then '55-64'
    when age < 75 then '65-74'
    when age >= 75 then '75+'
end as age_band,
segment,
risk_rating,
kyc_status, 
marketing_consent,
city,
region,
date(created_at) AS customer_since
from bank_platform.silver_banking.silver_customers
where op!='d'
