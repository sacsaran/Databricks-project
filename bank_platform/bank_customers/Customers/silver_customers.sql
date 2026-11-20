
CREATE OR REFRESH STREAMING TABLE silver_banking.silver_customers(
CONSTRAINT has_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
CONSTRAINT valid_op        EXPECT (op IN ('r','c','u','d')) ON VIOLATION DROP ROW,
CONSTRAINT valid_event EXPECT(event_ts_ms IS NOT NULL) ON VIOLATION DROP ROW
)
AS
SELECT 
    customer_id,
    national_id,
    first_name, 
    last_name,
    cast(date_of_birth as date) as date_of_birth,
    round(datediff(current_date(), cast(date_of_birth as date))/365.25,0) as age,
    trim(email) as email,
    segment,
    timestamp_millis(source_ts_ms) as event_ts_ms,
    op,
    risk_rating,
    replace(phone,' ','') as phone,
    address_line,
    city,
    region, 
    postcode,
    kyc_status,
    marketing_consent,
    COALESCE(language, 'English') AS preferred_language,
    cast(updated_at as timestamp) as updated_at,
    cast(created_at as timestamp) as created_at
FROM  STREAM (bank_platform.bronze_banking.bronze_customers) as customer
LEFT JOIN 
   read_files("/Volumes/bank_platform/silver_banking/external_data/language.csv",
 format => 'csv', header => true
) AS lg
 on customer.preferred_language=lg.code;
