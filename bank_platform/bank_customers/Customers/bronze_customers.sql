CREATE OR REFRESH STREAMING TABLE bank_platform.bronze_banking.bronze_customers
AS 
SELECT *,
current_timestamp() as processing_ts,
_metadata.file_name as file_name,
_metadata.file_modification_time as file_modification_time
FROM STREAM read_files("/Volumes/bank_platform/bank_raw/landing/customers_cdc/",
FORMAT => 'json',
schemaEvolutionMode => 'addNewColumns'
)

