CREATE EXTERNAL TABLE IF NOT EXISTS shopify_inventory_report(

sku INT
, sku_name STRING
, forecast STRING
, lower_bound DOUBLE
, upper_bound DOUBLE
, inventory_on_hand INT
, days_of_stock_onhand INT


)
PARTITIONED BY 
(
partition_date DATE 
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LOCATION 's3://prymal-analytics/shopify/inventory_report/' 
TBLPROPERTIES ("skip.header.line.count"="1")