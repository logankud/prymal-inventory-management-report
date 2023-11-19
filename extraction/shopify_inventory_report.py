import boto3 
import botocore
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError, PartialCredentialsError, ParamValidationError, WaiterError
import loguru
from loguru import logger
import os
import io
import pandas as pd
import numpy as np
from datetime import timedelta

AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY=os.environ['AWS_SECRET_ACCESS_KEY']


# ---------------------------------------
# FUNCTIONS
# ---------------------------------------

# FUNCTION TO EXECUTE ATHENA QUERY AND RETURN RESULTS
# ----------

def run_athena_query(query:str, database: str, region:str):

        
    # Initialize Athena client
    athena_client = boto3.client('athena', 
                                 region_name=region,
                                 aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                                 aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])

    # Execute the query
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': 's3://prymal-ops/athena_query_results/'  # Specify your S3 bucket for query results
            }
        )

        query_execution_id = response['QueryExecutionId']

        # Wait for the query to complete
        state = 'RUNNING'

        while (state in ['RUNNING', 'QUEUED']):
            response = athena_client.get_query_execution(QueryExecutionId = query_execution_id)
            logger.info(f'Query is in {state} state..')
            if 'QueryExecution' in response and 'Status' in response['QueryExecution'] and 'State' in response['QueryExecution']['Status']:
                # Get currentstate
                state = response['QueryExecution']['Status']['State']

                if state == 'FAILED':
                    logger.error('Query Failed!')
                elif state == 'SUCCEEDED':
                    logger.info('Query Succeeded!')
            

        # OBTAIN DATA

        # --------------



        query_results = athena_client.get_query_results(QueryExecutionId=query_execution_id,
                                                MaxResults= 1000)
        


        # Extract qury result column names into a list  

        cols = query_results['ResultSet']['ResultSetMetadata']['ColumnInfo']
        col_names = [col['Name'] for col in cols]



        # Extract query result data rows
        data_rows = query_results['ResultSet']['Rows'][1:]



        # Convert data rows into a list of lists
        query_results_data = [[r['VarCharValue'] for r in row['Data']] for row in data_rows]



        # Paginate Results if necessary
        while 'NextToken' in query_results:
                query_results = athena_client.get_query_results(QueryExecutionId=query_execution_id,
                                                NextToken=query_results['NextToken'],
                                                MaxResults= 1000)



                # Extract quuery result data rows
                data_rows = query_results['ResultSet']['Rows'][1:]


                # Convert data rows into a list of lists
                query_results_data.extend([[r['VarCharValue'] for r in row['Data']] for row in data_rows])



        results_df = pd.DataFrame(query_results_data, columns = col_names)
        
        return results_df


    except ParamValidationError as e:
        logger.error(f"Validation Error (potential SQL query issue): {e}")
        # Handle invalid parameters in the request, such as an invalid SQL query

    except WaiterError as e:
        logger.error(f"Waiter Error: {e}")
        # Handle errors related to waiting for query execution

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        
        if error_code == 'InvalidRequestException':
            logger.error(f"Invalid Request Exception: {error_message}")
            # Handle issues with the Athena request, such as invalid SQL syntax
            
        elif error_code == 'ResourceNotFoundException':
            logger.error(f"Resource Not Found Exception: {error_message}")
            # Handle cases where the database or query execution does not exist
            
        elif error_code == 'AccessDeniedException':
            logger.error(f"Access Denied Exception: {error_message}")
            # Handle cases where the IAM role does not have sufficient permissions
            
        else:
            logger.error(f"Athena Error: {error_code} - {error_message}")
            # Handle other Athena-related errors

    except Exception as e:
        logger.error(f"Other Exception: {str(e)}")
        # Handle any other unexpected exceptions


def generate_daily_run_rate(selected_value):

    logger.info(f'UPDATING FORECAST TABLE - {selected_value}')

    # DAILY QTY SOLD
    # -----

    # Calculate daily dataframe
    daily_df = result_df.loc[result_df['sku_name']==selected_value].sort_values('order_date',ascending=False)
    product_sku = daily_df['sku'].unique().tolist()[0]

    # Calculate statistics for past 7, 14, 30 & 60 days
    last_7_median = daily_df.head(7)['qty_sold'].median()
    last_7_p25 = np.percentile(daily_df.head(7)['qty_sold'],25)
    last_7_p75 = np.percentile(daily_df.head(7)['qty_sold'],75)

    last_14_median = daily_df.head(14)['qty_sold'].median()
    last_14_p25 = np.percentile(daily_df.head(14)['qty_sold'],25)
    last_14_p75 = np.percentile(daily_df.head(14)['qty_sold'],75)

    last_30_median = daily_df.head(30)['qty_sold'].median()
    last_30_p25 = np.percentile(daily_df.head(30)['qty_sold'],25)
    last_30_p75 = np.percentile(daily_df.head(30)['qty_sold'],75)

    last_60_median = daily_df.head(60)['qty_sold'].median()
    last_60_p25 = np.percentile(daily_df.head(60)['qty_sold'],25)
    last_60_p75 = np.percentile(daily_df.head(60)['qty_sold'],75)

    # Consolidate stats
    recent_stats_df = pd.DataFrame([[last_7_p25, last_7_median, last_7_p75],
                [last_14_p25, last_14_median, last_14_p75],
                [last_30_p25, last_30_median, last_30_p75],
                [last_60_p25, last_60_median, last_60_p75]],
                columns=['percentile_25','median','percentile_75'])



    # Calculate median of lower bound (median) and upper bound (75th percentile) 
    lower_bound = recent_stats_df['median'].median()
    upper_bound = recent_stats_df['percentile_75'].median()

    # Extrapolate out 30, 60, 90 days
    daily_run_rate = ['forecast_daily', lower_bound,upper_bound]
    
    # Consolidate into dataframe
    df = pd.DataFrame(daily_run_rate,
                columns=['forecast','lower_bound','upper_bound'])
    
    # Append product name & sku
    df['sku'] = product_sku
    df['sku_name'] = selected_value

    # Set partition date to yesterday (data as of yesterday)
    df['partition_date'] = pd.to_datetime(pd.to_datetime('today') - timedelta(1)).strftime('%Y-%m-%d')


    return df[['partition_date','sku','sku_name','forecast','lower_bound','upper_bound']]


# Check S3 Path for Existing Data
# -----------

def check_path_for_objects(bucket: str, s3_prefix:str):

  logger.info(f'Checking for existing data in {bucket}/{s3_prefix}')

  # Create s3 client
  s3_client = boto3.client('s3', 
                          region_name = REGION,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

  # List objects in s3_prefix
  result = s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_prefix )

  # Instantiate objects_exist
  objects_exist=False

  # Set objects_exist to true if objects are in prefix
  if 'Contents' in result:
      objects_exist=True

      logger.info('Data already exists!')

  return objects_exist

# Delete Existing Data from S3 Path
# -----------

def delete_s3_prefix_data(bucket:str, s3_prefix:str):


  logger.info(f'Deleting existing data from {bucket}/{s3_prefix}')

  # Create an S3 client
  s3_client = boto3.client('s3', 
                          region_name = REGION,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

  # Use list_objects_v2 to list all objects within the specified prefix
  objects_to_delete = s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_prefix)

  # Extract the list of object keys
  keys_to_delete = [obj['Key'] for obj in objects_to_delete.get('Contents', [])]

  # Check if there are objects to delete
  if keys_to_delete:
      # Delete the objects using 'delete_objects'
      response = s3_client.delete_objects(
          Bucket=bucket,
          Delete={'Objects': [{'Key': key} for key in keys_to_delete]}
      )
      logger.info(f"Deleted {len(keys_to_delete)} objects")
  else:
      logger.info("No objects to delete")


# ========================================================================
# Execute Code
# ========================================================================

DATABASE = 'prymal-analytics'
REGION = 'us-east-1'

# Construct query to pull data by product
# ----

QUERY = f"""SELECT partition_date
            , order_date
            , sku
            , sku_name
            , SUM(qty_sold) as qty_sold 
            FROM shopify_qty_sold_by_sku_daily 
            
            WHERE partition_date >= DATE(current_date - interval '120' day)
            GROUP BY partition_date
            , order_date
            , sku
            , sku_name
            ORDER BY order_date ASC
            """

# Query datalake to get quantiy sold per sku for the last 120 days
# ----

result_df = run_athena_query(query=QUERY, database=DATABASE, region=REGION)
result_df.columns = ['partition_date','order_date','sku','sku_name','qty_sold']

logger.info(result_df.head(3))
logger.info(result_df.info())
logger.info(f"Count of NULL RECORDS: {len(result_df.loc[result_df['order_date'].isna()])}")
# Format datatypes & new columns
result_df['order_date'] = pd.to_datetime(result_df['order_date']).dt.strftime('%Y-%m-%d')
result_df['qty_sold'] = result_df['qty_sold'].astype(int)
result_df['week'] = pd.to_datetime(result_df['order_date']).dt.strftime('%Y-%W')

logger.info(f"MIN DATE: {result_df['order_date'].min()}")
logger.info(f"MAX DATE: {result_df['order_date'].max()}")


# Create dataframe of skus sold in the time range
skus_sold_df = result_df.loc[~result_df['sku_name'].isna(),['sku','sku_name']].drop_duplicates()




# Construct query to pull latest (as of yesterday) inventory details 
# ----


yesterday_date = pd.to_datetime(pd.to_datetime('today') - timedelta(1)).strftime('%Y-%m-%d')

QUERY =f"""with inventory AS (

        SELECT partition_date
        , CAST(sku AS VARCHAR) as sku
        , total_fulfillable_quantity
        FROM shipbob_inventory 
        WHERE partition_date = '{yesterday_date}'
        )


        SELECT partition_date
        , CASE WHEN sku IS NULL THEN 'Not Reported'
            ELSE sku end as sku
        , SUM(total_fulfillable_quantity)
        FROM inventory 
        GROUP BY partition_date
        , CASE WHEN sku IS NULL THEN 'Not Reported'
            ELSE sku end 

                    """

# Query datalake to get current inventory details for skus sold in the last 120 days
# ----

inventory_df = run_athena_query(query=QUERY, database='prymal', region=REGION)
inventory_df.columns = ['partition_date','sku','inventory_on_hand']

logger.info(inventory_df.head(3))
logger.info(inventory_df.info())

# Format datatypes & new columns
inventory_df['inventory_on_hand'] = inventory_df['inventory_on_hand'].astype(int)



# List of products to forecast stockout dates for 
PRODUCT_LIST = ['Salted Caramel - Large Bag (320 g)',
                'Cacao Mocha - Large Bag (320 g)',
                'Original - Large Bag (320 g)',
                'Vanilla Bean - Large Bag (320 g)',
                'Butter Pecan - Large Bag (320 g)',
                'Cinnamon Dolce - Large Bag (320 g)']


# Blank df to store results
product_run_rate_df = pd.DataFrame()

# For each sku in products list, generate forecast using recent sales data
for p in PRODUCT_LIST:

    # Generate daily run rates for the product
    df = generate_daily_run_rate(p)

    # Append to run rate dataframe
    product_run_rate_df = pd.concat([product_run_rate_df,df])

# Reset index
product_run_rate_df.reset_index(inplace=True,drop=True)

# Merge run rate df with yesterday's partition of inventory df 
inventory_details_df = product_run_rate_df.merge(inventory_df,
                how='left',
                on=['partition_date', 'sku'])

# Calculate days of stock on hand given the inventory on hand and daily run rate
inventory_details_df['days_of_stock_onhand'] = round(inventory_details_df['inventory_on_hand'] / inventory_details_df['upper_bound'],0).astype(int)




# Create s3 client
s3_client = boto3.client('s3', 
                          region_name = REGION,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                          )

# Set bucket
BUCKET = os.environ['S3_PRYMAL_ANALYTICS']

# WRITE TO S3 =======================================


# Log number of rows
logger.info(f'{len(inventory_details_df)} rows in inventory_details_df')
 
# Configure S3 Prefix
S3_PREFIX_PATH = f"shopify/inventory_report/partition_date={yesterday_date}/shopify_inventory_report_{yesterday_date}.csv"

# Check if data already exists for this partition
data_already_exists = check_path_for_objects(bucket=BUCKET, s3_prefix=S3_PREFIX_PATH)

# If data already exists, delete it .. 
if data_already_exists == True:
   
   # Delete data 
   delete_s3_prefix_data(bucket=BUCKET, s3_prefix=S3_PREFIX_PATH)


logger.info(f'Writing to {S3_PREFIX_PATH}')


with io.StringIO() as csv_buffer:
    inventory_details_df.to_csv(csv_buffer, index=False)

    response = s3_client.put_object(
        Bucket=BUCKET, 
        Key=S3_PREFIX_PATH, 
        Body=csv_buffer.getvalue()
    )

    status = response['ResponseMetadata']['HTTPStatusCode']

    if status == 200:
        logger.info(f"Successful S3 put_object response for PUT ({S3_PREFIX_PATH}). Status - {status}")
    else:
        logger.error(f"Unsuccessful S3 put_object response for PUT ({S3_PREFIX_PATH}. Status - {status}")




