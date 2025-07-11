# Databricks notebook source
# DBTITLE 1,Widget Creation

dbutils.widgets.text("source_catalog_name", "gocart","Catalog Name")
dbutils.widgets.text("source_schema_names", "campaigns", "Schema Names")
dbutils.widgets.text("source_table_names", "ct_events_inc", "Table Name")

source_catalog_name = dbutils.widgets.get("source_catalog_name")
source_schema_names = dbutils.widgets.get("source_schema_names")
source_table_names = dbutils.widgets.get("source_table_names")

print(f"Source Catalog Name: {source_catalog_name}")
print(f"Source Schema Names: {source_schema_names}")

# COMMAND ----------

# DBTITLE 1,Has Directory Function
from typing import Generator

def deep_ls(path: str) -> Generator:
    """List all files in base path recursively."""
    for x in dbutils.fs.ls(path):
        if not x.path.endswith('/'):
            yield x
        else:
            yield from deep_ls(x.path)

def has_subdirectory(path: str):
    items = dbutils.fs.ls(path)
    for item in items:
        if item.isDir:  # Check if the item is a directory
            return dbutils.fs.ls(item.path)
    return None

# COMMAND ----------

# DBTITLE 1,Wrapper
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.functions import lit
from pyspark.sql import Row

def process_sub_directory(row_path, uri_original_value):
    # Use the deep_ls function to list all files in the base path recursively
    sub_directory = deep_ls(row_path)
    
    # Create a DataFrame from the generator
    sub_directory_df = spark.createDataFrame(
        (Row(path=file.path, name=file.name, size=file.size) for file in sub_directory)
    )
    
    # Add the 'uri_original' column with a constant value
    sub_directory_df = sub_directory_df.withColumn('uri_original', lit(uri_original_value))
    
    return sub_directory_df

# Function to process each sub-directory, modified to accept a single argument
# for compatibility with ThreadPoolExecutor
def process_sub_directory_wrapper(row_path_uri_original):
    row_path, uri_original_value = row_path_uri_original
    return process_sub_directory(row_path, uri_original_value)

# COMMAND ----------

# DBTITLE 1,Main Program
from pyspark.sql.functions import concat, lit, expr, col, trim, explode, struct, array
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import Row

# Define the schema for the UDF
schema = StructType([
    StructField("uri", StringType(), True),
    StructField("name", StringType(), True),
    StructField("delta_name", StringType(), True),
    StructField("path", StringType(), True),
    StructField("size", LongType(), True),
    StructField("has_subdirectory", BooleanType(), True)
])

# Function to check if a path has a subdirectory
def has_subdirectory(path):
    try:
        return any(dbutils.fs.ls(path))
    except:
        return False

# Function to check if a path exists
def path_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except:
        return False

# Function to process each location
def process_location(loc, name):
    if not path_exists(loc):
        return []
    details_path = dbutils.fs.ls(loc)
    result = []
    for detail in details_path:
        uri = loc
        delta_name = detail.name
        path = detail.path
        size = detail.size
        subdirectory = has_subdirectory(detail.path)
        result.append((uri, name, delta_name, path, size, subdirectory))
    return result

def fetch_table_stats(table_df):
    try:
        table_stats_row = table_df.first()      
        # Extracting values from the Row object
        name = table_stats_row['name']
        format = table_stats_row['format']
        numFiles = table_stats_row['numFiles']
        sizeInBytes = table_stats_row['sizeInBytes']
        location = table_stats_row['location']
        # Replace the location string as needed
        mounted_location = location.replace(
            'abfss://metastore@daviunitycatalog.dfs.core.windows.net/davi_metastore/1f953362-d820-4ab6-b983-012fa267fad5', 
            '/mnt/daviunitycatalog/metastore/davi_metastore/1f953362-d820-4ab6-b983-012fa267fad5/'
        )
        # Return a new Row object with the processed information
        return Row(
            name=name, 
            format=format, 
            numFiles=numFiles, 
            sizeInBytes=sizeInBytes, 
            location=location, 
            mounted_location=mounted_location
        )
    except Exception as e:
        print(f"Error processing table: {e}")
        return None

# Main code

tables_df = spark.sql("SELECT * FROM system.information_schema.tables") \
    .select("table_catalog", "table_schema", "table_name", "table_type", "table_owner", "last_altered_by") \
    .orderBy(["table_catalog", "table_schema", "table_name"]) \
    .filter("table_type NOT IN ('VIEW', 'EXTERNAL', 'MATERIALIZED_VIEW', 'FOREIGN')") \
    .filter("table_catalog NOT IN ('system', '__databricks_internal')") \
    .filter("table_schema NOT IN ('information_schema', 'system')") \
    .withColumn('uri_original', expr("concat(table_catalog, '.', table_schema, '.', table_name)")) \
    .withColumn('uri', expr("concat('`', table_catalog, '`.`', table_schema, '`.`', table_name, '`')"))

schema_names_list = source_schema_names.split(',')
table_names_list = source_table_names.split(',')
tables_filter_df = tables_df.filter(col("table_catalog") == source_catalog_name) \
                            .filter(col("table_schema").isin(schema_names_list)) \
                            .filter(col("table_name").isin(table_names_list)) \
                            .select("table_catalog", "table_schema", "table_name", "table_type", "table_owner", "last_altered_by", "uri", "uri_original")

uri_list_df = tables_filter_df.select("uri").orderBy("uri").distinct()
describe_table_path_size = []

with ThreadPoolExecutor(max_workers=100) as executor:
    futures = []
    table_stats_list = []
    for row in uri_list_df.collect():
        df = spark.sql(f"DESCRIBE DETAIL {row['uri']}")
        table_stats = fetch_table_stats(df)
        if table_stats:
            table_stats_list.append(table_stats.asDict())
            futures.append(executor.submit(process_location, table_stats.mounted_location, table_stats.name))
    for future in as_completed(futures):
        describe_table_path_size.extend(
            {'uri': item[0], 'name': item[1], 'delta_name': item[2], 'path': item[3], 'size': item[4], 'has_subdirectory': item[5]}
            for item in future.result()
        )

# Convert describe_table_path_size to a DataFrame
if describe_table_path_size:
    describe_table_path_size_df = spark.createDataFrame(describe_table_path_size, schema)
    #display(describe_table_path_size_df)

# Process each location and create a DataFrame
processed_locations = [
    tuple(item) for sublist in [
        process_location(row['path'], row['name']) for row in describe_table_path_size
    ] for item in sublist if isinstance(item, tuple)
]
processed_locations_df = spark.createDataFrame(processed_locations, schema)
  
# Perform a right join
combined_result = tables_filter_df.drop('uri').join(
    processed_locations_df,
    trim(tables_filter_df.uri_original) == trim(processed_locations_df.name),"right"
)

final_result = combined_result.withColumnRenamed("path", "file_path").withColumnRenamed("size", "file_size").drop("has_subdirectory")
for_write_result = final_result.select("table_catalog","table_schema","table_name","table_type","table_owner","last_altered_by","delta_name","file_path","file_size","uri","uri_original"
)
for_write_result.write.mode("append").saveAsTable("davi_analytics_wh_dev.dataset_uage.table_usage_history_v2")
