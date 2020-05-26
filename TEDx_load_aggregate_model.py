
####### TEDx-Load-Aggregate-Model

import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

###### FROM FILES
tedx_dataset_path = "s3://unibg-mytedx-data-xxx/tedx_dataset.csv" 

####### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

###### START JOB CONTEXT AND JOB
sc = SparkContext()

glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

##### READ INPUT FILES TO CREATE AN INPUT DATASET
tedx_dataset = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(tedx_dataset_path)
    
tedx_dataset.printSchema()

##### FILTER ITEMS WITH NULL POSTING KEY
count_items = tedx_dataset.count()
count_items_null = tedx_dataset.filter("idx is not null").count()

print(f"Number of items from RAW DATA {count_items}")
print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")

### READ TAGS DATASET
tags_dataset_path = "s3://unibg-mytedx-data-xxx/tags_dataset.csv"
tags_dataset = spark.read.option("header","true").csv(tags_dataset_path)

tags_dataset_agg = tags_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("tag").alias("tags")) 
tags_dataset_agg.printSchema()

## READ WATCH NEXT DATASET
watch_next_dataset_path = "s3://unibg-mytedx-data-xxx/watch_next_dataset.csv"
watch_next_dataset = spark.read.option("header","true").csv(watch_next_dataset_path).dropDuplicates()

watch_next_dataset_agg =    watch_next_dataset.groupBy(col("idx").alias("idx_ref2")) \
    .agg(collect_list("watch_next_idx") \
    .alias("watch_next_idxs")).drop("url")

watch_next_dataset_agg.printSchema()

# CREATE THE AGGREGATE MODEL, ADD TAGS TO TEDX_DATASET
tedx_dataset_agg = tedx_dataset.join(tags_dataset_agg, tedx_dataset.idx == tags_dataset_agg.idx_ref, "left") \
    .drop("idx_ref") \
    .select(col("idx").alias("_id"), col("*")) \
    .drop("idx") \

tedx_dataset_agg.printSchema()

# CREATE THE AGGREGATE MODEL, ADD WATCH NEXT TO TEDX_DATASET
tedx_dataset_agg = tedx_dataset_agg.join(watch_next_dataset_agg, tedx_dataset_agg._id == watch_next_dataset_agg.idx_ref2, "left") \
    .drop("idx_ref2")

tedx_dataset_agg.printSchema() 


mongo_uri = "mongodb://xxx"

write_mongo_options = {
    "uri": mongo_uri,
    "database": "unibg_tedx",
    "collection": "tedz_data",
    "username": "xxx",
    "password": "xxx",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
