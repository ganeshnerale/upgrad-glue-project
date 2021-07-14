import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [format_options = {"quoteChar":"","escaper":"","withHeader":False,"separator":","}, connection_type = "s3", format = "csv", connection_options = {"paths": ["s3://covid-usecase-raw/raw-data/date_dim_raw/date_dim.csv"], "recurse":True}, transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_options(format_options = {"quoteChar":"","escaper":"","withHeader":False,"separator":","}, connection_type = "s3", format = "csv", connection_options = {"paths": ["s3://covid-usecase-raw/raw-data/date_dim_raw/date_dim.csv"], "recurse":True}, transformation_ctx = "DataSource0")
## @type: ApplyMapping
## @args: [mappings = [("date_id", "long", "date_id", "int"), ("full_date", "string", "full_date", "string"), ("year", "long", "year", "int"), ("month", "string", "month", "string"), ("day", "string", "day", "string"), ("weekend", "boolean", "weekend", "boolean"), ("holiday", "boolean", "holiday", "boolean")], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = DataSource0]
Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = [("date_id", "long", "date_id", "int"), ("full_date", "string", "full_date", "string"), ("year", "long", "year", "int"), ("month", "string", "month", "string"), ("day", "string", "day", "string"), ("weekend", "boolean", "weekend", "boolean"), ("holiday", "boolean", "holiday", "boolean")], transformation_ctx = "Transform0")
## @type: DataSink
## @args: [connection_type = "s3", format = "csv", connection_options = {"path": "s3://covid-usecase-raw/load-ready/date_dim/", "partitionKeys": []}, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform0]
DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform0, connection_type = "s3", format = "csv", connection_options = {"path": "s3://covid-usecase-raw/load-ready/date_dim/", "partitionKeys": []}, transformation_ctx = "DataSink0",format_options = {"writeHeader":False})
job.commit()