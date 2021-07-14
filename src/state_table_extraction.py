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
## @args: [database = "covid-raw", table_name = "pg_postgres_covid_raw_state_tracker", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "covid-raw", table_name = "pg_postgres_covid_raw_state_tracker", transformation_ctx = "DataSource0")
## @type: ApplyMapping
## @args: [mappings = [("state_name", "string", "state_name", "string"), ("state_code", "string", "state_code", "string"),("capital", "string", "capital", "string"), ("latitude", "string", "latitude", "string"),  ("longitude", "string", "longitude", "string"), ("population", "string", "population", "string")], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = DataSource0]
Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = [("state_name", "string", "state_name", "string"), ("state_code", "string", "state_code", "string"), ("capital", "string", "capital", "string"),  ("latitude", "string", "latitude", "string"), ("longitude", "string", "longitude", "string"), ("population", "string", "population", "string")], transformation_ctx = "Transform0")
## @type: DataSink
## @args: [connection_type = "s3", format = "csv", connection_options = {"path": "s3://covid-usecase-raw/raw-data/covid-raw-state-tracker/", "compression": "gzip", "partitionKeys": []}, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform0]

Transform0=Transform0.coalesce(1)
DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform0, connection_type = "s3", format = "csv", connection_options = {"path": "s3://covid-usecase-raw/raw-data/covid-raw-state-tracker/", "compression": "gzip", "partitionKeys": []}, transformation_ctx = "DataSink0",format_options = {"writeHeader":False})
job.commit()