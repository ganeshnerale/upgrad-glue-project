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
## @args: [database = "covid-raw", table_name = "pg_postgres_covid_raw_covid_raw_data", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "covid-raw", table_name = "pg_postgres_covid_raw_covid_raw_data", transformation_ctx = "DataSource0")
## @type: ApplyMapping
## @args: [mappings = [("currentstatus", "string", "currentstatus", "string"), ("entry_id", "int", "entry_id", "int"), ("agebracket", "string", "agebracket", "string"), ("gender", "string", "gender", "string"), ("detecteddistrict", "string", "detecteddistrict", "string"), ("contractedfromwhichpatient", "string", "contractedfromwhichpatient", "string"), ("nationality", "string", "nationality", "string"), ("numcases", "string", "numcases", "string"), ("detectedstate", "string", "detectedstate", "string"), ("statuschangedate", "string", "statuschangedate", "string"), ("typeoftransmission", "string", "typeoftransmission", "string"), ("statepatientnumber", "string", "statepatientnumber", "string"), ("dateannounced", "date", "dateannounced", "date"), ("statecode", "string", "statecode", "string"), ("source_1", "string", "source_1", "string"), ("patientnumber", "string", "patientnumber", "string"), ("detectedcity", "string", "detectedcity", "string"), ("notes", "string", "notes", "string"), ("source_3", "string", "source_3", "string"), ("source_2", "string", "source_2", "string")], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = DataSource0]
Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = [("currentstatus", "string", "currentstatus", "string"), ("entry_id", "int", "entry_id", "int"), ("agebracket", "string", "agebracket", "string"), ("gender", "string", "gender", "string"), ("detecteddistrict", "string", "detecteddistrict", "string"), ("contractedfromwhichpatient", "string", "contractedfromwhichpatient", "string"), ("nationality", "string", "nationality", "string"), ("numcases", "string", "numcases", "string"), ("detectedstate", "string", "detectedstate", "string"), ("statuschangedate", "string", "statuschangedate", "string"), ("typeoftransmission", "string", "typeoftransmission", "string"), ("statepatientnumber", "string", "statepatientnumber", "string"), ("dateannounced", "date", "dateannounced", "date"), ("statecode", "string", "statecode", "string"), ("source_1", "string", "source_1", "string"), ("patientnumber", "string", "patientnumber", "string"), ("detectedcity", "string", "detectedcity", "string"), ("notes", "string", "notes", "string"), ("source_3", "string", "source_3", "string"), ("source_2", "string", "source_2", "string")], transformation_ctx = "Transform0")
## @type: DataSink
## @args: [connection_type = "s3", format = "parquet", connection_options = {"path": "s3://covid-usecase-raw/raw-data/covid-raw-district-level/", "compression": "gzip", "partitionKeys": ["dateannounced"]}, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform0]
DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform0, connection_type = "s3", format = "parquet", connection_options = {"path": "s3://covid-usecase-raw/raw-data/covid-raw-district-level/", "compression": "gzip", "partitionKeys": ["dateannounced"]}, transformation_ctx = "DataSink0")
job.commit()