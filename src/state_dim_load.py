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
## @args: [database = "covid_cleaned", table_name = "s3_state_dim_lr", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "covid_cleaned", table_name = "s3_state_dim_lr", transformation_ctx = "DataSource0")
## @type: ApplyMapping
## @args: [mappings = [("state_name", "string", "state_name", "string"), ("state_code", "string", "state_code", "string"), ("capital", "string", "capital", "string"), ("latitude", "string", "latitude", "decimal"), ("longitude", "string", "longitude", "decimal"), ("population", "string", "population", "string")], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = DataSource0]
Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = [("state_name", "string", "state_name", "string"), ("state_code", "string", "state_code", "string"), ("capital", "string", "capital", "string"), ("latitude", "string", "latitude", "decimal"), ("longitude", "string", "longitude", "decimal"), ("population", "string", "population", "string")], transformation_ctx = "Transform0")
## @type: DataSink
## @args: [database = "covid-datamart", additionalOptions = {"aws_iam_role":"arn:aws:iam::999305447493:role/AWSGlueServiceadmin"}, redshift_tmp_dir = "s3://upgrad-glue-dir/temp/", table_name = "dmdev_public_states_dim", transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform0]
DataSink0 = glueContext.write_dynamic_frame.from_catalog(frame = Transform0, database = "covid-datamart", redshift_tmp_dir = "s3://upgrad-glue-dir/temp/", table_name = "dmdev_public_states_dim", transformation_ctx = "DataSink0", additional_options = {"aws_iam_role":"arn:aws:iam::999305447493:role/AWSGlueServiceadmin"})
job.commit()