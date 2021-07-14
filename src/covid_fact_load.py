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
## @args: [database = "covid_cleaned", table_name = "covid_fact_loadready_s3", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "covid_cleaned", table_name = "covid_fact_loadready_s3", transformation_ctx = "DataSource0")
## @type: ApplyMapping
## @args: [mappings = [("covid_trans_id", "int", "covid_trans_id", "int"), ("state_id", "int", "state_id", "int"), ("date_id", "int", "date_id", "int"), ("total_conf_cases", "int", "total_conf_cases", "int"), ("total_death_cnt", "int", "total_death_cnt", "int"), ("total_cured_cnt", "int", "total_cured_cnt", "int"), ("temp", "int", "temp", "int")], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = DataSource0]
Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = [("covid_trans_id", "int", "covid_trans_id", "int"), ("state_id", "int", "state_id", "int"), ("date_id", "int", "date_id", "int"), ("total_conf_cases", "int", "total_conf_cases", "int"), ("total_death_cnt", "int", "total_death_cnt", "int"), ("total_cured_cnt", "int", "total_cured_cnt", "int"), ("temp", "int", "temp", "int")], transformation_ctx = "Transform0")
## @type: DataSink
## @args: [database = "covid-datamart", redshift_tmp_dir = "s3://upgrad-glue-dir/temp/", table_name = "dmdev_public_covid_cases_fact", transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform0]
DataSink0 = glueContext.write_dynamic_frame.from_catalog(frame = Transform0, database = "covid-datamart", redshift_tmp_dir = "s3://upgrad-glue-dir/temp/", table_name = "dmdev_public_covid_cases_fact", transformation_ctx = "DataSink0")
job.commit()