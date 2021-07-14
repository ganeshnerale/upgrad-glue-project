import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

SqlQuery0 = '''
SELECT dateannounced, statecode, detecteddistrict, 
SUM(CASE WHEN a.currentstatus = 'Hospitalized' THEN cast(a.numcases as integer) ELSE 0 END) AS total_conf_cases,
SUM(CASE WHEN a.currentstatus = 'Deceased' THEN cast(a.numcases as integer) ELSE 0 END) AS total_death_cnt,
SUM(CASE WHEN a.currentstatus = 'Recovered' THEN cast(a.numcases as integer) ELSE 0 END) AS total_cured_cnt 
FROM myDataSource  a
group by dateannounced, statecode, detecteddistrict

'''
SqlQuery1 = '''
select b.date_id,concat('IN.',a.statecode) as statecode,a.detecteddistrict,a.total_conf_cases,a.total_death_cnt,a.total_cured_cnt,a.temp
from myDataSource a left outer join date_dim b
on a.dateannounced = b.full_date_time

'''
SqlQuery2 = '''
select a.date_id,b.state_id,a.detecteddistrict,a.total_conf_cases,a.total_death_cnt,a.total_cured_cnt,a.temp
from myDataSource a left outer join state_dim b
on a.statecode = b.state_code

'''

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "covid-datamart", table_name = "dmdev_public_states_dim", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "covid-datamart", table_name = "dmdev_public_states_dim", transformation_ctx = "DataSource0")
## @type: DataSource
## @args: [database = "covid-datamart", table_name = "dmdev_public_date_dim", transformation_ctx = "DataSource2"]
## @return: DataSource2
## @inputs: []
DataSource2 = glueContext.create_dynamic_frame.from_catalog(database = "covid-datamart", table_name = "dmdev_public_date_dim", transformation_ctx = "DataSource2")
## @type: DataSource
## @args: [database = "covid_raw_db", table_name = "pg_covid_raw_district_level", transformation_ctx = "DataSource1"]
## @return: DataSource1
## @inputs: []
DataSource1 = glueContext.create_dynamic_frame.from_catalog(database = "covid_raw_db", table_name = "pg_covid_raw_district_level", transformation_ctx = "DataSource1")
## @type: SqlCode
## @args: [sqlAliases = {"myDataSource": DataSource1}, sqlName = SqlQuery0, transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [dfc = DataSource1]
Transform0 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource": DataSource1}, transformation_ctx = "Transform0")
## @type: SqlCode
## @args: [sqlAliases = {"date_dim": DataSource2, "myDataSource": Transform0}, sqlName = SqlQuery1, transformation_ctx = "Transform1"]
## @return: Transform1
## @inputs: [dfc = Transform0,DataSource2]
Transform1 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"date_dim": DataSource2, "myDataSource": Transform0}, transformation_ctx = "Transform1")
## @type: SqlCode
## @args: [sqlAliases = {"myDataSource": Transform1, "state_dim": DataSource0}, sqlName = SqlQuery2, transformation_ctx = "Transform2"]
## @return: Transform2
## @inputs: [dfc = Transform1,DataSource0]
Transform2 = sparkSqlQuery(glueContext, query = SqlQuery2, mapping = {"myDataSource": Transform1, "state_dim": DataSource0}, transformation_ctx = "Transform2")
## @type: DataSink
## @args: [database = "covid_cleaned", additionalOptions = {"aws_iam_role":"arn:aws:iam::999305447493:role/AWSGlueServiceDefault"}, redshift_tmp_dir = "s3://upgrad-glue-dir/temp/", table_name = "covid_fact_loadready_s3", transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform2]
DataSink0 = glueContext.write_dynamic_frame.from_catalog(frame = Transform2, database = "covid_cleaned", redshift_tmp_dir = "s3://upgrad-glue-dir/temp/", table_name = "covid_fact_loadready_s3", transformation_ctx = "DataSink0", additional_options = {"aws_iam_role":"arn:aws:iam::999305447493:role/AWSGlueServiceDefault"})
job.commit()