import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Step_trainer_landing
Step_trainer_landing_node1742147533195 = glueContext.create_dynamic_frame.from_catalog(database="andistedi_project", table_name="step_trainer_landing", transformation_ctx="Step_trainer_landing_node1742147533195")

# Script generated for node Customer_curated
Customer_curated_node1742147528906 = glueContext.create_dynamic_frame.from_catalog(database="andistedi_project", table_name="customer_curated", transformation_ctx="Customer_curated_node1742147528906")

# Script generated for node Join And Select Fields
SqlQuery6995 = '''
select st.sensorreadingtime, st.serialnumber, st.distancefromobject from st 
Join cc on st.serialnumber = cc.serialnumber

'''
JoinAndSelectFields_node1742252562647 = sparkSqlQuery(glueContext, query = SqlQuery6995, mapping = {"st":Step_trainer_landing_node1742147533195, "cc":Customer_curated_node1742147528906}, transformation_ctx = "JoinAndSelectFields_node1742252562647")

# Script generated for node Amazon S3
AmazonS3_node1742150635723 = glueContext.getSink(path="s3://andistedi-project/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1742150635723")
AmazonS3_node1742150635723.setCatalogInfo(catalogDatabase="andistedi_project",catalogTableName="step_trainer_trusted")
AmazonS3_node1742150635723.setFormat("json")
AmazonS3_node1742150635723.writeFrame(JoinAndSelectFields_node1742252562647)
job.commit()