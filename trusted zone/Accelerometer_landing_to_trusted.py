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

# Script generated for node Accelerometer_Landing
Accelerometer_Landing_node1742250993304 = glueContext.create_dynamic_frame.from_catalog(database="andistedi_project", table_name="acceleromenter_landing", transformation_ctx="Accelerometer_Landing_node1742250993304")

# Script generated for node Customer_trusted
Customer_trusted_node1742251090667 = glueContext.create_dynamic_frame.from_catalog(database="andistedi_project", table_name="customer_trusted", transformation_ctx="Customer_trusted_node1742251090667")

# Script generated for node Join
Join_node1742251160443 = Join.apply(frame1=Accelerometer_Landing_node1742250993304, frame2=Customer_trusted_node1742251090667, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1742251160443")

# Script generated for node SQL Query For Drop Field
SqlQuery7077 = '''
select user, timestamp, x, y, z from myDataSource

'''
SQLQueryForDropField_node1742251183962 = sparkSqlQuery(glueContext, query = SqlQuery7077, mapping = {"myDataSource":Join_node1742251160443}, transformation_ctx = "SQLQueryForDropField_node1742251183962")

# Script generated for node Amazon S3
AmazonS3_node1742251342305 = glueContext.getSink(path="s3://andistedi-project/accelerometer/trusted2/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1742251342305")
AmazonS3_node1742251342305.setCatalogInfo(catalogDatabase="andistedi_project",catalogTableName="accelerometer_trusted2")
AmazonS3_node1742251342305.setFormat("json")
AmazonS3_node1742251342305.writeFrame(SQLQueryForDropField_node1742251183962)
job.commit()