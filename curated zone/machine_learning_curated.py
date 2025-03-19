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

# Script generated for node Accelerometer_trusted
Accelerometer_trusted_node1742253310488 = glueContext.create_dynamic_frame.from_catalog(database="andistedi_project", table_name="accelerometer_trusted2", transformation_ctx="Accelerometer_trusted_node1742253310488")

# Script generated for node Step_Trainer
Step_Trainer_node1742253243880 = glueContext.create_dynamic_frame.from_catalog(database="andistedi_project", table_name="step_trainer_trusted", transformation_ctx="Step_Trainer_node1742253243880")

# Script generated for node SQL Query
SqlQuery7489 = '''
select
st.sensorreadingtime,
st.serialnumber,
st.distancefromobject,
at.x,
at.y,
at.z
from at
Join st on (at.timestamp = st.sensorreadingtime);
'''
SQLQuery_node1742253527928 = sparkSqlQuery(glueContext, query = SqlQuery7489, mapping = {"at":Accelerometer_trusted_node1742253310488, "st":Step_Trainer_node1742253243880}, transformation_ctx = "SQLQuery_node1742253527928")

# Script generated for node Amazon S3
AmazonS3_node1742254598771 = glueContext.getSink(path="s3://andistedi-project/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1742254598771")
AmazonS3_node1742254598771.setCatalogInfo(catalogDatabase="andistedi_project",catalogTableName="machine_learning_curated")
AmazonS3_node1742254598771.setFormat("json")
AmazonS3_node1742254598771.writeFrame(SQLQuery_node1742253527928)
job.commit()