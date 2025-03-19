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
Accelerometer_trusted_node1742250711393 = glueContext.create_dynamic_frame.from_catalog(database="andistedi_project", table_name="accelerometer_trusted2", transformation_ctx="Accelerometer_trusted_node1742250711393")

# Script generated for node Customer_trusted
Customer_trusted_node1742251698788 = glueContext.create_dynamic_frame.from_catalog(database="andistedi_project", table_name="customer_trusted", transformation_ctx="Customer_trusted_node1742251698788")

# Script generated for node Select Field
SqlQuery7438 = '''
SELECT DISTINCT c.customername,
       c.email,
       c.phone,
       c.birthday,
       c.serialnumber,
       c.registrationdate,
       c.lastupdatedate,
       c.sharewithresearchasofdate,
       c.sharewithpublicasofdate,
       c.sharewithfriendsasofdate
FROM  c
INNER JOIN a
    ON c.email = a.user
WHERE c.sharewithresearchasofdate != 0;

'''
SelectField_node1742249787971 = sparkSqlQuery(glueContext, query = SqlQuery7438, mapping = {"a":Accelerometer_trusted_node1742250711393, "c":Customer_trusted_node1742251698788}, transformation_ctx = "SelectField_node1742249787971")

# Script generated for node Amazon S3
AmazonS3_node1742252032606 = glueContext.getSink(path="s3://andistedi-project/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1742252032606")
AmazonS3_node1742252032606.setCatalogInfo(catalogDatabase="andistedi_project",catalogTableName="customer_curated")
AmazonS3_node1742252032606.setFormat("json")
AmazonS3_node1742252032606.writeFrame(SelectField_node1742249787971)
job.commit()