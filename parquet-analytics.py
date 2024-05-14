import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1714342121996 = glueContext.create_dynamic_frame.from_catalog(database="db_youtube_cleaned", table_name="cleaned_statistics_referance_data", transformation_ctx="AWSGlueDataCatalog_node1714342121996")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1714342161182 = glueContext.create_dynamic_frame.from_catalog(database="db_youtube_cleaned", table_name="raw_statistics", transformation_ctx="AWSGlueDataCatalog_node1714342161182")

# Script generated for node Join
Join_node1714342230053 = Join.apply(frame1=AWSGlueDataCatalog_node1714342161182, frame2=AWSGlueDataCatalog_node1714342121996, keys1=["category_id"], keys2=["id"], transformation_ctx="Join_node1714342230053")

# Script generated for node Amazon S3
AmazonS3_node1714342686583 = glueContext.getSink(path="s3://de-on-yt-analytics-useast1-dev-rht", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["region", "category_id"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1714342686583")
AmazonS3_node1714342686583.setCatalogInfo(catalogDatabase="db_youtube_analytics",catalogTableName="final_analytics")
AmazonS3_node1714342686583.setFormat("glueparquet", compression="snappy")
AmazonS3_node1714342686583.writeFrame(Join_node1714342230053)
job.commit()
