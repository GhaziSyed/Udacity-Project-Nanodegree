import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer_Landing
Accelerometer_Landing_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://albis-lake-house/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometer_Landing_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1687115942255 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://albis-lake-house/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1687115942255",
)

# Script generated for node Join Customer
JoinCustomer_node1687115859005 = Join.apply(
    frame1=Accelerometer_Landing_node1,
    frame2=AmazonS3_node1687115942255,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomer_node1687115859005",
)

# Script generated for node Drop Fields
DropFields_node1687116038098 = DropFields.apply(
    frame=JoinCustomer_node1687115859005,
    paths=[
        "serialNumber",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
        "shareWithPublicAsOfDate",
    ],
    transformation_ctx="DropFields_node1687116038098",
)

# Script generated for node Accelerometer_Trusted
Accelerometer_Trusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1687116038098,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://albis-lake-house/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="Accelerometer_Trusted_node3",
)

job.commit()
