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
Accelerometer_Landing_node1687117158907 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://albis-lake-house/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometer_Landing_node1687117158907",
)

# Script generated for node CustomerTrusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://albis-lake-house/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Join
Join_node1687117229822 = Join.apply(
    frame1=CustomerTrusted_node1,
    frame2=Accelerometer_Landing_node1687117158907,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1687117229822",
)

# Script generated for node Drop Fields
DropFields_node1687117748455 = DropFields.apply(
    frame=Join_node1687117229822,
    paths=["z", "y", "x", "timeStamp", "user"],
    transformation_ctx="DropFields_node1687117748455",
)

# Script generated for node CustomerTrusted
CustomerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1687117748455,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://albis-lake-house/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrusted_node3",
)

job.commit()
