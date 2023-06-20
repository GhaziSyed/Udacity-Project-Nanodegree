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

# Script generated for node CustomerCurated
CustomerCurated_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://albis-lake-house/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1",
)

# Script generated for node Step_trainer_landing
Step_trainer_landing_node1687118827753 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://albis-lake-house/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Step_trainer_landing_node1687118827753",
)

# Script generated for node Join
Join_node1687118890391 = Join.apply(
    frame1=CustomerCurated_node1,
    frame2=Step_trainer_landing_node1687118827753,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1687118890391",
)

# Script generated for node Drop Fields
DropFields_node1687118898854 = DropFields.apply(
    frame=Join_node1687118890391,
    paths=[
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1687118898854",
)

# Script generated for node steptrainer_trusted
steptrainer_trusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1687118898854,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://albis-lake-house/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="steptrainer_trusted_node3",
)

job.commit()
