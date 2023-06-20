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

# Script generated for node AccelerometerTrusted
AccelerometerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://albis-lake-house/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1",
)

# Script generated for node StepTrainedTrusted
StepTrainedTrusted_node1687263857073 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://albis-lake-house/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainedTrusted_node1687263857073",
)

# Script generated for node Join
Join_node1687263896777 = Join.apply(
    frame1=AccelerometerTrusted_node1,
    frame2=StepTrainedTrusted_node1687263857073,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="Join_node1687263896777",
)

# Script generated for node machineLearningmodel
machineLearningmodel_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1687263896777,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://albis-lake-house/step_trainer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="machineLearningmodel_node3",
)

job.commit()
