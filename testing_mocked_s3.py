import os
import signal
import subprocess
import boto3
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


# start moto server, by default it runs on localhost on port 5000.
process = subprocess.Popen(
    "moto_server s3", stdout=subprocess.PIPE,
    shell=True, preexec_fn=os.setsid
)
# create an s3 connection that points to the moto server.
s3_conn = boto3.resource(
    "s3", endpoint_url="http://127.0.0.1:5000"
)
# create an S3 bucket.
s3_conn.create_bucket(Bucket="bucket")
# configure pyspark to use hadoop-aws module.
# notice that we reference the hadoop version we installed.
os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'
# get the spark session object and hadoop configuration.
spark = SparkSession.builder.getOrCreate()
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
# mock the aws credentials to access s3.
hadoop_conf.set("fs.s3a.access.key", "dummy-value")
hadoop_conf.set("fs.s3a.secret.key", "dummy-value")
# we point s3a to our moto server.
hadoop_conf.set("fs.s3a.endpoint", "http://127.0.0.1:5000")
# we need to configure hadoop to use s3a.
hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# create a pyspark dataframe.
values = [("k1", 1), ("k2", 2)]
columns = ["key", "value"]
df = spark.createDataFrame(values, columns)
# write the dataframe as csv to s3.
df.write.csv("s3://bucket/source.csv")
# read the dataset from s3 and assert
df = spark.read.csv("s3://bucket/source.csv")
assert isinstance(df, DataFrame)
# shut down the moto server.
os.killpg(os.getpgid(process.pid), signal.SIGTERM)
print("yeeey, the test ran without errors.")