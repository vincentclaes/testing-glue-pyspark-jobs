import os
import signal
import subprocess
from io import BytesIO

import boto3
import pandas as pd
from pyspark.sql import SparkSession

# The os.setsid() is passed in the argument preexec_fn so
# it's run after the fork() and before  exec() to run the shell.
pro = subprocess.Popen("moto_server s3", stdout=subprocess.PIPE,
                       shell=True, preexec_fn=os.setsid)

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'
)
spark = SparkSession.builder.getOrCreate()

# Setup spark to use s3, and point it to the moto server.
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.access.key", "mock")
hadoop_conf.set("fs.s3a.secret.key", "mock")
hadoop_conf.set("fs.s3a.endpoint", "http://127.0.0.1:5000")

conn = boto3.resource("s3", endpoint_url="http://127.0.0.1:5000")
conn.create_bucket(Bucket="bucket")

data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
with BytesIO(data.to_csv(index=False).encode()) as buffer:
    conn.Bucket("bucket").put_object(Key="test/test.csv", Body=buffer)

df = spark.read.csv("s3://bucket/test/test.csv", header=True)
print(df)
# DataFrame[a: string, b: string]


os.killpg(os.getpgid(pro.pid), signal.SIGTERM)  # Send the signal to all the process
