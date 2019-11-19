import logging
from pyspark.context import SparkContext

logging.basicConfig(level=logging.INFO)


class GlueJob(object):
    def __init__(self, cli_args, spark=None):
        self.cli_args = cli_args
        self.spark = spark

    def handler(self, cli_args, spark):
        source = cli_args["source"]
        destination = cli_args["destination"]
        df = spark.read.csv(source)
        df.write.csv(destination)
        # df = self.read_source(source, spark)

    def run(self):
        """when we start the job we initialize the spark environment
        and trigger the handler function of the glue script."""

        glue_args = self._get_glue_args(cli_args=self.cli_args)
        spark_session, job = self._get_spark_session_and_job(glue_args)
        logging.info('run {} handler script with arguments {}'.format(self.__class__.__name__, glue_args))

        self.handler(cli_args=glue_args, spark=spark_session)

        self._commit_job(job)

    def _get_spark_session_and_job(self, glue_args):
        glue_context = self._get_glue_context()
        job = self._get_glue_job(glue_context=glue_context)
        self._init_job(job=job, glue_args=glue_args)
        return glue_context.spark_session, job

    @staticmethod
    def read_source(source, spark, spark_format='parquet', options={}):
        """read the source we need for our glue job based on the context."""
        logging.info('reading in format {} from path {}'.format(spark_format, source))
        return spark.read.format(spark_format).options(**options).load(source)

    @staticmethod
    def write_dataframe(df, destination, spark_format='parquet', savemode='overwrite', options={}):
        """write the target df to a destination based on the context."""
        logging.info('writing df {} in format {} with savemode {} to path {}'
                     .format(df, spark_format, savemode, destination))
        df.write.format(spark_format).options(**options).mode(savemode).save(destination)

    @staticmethod
    def _get_glue_context():
        sc = SparkContext.getOrCreate()
        from awsglue.context import GlueContext
        glue_context = GlueContext(sparkContext=sc)
        return glue_context

    @staticmethod
    def _get_glue_job(glue_context):
        from awsglue.job import Job
        return Job(glue_context=glue_context)

    @staticmethod
    def _init_job(job, glue_args):
        job.init(glue_args['JOB_NAME'], glue_args)

    @staticmethod
    def _commit_job(job):
        job.commit()

    @staticmethod
    def _get_glue_args(cli_args):
        from awsglue.utils import getResolvedOptions
        glue_args = getResolvedOptions(args=sys.argv, options=["JOB_NAME"] + cli_args)
        logger.debug('get glue args {}'.format(glue_args))
        return glue_args


if __name__ == '__main__':
    GlueJob(cli_args=["source", "destination"]).run()
