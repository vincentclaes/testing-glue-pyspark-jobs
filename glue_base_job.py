from abc import abstractmethod
import logging
logging.basicConfig(level=logging.INFO)


class GlueBaseJobForSpark(object):
    """ Baseclass for Glue jobs that run with Spark"""

    def __init__(self, cli_args=None, spark=None, *args, **kwargs):
        self.cli_args = cli_args
        self.spark = spark

    @staticmethod
    @abstractmethod
    def handler(cli_args, spark):
        """
        abstract function that needs to be implemented by the glue child class.
        the logic of the script should go into the handler.
        :param cli_args: arguments that are provided via the cli.
                glue passes a list of args. tests pass a dictionairy.
        :param spark: spark session object that is used in the job.
        """

    def run(self, *args, **kwargs):
        """run the code of the handler inside the context of the job"""
        return self._run(handler=self.handler, cli_args=self.cli_args, spark=self.spark, *args, **kwargs)

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
        logger.debug('get glue context')
        sc = SparkContext.getOrCreate()
        from awsglue.context import GlueContext
        glue_context = GlueContext(sparkContext=sc)
        return glue_context

    @staticmethod
    def _get_glue_job(glue_context):
        logger.debug('get glue job')
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

    def _run(self, handler, cli_args, spark=None):
        """when we start the job we initialize the spark environment
        and trigger the handler function of the glue script."""

        glue_args = self._get_glue_args(cli_args=cli_args)
        glue_context = self._get_glue_context()
        job = self._get_glue_job(glue_context=glue_context)
        self._init_job(job=job, glue_args=glue_args)
        logging.info('run {} handler script with arguments {}'.format(self.__class__.__name__, cli_args))
        # run the implementation of the abstract method
        handler(cli_args=glue_args, spark=glue_context.spark_session)
        self._commit_job(job)
