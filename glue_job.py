from pyspark.context import SparkContext


class GlueJob(object):
    def run(self, cli_args, spark):
        # init glue pyspark job
        glue_args = self._get_glue_args(cli_args=cli_args)
        spark_session, job = self._get_spark_session_and_glue_job(glue_args)

        # run glue job code
        source = cli_args["source"]
        destination = cli_args["destination"]
        df = spark.read.csv(source)
        df.write.csv(destination)

        # commit job
        self._commit_job(job)

    @staticmethod
    def _get_spark_session_and_glue_job(glue_args):
        from awsglue.context import GlueContext
        from awsglue.job import Job

        sc = SparkContext.getOrCreate()
        glue_context = GlueContext(sparkContext=sc)
        job = Job(glue_context=glue_context)
        job.init(glue_args["JOB_NAME"], glue_args)
        return glue_context.spark_session, job

    @staticmethod
    def _commit_job(job):
        job.commit()

    @staticmethod
    def _get_glue_args(cli_args):
        from awsglue.utils import getResolvedOptions

        glue_args = getResolvedOptions(args=sys.argv, options=["JOB_NAME"] + cli_args)
        return glue_args


if __name__ == "__main__":
    GlueJob().run(["source", "destination"])
