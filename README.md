# Testing Glue Pyspark jobs

This repo contains example code that shows how you can test your glue pyspark jobs.

It accompanies this article on medium https://medium.com/@vincentclaes_43752/testing-glue-pyspark-jobs-4b544d62106e

`testing_mocked_s3.py` -> is the script used in the article.

`glue_job.py` -> is the glue pyspark job

`test_glue_job.py` -> is the test for glue_job.py

## Prerequisites

- you have python 3.6.8 installed;
- you have java jdk 8 installed;
- you have spark 2.4.3 for hadoop 2.7 installed.

## Run the test

    pip install pipenv
    git clone git@github.com:vincentclaes/testing-glue-pyspark-jobs.git
    cd testing-glue-pyspark-jobs
    pipenv install
    pipenv shell
    python -m unittest test_glue_job.py


    