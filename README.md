# Testing Glue Pyspark jobs

This repo contains example code on how you can test your glue pyspark jobs.
It accompanies this article on medium

`testing_mocked_s3.py` -> is the script used in the article

`glue_job.py` -> is the glue pyspark job

`test_glue_job.py` -> is the test for glue_job.py

## Run the test

    pip install pipenv
    git clone git@github.com:vincentclaes/testing-glue-pyspark-jobs.git
    cd testing-glue-pyspark-jobs
    pipenv install
    pipenv shell
    python -m unittest test_glue_job.py


    