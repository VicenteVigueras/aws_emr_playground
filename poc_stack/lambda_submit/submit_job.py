import boto3
import os
import json

def handler(event, context):
    """
    Lambda handler to submit PySpark job to EMR Serverless
    """
    client = boto3.client("emr-serverless")

    app_id = os.environ["EMR_APP_ID"]
    exec_role = os.environ["EMR_EXEC_ROLE"]
    script_path = os.environ["SCRIPT_PATH"]
    input_path = os.environ["INPUT_PATH"]
    output_path = os.environ["OUTPUT_PATH"]

    print(f"Starting EMR Serverless job with:")
    print(f"  Application ID: {app_id}")
    print(f"  Script: {script_path}")
    print(f"  Input: {input_path}")
    print(f"  Output: {output_path}")

    try:
        response = client.start_job_run(
            applicationId=app_id,
            executionRoleArn=exec_role,
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": script_path,
                    "entryPointArguments": [input_path, output_path],
                    "sparkSubmitParameters": (
                        f"--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
                        f"--conf spark.executor.instances=1 "
                        f"--conf spark.executor.cores=2 "
                        f"--conf spark.executor.memory=3G "
                        f"--conf spark.driver.cores=2 "
                        f"--conf spark.driver.memory=3G "
                        f"--conf spark.dynamicAllocation.enabled=false "
                        f"--conf spark.app.input.path={input_path} "
                        f"--conf spark.app.output.path={output_path}"
                    )
                }
            },
            configurationOverrides={
                "monitoringConfiguration": {
                    "cloudWatchLoggingConfiguration": {
                        "enabled": True,
                        "logGroupName": "/emr-serverless/pyspark-poc",
                        "logStreamNamePrefix": "job"
                    }
                },
                "applicationConfiguration": [
                    {
                        "classification": "spark-defaults",
                        "properties": {
                            "spark.hadoop.hive.metastore.client.factory.class":
                                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
                        }
                    }
                ]
            },
            executionTimeoutMinutes=30
        )

        job_run_id = response["jobRunId"]
        print(f"Job submitted successfully. JobRunId: {job_run_id}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "submitted",
                "jobRunId": job_run_id,
                "applicationId": app_id
            })
        }

    except Exception as e:
        print(f"Error submitting job: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "status": "error",
                "message": str(e)
            })
        }