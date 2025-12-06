import boto3
import os

def handler(event, context):
    emr_app_id = os.environ["EMR_APP_ID"]
    emr_role = os.environ["EMR_EXEC_ROLE"]
    input_path = os.environ["INPUT_PATH"]
    output_path = os.environ["OUTPUT_PATH"]

    client = boto3.client("emr-serverless")

    bucket_name = input_path.split("/")[2] 
    
    response = client.start_job_run(
        applicationId=emr_app_id,
        executionRoleArn=emr_role,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": f"s3://{bucket_name}/src/main.py",
                "entryPointArguments": [],
                "sparkSubmitParameters": (
                    f"--conf spark.executor.memory=2G "
                    f"--conf spark.emr-serverless.driverEnv.INPUT_PATH={input_path} "
                    f"--conf spark.emr-serverless.driverEnv.OUTPUT_PATH={output_path} "
                    f"--conf spark.emr-serverless.executorEnv.INPUT_PATH={input_path} "
                    f"--conf spark.emr-serverless.executorEnv.OUTPUT_PATH={output_path}"
                )
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{output_path.split('/')[2]}/logs/"
                }
            }
        }
    )

    print("Started EMR job:", response.get("jobRunId", response))
    return {
        "statusCode": 200,
        "jobRunId": response.get("jobRunId"),
        "applicationId": emr_app_id
    }