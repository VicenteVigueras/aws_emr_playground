from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_iam as iam,
    aws_emrserverless as emr,
    aws_events as events,
    aws_events_targets as targets,
)
from constructs import Construct

class PocStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # S3 Buckets
        self.input_bucket = s3.Bucket(
            self, "InputBucket",
            bucket_name="pyspark-poc-input-bucket",
            versioned=False,
            removal_policy=s3.RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        self.output_bucket = s3.Bucket(
            self, "OutputBucket",
            bucket_name="pyspark-poc-output-bucket",
            versioned=False,
            removal_policy=s3.RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        # IAM Role for EMR Serverless
        self.emr_role = iam.Role(
            self, "EMRServerlessRole",
            assumed_by=iam.ServicePrincipal("emr-serverless.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")
            ]
        )

        # EMR Serverless Application
        self.emr_app = emr.CfnApplication(
            self, "PySparkApp",
            release_label="emr-6.13.0",  # EMR release
            type="SPARK",
            name="pyspark-poc-app",
            initial_capacity=[{
                "workerConfiguration": {
                    "cpu": "2vCPU",
                    "memory": "4GB",
                    "disk": "10GB"
                },
                "workerCount": 1
            }],
            maximum_capacity={
                "cpu": "4vCPU",
                "memory": "8GB",
                "disk": "20GB"
            }
        )

        # EventBridge Rule for Scheduling
        schedule_rule = events.Rule(
            self, "PySparkSchedule",
            schedule=events.Schedule.cron(minute="0", hour="6") 
        )

    # This is a WIP. I am still learning how IaC is provisioned for this use case.
    # The scheduling of jobs will also be done in such a way that it can be tested at different times to validate proper execution
    # Lastly, I will specify the cost associated with running this program as of December 2025
       