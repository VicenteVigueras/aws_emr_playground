from aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_s3 as s3,
    aws_iam as iam,
    aws_emrserverless as emr,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda as _lambda,
)
from constructs import Construct
import os

class PocStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        input_bucket_name = f"pyspark-poc-input-{self.account}-{self.region}"
        output_bucket_name = f"pyspark-poc-output-{self.account}-{self.region}"

        self.input_bucket = s3.Bucket(
            self, "InputBucket",
            bucket_name=input_bucket_name,
            versioned=False,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        self.output_bucket = s3.Bucket(
            self, "OutputBucket",
            bucket_name=output_bucket_name,
            versioned=False,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        self.emr_role = iam.Role(
            self, "EMRServerlessRole",
            assumed_by=iam.ServicePrincipal("emr-serverless.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")
            ]
        )

        self.emr_app = emr.CfnApplication(
            self, "PySparkApp",
            release_label="emr-6.13.0",
            type="SPARK",
            name="pyspark-poc-app",
            initial_capacity=[
                emr.CfnApplication.InitialCapacityConfigKeyValuePairProperty(
                    key="Driver",
                    value=emr.CfnApplication.InitialCapacityConfigProperty(
                        worker_count=1,
                        worker_configuration=emr.CfnApplication.WorkerConfigurationProperty(
                            cpu="1vCPU",
                            memory="2GB",
                            disk="20GB"
                        )
                    )
                ),
                emr.CfnApplication.InitialCapacityConfigKeyValuePairProperty(
                    key="Executor",
                    value=emr.CfnApplication.InitialCapacityConfigProperty(
                        worker_count=1,
                        worker_configuration=emr.CfnApplication.WorkerConfigurationProperty(
                            cpu="2vCPU",
                            memory="4GB",
                            disk="40GB"
                        )
                    )
                )
            ],
            maximum_capacity=emr.CfnApplication.MaximumAllowedResourcesProperty(
                cpu="4vCPU",
                memory="8GB",
                disk="80GB"
            )
        )

        lambda_asset_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "poc_stack", "lambda_submit")
        )

        submit_lambda = _lambda.Function(
            self, "SubmitJobLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="submit_job.handler",
            code=_lambda.Code.from_asset(lambda_asset_path),
            environment={
                "EMR_APP_ID": self.emr_app.ref,
                "EMR_EXEC_ROLE": self.emr_role.role_arn,
                "INPUT_PATH": f"s3://{self.input_bucket.bucket_name}/data.csv",
                "OUTPUT_PATH": f"s3://{self.output_bucket.bucket_name}/output/"
            }
        )

        submit_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "emr-serverless:StartJobRun",
                    "emr-serverless:GetApplication",
                    "s3:*",
                    "iam:PassRole"  
                ],
                resources=[
                    "*", 
                ]
            )
        )

        schedule_rule = events.Rule(
            self, "PySparkSchedule",
            schedule=events.Schedule.cron(minute="0", hour="9,17")
        )
        schedule_rule.add_target(targets.LambdaFunction(submit_lambda))
