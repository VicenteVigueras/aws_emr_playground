from aws_cdk import (
    Stack,
    aws_iam as iam,
    aws_emrserverless as emr,
)
from constructs import Construct

class EmrServerlessStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        self.emr_role = iam.Role(
            self, "EMRServerlessRole",
            assumed_by=iam.ServicePrincipal("emr-serverless.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchLogsFullAccess")
            ]
        )

        self.emr_app = emr.CfnApplication(
            self,
            "PySparkApp",
            release_label="emr-6.13.0",
            type="SPARK",
            name="pyspark-poc-app",
            initial_capacity=[],
            maximum_capacity=emr.CfnApplication.MaximumAllowedResourcesProperty(
                cpu="4vCPU",
                memory="8GB",
                disk="40GB"
            )
        )
