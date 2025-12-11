from aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
)
from constructs import Construct
import os

class S3Stack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        self.input_bucket = s3.Bucket(
            self, "InputBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        self.output_bucket = s3.Bucket(
            self, "OutputBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        self.scripts_bucket = s3.Bucket(
            self, "ScriptsBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        s3deploy.BucketDeployment(
            self, "DeployScripts",
            sources=[s3deploy.Source.asset(
                os.path.join(os.path.dirname(__file__), "..", "..", "src")
            )],
            destination_bucket=self.scripts_bucket,
            destination_key_prefix="scripts"
        )

        s3deploy.BucketDeployment(
            self, "DeployData",
            sources=[s3deploy.Source.asset(
                os.path.join(os.path.dirname(__file__), "..", "..", "data")
            )],
            destination_bucket=self.input_bucket
        )
