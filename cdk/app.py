#!/usr/bin/env python3
import os
from aws_cdk import App

from stacks.s3_stack import S3Stack
from stacks.emr_serverless_stack import EmrServerlessStack
from stacks.lambda_stack import LambdaStack

app = App()

env_settings = {
    "account": os.environ.get("CDK_DEFAULT_ACCOUNT"),
    "region": os.environ.get("CDK_DEFAULT_REGION"),
}

s3_stack = S3Stack(
    app,
    "PySparkPOC-S3",
    env=env_settings
)

emr_stack = EmrServerlessStack(
    app,
    "PySparkPOC-EMR",
    env=env_settings
)

lambda_stack = LambdaStack(
    app,
    "PySparkPOC-Lambda",
    env=env_settings,
    input_bucket=s3_stack.input_bucket,
    output_bucket=s3_stack.output_bucket,
    scripts_bucket=s3_stack.scripts_bucket,
    emr_app=emr_stack.emr_app,
    emr_role=emr_stack.emr_role
)

app.synth()
