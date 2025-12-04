#!/usr/bin/env python3
import os
from aws_cdk import App
from poc_stack.poc_stack import PocStack

app = App()
PocStack(app, "PySparkPOCStack", env={
    "account": os.environ.get("CDK_DEFAULT_ACCOUNT"),
    "region": os.environ.get("CDK_DEFAULT_REGION")
})
app.synth()