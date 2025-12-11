from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
)
from constructs import Construct
import os

class LambdaStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        input_bucket,
        output_bucket,
        scripts_bucket,
        emr_app,
        emr_role,
        **kwargs
    ):
        super().__init__(scope, construct_id, **kwargs)

        lambda_asset = os.path.join(
            os.path.dirname(__file__), "..", "..", "poc_stack", "lambda_submit"
        )

        submit_lambda = _lambda.Function(
            self, "SubmitJobLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="submit_job.handler",
            code=_lambda.Code.from_asset(lambda_asset),
            timeout=Duration.seconds(60),
            environment={
                "EMR_APP_ID": emr_app.ref,
                "EMR_EXEC_ROLE": emr_role.role_arn,
                "SCRIPT_PATH": f"s3://{scripts_bucket.bucket_name}/scripts/main.py",
                "INPUT_PATH": f"s3://{input_bucket.bucket_name}/data.csv",
                "OUTPUT_PATH": f"s3://{output_bucket.bucket_name}/output/"
            }
        )

        submit_lambda.add_to_role_policy(iam.PolicyStatement(
            actions=["emr-serverless:StartJobRun", "iam:PassRole"],
            resources=["*"]
        ))

        # S3 permissions
        input_bucket.grant_read(submit_lambda)
        output_bucket.grant_read_write(submit_lambda)
        scripts_bucket.grant_read(submit_lambda)

        # Optional cron schedule
        rule = events.Rule(
            self, "PySparkSchedule",
            schedule=events.Schedule.cron(minute="0", hour="9,17"),
            enabled=False
        )
        rule.add_target(targets.LambdaFunction(submit_lambda))
