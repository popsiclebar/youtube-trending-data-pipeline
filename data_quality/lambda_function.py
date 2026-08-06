"""Lambda entry point for the Silver-to-Gold data-quality gate."""

from dataclasses import asdict
import json
import logging

import boto3

from athena import AthenaQueryRunner
from checks import (
    resolve_process_date,
    run_category_checks,
    run_mapping_check,
    run_video_checks,
    validate_catalog,
)
from config import load_settings, scope_from_event


logger = logging.getLogger()
logger.setLevel(logging.INFO)

SETTINGS = load_settings()
athena_runner = AthenaQueryRunner(
    boto3.client("athena"),
    SETTINGS.silver_database,
    SETTINGS.athena_workgroup,
    SETTINGS.athena_output_location,
    SETTINGS.query_timeout_seconds,
)
glue = boto3.client("glue")
sns = boto3.client("sns")


def lambda_handler(event, context):
    """Run the requested gate and return a Step Functions-friendly decision."""
    try:
        scope = resolve_process_date(
            athena_runner, SETTINGS, scope_from_event(event or {}, SETTINGS)
        )
        results = validate_catalog(glue, SETTINGS, scope.checks)
        results.extend(run_video_checks(athena_runner, SETTINGS, scope))
        results.extend(run_category_checks(athena_runner, SETTINGS, scope))
        results.extend(run_mapping_check(athena_runner, SETTINGS, scope))
        failed = [result for result in results if not result.passed]
        response = {
            "quality_passed": not failed,
            "scope": asdict(scope) | {"checks": sorted(scope.checks)},
            "summary": {
                "passed": len(results) - len(failed),
                "failed": len(failed),
                "total": len(results),
            },
            "results": [result.as_dict() for result in results],
        }
        logger.info("Data-quality result: %s", json.dumps(response, default=str))
        if failed:
            _publish_alert("[YT Pipeline] Silver data-quality gate failed", response)
        return response
    except Exception as exc:
        logger.exception("Data-quality gate could not complete")
        _publish_alert(
            "[YT Pipeline] Data-quality gate execution error",
            {"quality_passed": False, "error_type": type(exc).__name__, "error": str(exc)},
        )
        raise


def _publish_alert(subject: str, payload: dict) -> None:
    """Send one compact failure notification without masking the gate outcome."""
    if not SETTINGS.sns_topic_arn:
        logger.warning("SNS_TOPIC_ARN is not configured; failure alert was skipped")
        return
    try:
        sns.publish(
            TopicArn=SETTINGS.sns_topic_arn,
            Subject=subject,
            Message=json.dumps(payload, default=str, indent=2),
        )
    except Exception:
        logger.exception("Failed to publish the data-quality SNS alert")
