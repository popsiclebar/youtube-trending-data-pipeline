"""Athena-backed checks for the daily Silver video and category datasets."""

from dataclasses import asdict, dataclass

from config import QualityRequest, QualityScope, Settings


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    actual: object
    expected: object

    def as_dict(self) -> dict:
        return asdict(self)


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _table(database: str, table: str) -> str:
    return f'"{database}"."{table}"'


def resolve_process_date(
    runner, settings: Settings, request: QualityRequest
) -> QualityScope:
    """Use the newest video partition date when the invocation omits one."""
    process_date = request.process_date
    if process_date is None:
        row = runner.execute(
            f"SELECT CAST(max(date) AS varchar) AS process_date "
            f"FROM {_table(settings.silver_database, settings.video_table)} "
            f"WHERE source = {_literal(request.source)}"
        )[0]
        process_date = row.get("process_date")
        if process_date is None:
            raise ValueError(
                f"No Silver video date found for source {request.source!r}"
            )
    return QualityScope(
        request.source,
        process_date,
        request.regions,
        request.expected_hours,
        request.checks,
    )


def validate_catalog(
    glue_client, settings: Settings, selected_checks: frozenset[str]
) -> list[CheckResult]:
    """Confirm that both Glue tables still expose the columns required by the gate."""
    if "catalog_schema" not in selected_checks:
        return []
    required = {
        settings.video_table: {
            "video_id", "batch_hour", "published_at", "channel_id", "category_id",
            "view_count", "like_count", "dislike_count", "favorite_count",
            "comment_count", "silver_ingestion_timestamp", "source", "region", "date",
        },
        settings.category_table: {
            "category_id",
            "category_title",
            "date",
            "source",
            "region",
        },
    }
    results = []
    for table_name, expected_columns in required.items():
        table = glue_client.get_table(
            DatabaseName=settings.silver_database, Name=table_name
        )["Table"]
        available = {
            column["Name"]
            for column in table["StorageDescriptor"]["Columns"] + table.get("PartitionKeys", [])
        }
        missing = sorted(expected_columns - available)
        results.append(CheckResult(f"catalog_schema:{table_name}", not missing, missing, []))
    return results


def run_video_checks(runner, settings: Settings, scope: QualityScope) -> list[CheckResult]:
    selected = scope.checks & {
        "video_rows",
        "video_critical_fields",
        "video_metric_ranges",
        "video_duplicates",
        "video_freshness",
        "region_coverage",
        "hour_coverage",
    }
    if not selected:
        return []

    regions_sql = ", ".join(_literal(region) for region in scope.regions)
    where = (
        f"source = {_literal(scope.source)} AND date = DATE {_literal(scope.process_date)} "
        f"AND region IN ({regions_sql})"
    )
    table = _table(settings.silver_database, settings.video_table)
    required_api_context_sql = (
        " OR channel_id IS NULL OR batch_hour IS NULL"
        if scope.source == "youtube_api"
        else ""
    )
    metrics = {}
    if selected & {
        "video_rows",
        "video_critical_fields",
        "video_metric_ranges",
        "video_freshness",
    }:
        metrics = runner.execute(
            "SELECT count(*) AS row_count, "
            "sum(CASE WHEN video_id IS NULL "
            f"OR category_id IS NULL OR published_at IS NULL{required_api_context_sql} "
            "THEN 1 ELSE 0 END) AS critical_null_rows, "
            "sum(CASE WHEN coalesce(view_count, 0) < 0 "
            "OR coalesce(like_count, 0) < 0 OR coalesce(dislike_count, 0) < 0 "
            "OR coalesce(favorite_count, 0) < 0 OR coalesce(comment_count, 0) < 0 "
            "THEN 1 ELSE 0 END) AS negative_metric_rows, "
            "date_diff('hour', max(silver_ingestion_timestamp), "
            "CAST(current_timestamp AS timestamp)) AS freshness_hours "
            f"FROM {table} WHERE {where}"
        )[0]

    duplicate_rows = 0
    if "video_duplicates" in selected:
        duplicate_metrics = runner.execute(
            "SELECT count(*) AS duplicate_groups, "
            "coalesce(sum(record_count - 1), 0) AS duplicate_rows FROM ("
            "SELECT region, date, batch_hour, video_id, count(*) AS record_count "
            f"FROM {table} WHERE {where} "
            "GROUP BY region, date, batch_hour, video_id HAVING count(*) > 1)"
        )[0]
        duplicate_rows = int(duplicate_metrics["duplicate_rows"] or 0)

    coverage = []
    if selected & {"region_coverage", "hour_coverage"}:
        coverage = runner.execute(
            f"SELECT region, batch_hour FROM {table} WHERE {where} "
            "GROUP BY region, batch_hour"
        )

    row_count = int(metrics.get("row_count") or 0)
    null_rows = int(metrics.get("critical_null_rows") or 0)
    negative_rows = int(metrics.get("negative_metric_rows") or 0)
    freshness = (
        int(metrics["freshness_hours"])
        if metrics.get("freshness_hours") is not None
        else None
    )
    observed_regions = {row["region"] for row in coverage}
    observed_hours = {}
    for row in coverage:
        observed_hours.setdefault(row["region"], set()).add(row["batch_hour"])

    candidates = {
        "video_rows": CheckResult(
            "video_rows",
            row_count >= settings.min_video_rows,
            row_count,
            f">={settings.min_video_rows}",
        ),
        "video_critical_fields": CheckResult("video_critical_fields", null_rows == 0, null_rows, 0),
        "video_metric_ranges": CheckResult(
            "video_metric_ranges", negative_rows == 0, negative_rows, 0
        ),
        "video_duplicates": CheckResult("video_duplicates", duplicate_rows == 0, duplicate_rows, 0),
        "video_freshness": CheckResult(
            "video_freshness", freshness is not None and freshness <= settings.max_freshness_hours,
            freshness, f"<={settings.max_freshness_hours} hours",
        ),
        "region_coverage": CheckResult(
            "region_coverage",
            observed_regions == set(scope.regions),
            sorted(observed_regions),
            sorted(scope.regions),
        ),
    }
    if scope.expected_hours:
        missing_hours = {
            region: sorted(set(scope.expected_hours) - observed_hours.get(region, set()))
            for region in scope.regions
        }
        missing_hours = {region: hours for region, hours in missing_hours.items() if hours}
        candidates["hour_coverage"] = CheckResult(
            "hour_coverage", not missing_hours, missing_hours, {}
        )
    return [result for name, result in candidates.items() if name in scope.checks]


def run_category_checks(runner, settings: Settings, scope: QualityScope) -> list[CheckResult]:
    selected = scope.checks & {
        "category_rows",
        "category_critical_fields",
        "category_duplicates",
    }
    if not selected:
        return []

    regions_sql = ", ".join(_literal(region) for region in scope.regions)
    date_filter = (
        "date IS NULL"
        if scope.source == "kaggle"
        else f"TRY_CAST(date AS DATE) = DATE {_literal(scope.process_date)}"
    )
    where = (
        f"source = {_literal(scope.source)} "
        f"AND {date_filter} "
        f"AND region IN ({regions_sql})"
    )
    table = _table(settings.silver_database, settings.category_table)
    metrics = {}
    if selected & {"category_rows", "category_critical_fields"}:
        metrics = runner.execute(
            "SELECT count(*) AS row_count, "
            "sum(CASE WHEN category_id IS NULL OR category_title IS NULL "
            "OR trim(category_title) = '' THEN 1 ELSE 0 END) "
            f"AS critical_null_rows FROM {table} WHERE {where}"
        )[0]
    duplicate_rows = 0
    if "category_duplicates" in selected:
        duplicates = runner.execute(
            "SELECT coalesce(sum(record_count - 1), 0) AS duplicate_rows FROM ("
            "SELECT region, date, category_id, count(*) AS record_count "
            f"FROM {table} WHERE {where} "
            "GROUP BY region, date, category_id HAVING count(*) > 1)"
        )[0]
        duplicate_rows = int(duplicates["duplicate_rows"] or 0)
    row_count = int(metrics.get("row_count") or 0)
    null_rows = int(metrics.get("critical_null_rows") or 0)
    candidates = {
        "category_rows": CheckResult(
            "category_rows",
            row_count >= settings.min_category_rows,
            row_count,
            f">={settings.min_category_rows}",
        ),
        "category_critical_fields": CheckResult(
            "category_critical_fields", null_rows == 0, null_rows, 0
        ),
        "category_duplicates": CheckResult(
            "category_duplicates", duplicate_rows == 0, duplicate_rows, 0
        ),
    }
    return [result for name, result in candidates.items() if name in scope.checks]


def run_mapping_check(runner, settings: Settings, scope: QualityScope) -> list[CheckResult]:
    if "category_mapping" not in scope.checks:
        return []
    regions_sql = ", ".join(_literal(region) for region in scope.regions)
    source = _literal(scope.source)
    process_date = _literal(scope.process_date)
    date_filter = (
        "date IS NULL"
        if scope.source == "kaggle"
        else f"TRY_CAST(date AS DATE) = DATE {process_date}"
    )
    video_table = _table(settings.silver_database, settings.video_table)
    category_table = _table(settings.silver_database, settings.category_table)
    row = runner.execute(
        "SELECT count(*) AS missing_category_rows FROM ("
        "SELECT DISTINCT region, category_id FROM " + video_table + " "
        f"WHERE source = {source} AND date = DATE {process_date} AND region IN ({regions_sql})"
        ") v LEFT JOIN ("
        "SELECT DISTINCT region, category_id FROM " + category_table + " "
        f"WHERE source = {source} AND {date_filter} "
        f"AND region IN ({regions_sql})"
        ") c ON v.region = c.region AND v.category_id = c.category_id WHERE c.category_id IS NULL"
    )[0]
    missing = int(row["missing_category_rows"] or 0)
    return [CheckResult("category_mapping", missing == 0, missing, 0)]
