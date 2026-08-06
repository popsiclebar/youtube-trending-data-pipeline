"""Small boto3 Athena query runner used by the Lambda quality gate."""

import time


class AthenaQueryError(RuntimeError):
    """Raised when Athena cannot complete a data-quality query."""


class AthenaQueryRunner:
    def __init__(
        self,
        client,
        database: str,
        workgroup: str,
        output_location: str,
        timeout_seconds: int,
    ):
        self.client = client
        self.database = database
        self.workgroup = workgroup
        self.output_location = output_location
        self.timeout_seconds = timeout_seconds

    def execute(self, sql: str) -> list[dict[str, str | None]]:
        """Execute SQL, wait for completion, and return rows keyed by column name."""
        response = self.client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": self.database},
            WorkGroup=self.workgroup,
            ResultConfiguration={"OutputLocation": self.output_location},
        )
        execution_id = response["QueryExecutionId"]
        deadline = time.monotonic() + self.timeout_seconds

        while True:
            execution = self.client.get_query_execution(QueryExecutionId=execution_id)
            status = execution["QueryExecution"]["Status"]
            state = status["State"]
            if state == "SUCCEEDED":
                break
            if state in {"FAILED", "CANCELLED"}:
                reason = status.get("StateChangeReason", "No failure reason returned")
                raise AthenaQueryError(f"Athena query {execution_id} {state.lower()}: {reason}")
            if time.monotonic() >= deadline:
                raise AthenaQueryError(
                    f"Athena query {execution_id} exceeded "
                    f"{self.timeout_seconds} seconds"
                )
            time.sleep(1)

        rows = []
        headers = None
        next_token = None
        while True:
            request = {"QueryExecutionId": execution_id}
            if next_token:
                request["NextToken"] = next_token
            page = self.client.get_query_results(**request)
            page_rows = page["ResultSet"]["Rows"]
            if headers is None:
                headers = [item.get("VarCharValue", "") for item in page_rows[0]["Data"]]
                page_rows = page_rows[1:]
            for row in page_rows:
                values = [item.get("VarCharValue") for item in row.get("Data", [])]
                values.extend([None] * (len(headers) - len(values)))
                rows.append(dict(zip(headers, values)))
            next_token = page.get("NextToken")
            if not next_token:
                return rows
