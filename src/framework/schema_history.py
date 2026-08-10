"""
Enterprise Schema History Framework

Reusable across:
- Incremental Batch
- Micro Batch
- Streaming

Tracks schema evolution over time.
"""

from __future__ import annotations
from pyspark.sql import SparkSession
from pyspark.errors import AnalysisException
import json
from typing import Any
import hashlib
from delta.tables import DeltaTable

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, col


class SchemaHistory:
    """
    Enterprise reusable schema history manager.
    """

    def __init__(
        self,
        spark: SparkSession,
        is_databricks: bool,
        schema_history_path: str,
        schema_history_table: str,
        schema_changes_path: str,
        schema_changes_table: str,
    ):
        self.spark = spark
        self.is_databricks = is_databricks

        self.schema_history_path = schema_history_path
        self.schema_history_table = schema_history_table

        self.schema_changes_path = schema_changes_path
        self.schema_changes_table = schema_changes_table

    ####################################################################
    # Schema Utilities
    ####################################################################

    @staticmethod
    def schema_dict(
        df: DataFrame,
    ) -> dict[str, str]:
        """
        Convert Spark schema into dictionary.
        """

        return {field.name: field.dataType.simpleString() for field in df.schema.fields}

    @staticmethod
    def schema_hash(schema_json: str) -> str:
        """
        Return SHA256 hash of a schema JSON.
        """

        return hashlib.sha256(schema_json.encode("utf-8")).hexdigest()

    ####################################################################
    # History Writer
    ####################################################################

    def save_schema(
        self,
        df: DataFrame,
        pipeline_name: str,
        stage: str,
    ) -> None:
        """
        Save current schema snapshot.
        """

        schema_json = json.dumps(
            self.schema_dict(df),
            sort_keys=True,
        )

        schema_hash = self.schema_hash(schema_json)

        latest_hash = self.load_latest_hash(
            df.sparkSession,
            pipeline_name,
            stage,
        )

        # No schema change
        if latest_hash == schema_hash:
            return

        version = self.get_next_version(
            df.sparkSession,
            pipeline_name,
            stage,
        )

        history_df = df.sparkSession.createDataFrame(
            [
                (
                    pipeline_name,
                    stage,
                    version,
                    schema_hash,
                    schema_json,
                )
            ],
            [
                "pipeline_name",
                "stage",
                "version",
                "schema_hash",
                "schema_json",
            ],
        ).withColumn(
            "recorded_at",
            current_timestamp(),
        )

        if self.is_databricks:

            if self.spark.catalog.tableExists(self.schema_history_table):

                (history_df.write.mode("append").saveAsTable(self.schema_history_table))

            else:

                (
                    history_df.write.mode("overwrite").saveAsTable(
                        self.schema_history_table
                    )
                )

        else:

            if DeltaTable.isDeltaTable(
                self.spark,
                self.schema_history_path,
            ):

                (
                    history_df.write.format("delta")
                    .mode("append")
                    .save(self.schema_history_path)
                )

            else:

                (
                    history_df.write.format("delta")
                    .mode("overwrite")
                    .save(self.schema_history_path)
                )

    def load_latest_schema(
        self,
        spark: SparkSession,
        pipeline_name: str,
        stage: str,
    ) -> dict[str, str] | None:
        """
        Return latest saved schema for a pipeline stage.
        """

        try:

            if self.is_databricks:

                if not self.spark.catalog.tableExists(
                    self.schema_history_table,
                ):
                    return None

                history_df = (
                    self.spark.table(self.schema_history_table)
                    .filter(col("pipeline_name") == pipeline_name)
                    .filter(col("stage") == stage)
                    .orderBy(col("version").desc())
                    .limit(1)
                )

            else:

                if not DeltaTable.isDeltaTable(
                    self.spark,
                    self.schema_history_path,
                ):
                    return None

                history_df = (
                    self.spark.read.format("delta")
                    .load(self.schema_history_path)
                    .filter(col("pipeline_name") == pipeline_name)
                    .filter(col("stage") == stage)
                    .orderBy(col("version").desc())
                    .limit(1)
                )

            row = history_df.first()

            if row is None:
                return None

            return json.loads(row["schema_json"])

        except AnalysisException:
            return None

    def load_latest_hash(
        self,
        spark: SparkSession,
        pipeline_name: str,
        stage: str,
    ) -> str | None:
        """
        Return the latest schema hash for a pipeline stage.
        """
        try:

            if self.is_databricks:

                if not self.spark.catalog.tableExists(
                    self.schema_history_table,
                ):
                    return None

                history_df = (
                    self.spark.table(self.schema_history_table)
                    .filter(col("pipeline_name") == pipeline_name)
                    .filter(col("stage") == stage)
                    .orderBy(col("version").desc())
                    .limit(1)
                )

            else:

                if not DeltaTable.isDeltaTable(
                    self.spark,
                    self.schema_history_path,
                ):
                    return None

                history_df = (
                    self.spark.read.format("delta")
                    .load(self.schema_history_path)
                    .filter(col("pipeline_name") == pipeline_name)
                    .filter(col("stage") == stage)
                    .orderBy(col("version").desc())
                    .limit(1)
                )

            row = history_df.first()

            if row is None:
                return None

            return row["schema_hash"]

        except AnalysisException:
            return None

    def get_next_version(
        self,
        spark: SparkSession,
        pipeline_name: str,
        stage: str,
    ) -> int:
        """
        Return the next schema version for a pipeline stage.
        """

        try:

            if self.is_databricks:

                if not self.spark.catalog.tableExists(
                    self.schema_history_table,
                ):
                    return 1

                history_df = (
                    self.spark.table(self.schema_history_table)
                    .filter(col("pipeline_name") == pipeline_name)
                    .filter(col("stage") == stage)
                )

            else:

                if not DeltaTable.isDeltaTable(
                    self.spark,
                    self.schema_history_path,
                ):
                    return 1

                history_df = (
                    self.spark.read.format("delta")
                    .load(self.schema_history_path)
                    .filter(col("pipeline_name") == pipeline_name)
                    .filter(col("stage") == stage)
                )

            if history_df.rdd.isEmpty():
                return 1

            latest_version = history_df.agg({"version": "max"}).first()[0]

            return 1 if latest_version is None else latest_version + 1

        except AnalysisException:
            return 1

    ####################################################################
    # Schema Comparison
    ####################################################################

    @staticmethod
    def compare(
        previous_schema: dict[str, str],
        current_schema: dict[str, str],
    ) -> list[dict[str, Any]]:
        """
        Compare two schema dictionaries.
        """

        changes = []

        columns = sorted(set(previous_schema.keys()) | set(current_schema.keys()))

        for column in columns:

            old_type = previous_schema.get(column)

            new_type = current_schema.get(column)

            if old_type is None:

                changes.append(
                    {
                        "change_type": "COLUMN_ADDED",
                        "column": column,
                        "old_type": None,
                        "new_type": new_type,
                    }
                )

            elif new_type is None:

                changes.append(
                    {
                        "change_type": "COLUMN_REMOVED",
                        "column": column,
                        "old_type": old_type,
                        "new_type": None,
                    }
                )

            elif old_type != new_type:

                changes.append(
                    {
                        "change_type": "TYPE_CHANGED",
                        "column": column,
                        "old_type": old_type,
                        "new_type": new_type,
                    }
                )

        return changes

    def record_changes(
        self,
        spark: SparkSession,
        pipeline_name: str,
        stage: str,
        changes: list[dict[str, Any]],
        action: str,
    ) -> None:
        """
        Persist schema evolution events.
        """

        if not changes:
            return

        rows = []

        for change in changes:

            rows.append(
                (
                    pipeline_name,
                    stage,
                    change["change_type"],
                    change["column"],
                    change["old_type"],
                    change["new_type"],
                    action,
                )
            )

        history_df = spark.createDataFrame(
            rows,
            [
                "pipeline_name",
                "stage",
                "change_type",
                "column_name",
                "old_type",
                "new_type",
                "action",
            ],
        ).withColumn(
            "recorded_at",
            current_timestamp(),
        )

        if self.is_databricks:

            (history_df.write.mode("append").saveAsTable(self.schema_changes_table))

        else:

            (
                history_df.write.format("delta")
                .mode("append")
                .save(self.schema_changes_path)
            )

    def has_schema_changed(
        self,
        previous_schema: dict[str, str] | None,
        current_schema: dict[str, str],
    ) -> bool:
        """
        Return True if the schema has changed.
        """

        if previous_schema is None:
            return True

        return (
            len(
                self.compare(
                    previous_schema,
                    current_schema,
                )
            )
            > 0
        )

    def schema_report(
        self,
        changes: list[dict[str, Any]],
    ) -> str:
        """
        Return a human-readable schema evolution report.
        """

        if not changes:
            return "No schema changes detected."

        report = [f"Schema Evolution Report ({len(changes)} change(s))"]

        for change in changes:

            report.append(
                (
                    f"[{change['change_type']}] "
                    f"{change['column']} "
                    f"({change['old_type']} -> {change['new_type']})"
                )
            )

        return "\n".join(report)
