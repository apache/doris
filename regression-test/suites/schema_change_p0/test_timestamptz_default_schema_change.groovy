// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_timestamptz_default_schema_change") {
    def originalTimeZone = sql("SELECT @@time_zone")[0][0]
    def originalPartialUpdate = sql("SELECT @@enable_unique_key_partial_update")[0][0]
    def originalInsertStrict = sql("SELECT @@enable_insert_strict")[0][0]
    try {
        sql "SET time_zone = 'America/New_York'"
        sql "DROP TABLE IF EXISTS test_timestamptz_default_schema_change"
        sql """
            CREATE TABLE test_timestamptz_default_schema_change (
                id INT,
                created_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            )
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true"
            )
        """
        sql "INSERT INTO test_timestamptz_default_schema_change (id) VALUES (1), (2)"

        def waitForAlter = {
            waitForSchemaChangeDone({
                sql """
                    SHOW ALTER TABLE COLUMN
                    WHERE TableName = 'test_timestamptz_default_schema_change'
                    ORDER BY CreateTime DESC LIMIT 1
                """
                time 600
            })
        }
        sql """
            ALTER TABLE test_timestamptz_default_schema_change
            ADD COLUMN backfill_plain TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP
        """
        waitForAlter()
        sql """
            ALTER TABLE test_timestamptz_default_schema_change ADD COLUMN (
                backfill_zero TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
                backfill_micro TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            )
        """
        waitForAlter()

        // Only output deterministic checks. The lower bound also detects a session-local
        // datetime incorrectly tagged as UTC; successful parsing alone would miss that error.
        def checkBackfill = """
            SELECT id,
                backfill_plain BETWEEN created_at - INTERVAL 1 SECOND
                    AND CAST(CURRENT_TIMESTAMP(6) AS TIMESTAMPTZ(6)) AS plain_in_range,
                backfill_zero BETWEEN created_at - INTERVAL 1 SECOND
                    AND CAST(CURRENT_TIMESTAMP(6) AS TIMESTAMPTZ(6)) AS zero_in_range,
                backfill_micro BETWEEN created_at - INTERVAL 1 SECOND
                    AND CAST(CURRENT_TIMESTAMP(6) AS TIMESTAMPTZ(6)) AS micro_in_range,
                MICROSECOND(backfill_plain) = 0 AS plain_precision,
                MICROSECOND(backfill_zero) = 0 AS zero_precision
            FROM test_timestamptz_default_schema_change
            ORDER BY id
        """
        qt_backfill_new_york checkBackfill
        qt_backfill_constant """
            SELECT COUNT(DISTINCT backfill_plain), COUNT(DISTINCT backfill_zero),
                COUNT(DISTINCT backfill_micro)
            FROM test_timestamptz_default_schema_change
        """

        // Reading the same historical rows in another timezone must preserve their instants.
        sql "SET time_zone = '+00:00'"
        qt_backfill_utc checkBackfill

        // The column defaults must remain expressions for subsequent inserts.
        sql "INSERT INTO test_timestamptz_default_schema_change (id) VALUES (3)"
        qt_insert_after_alter checkBackfill

        // A new row in a partial update evaluates defaults at load time, rather than
        // reusing the fixed backfill instant persisted by schema change.
        sql "SET enable_unique_key_partial_update = true"
        sql "SET enable_insert_strict = false"
        sql """
            INSERT INTO test_timestamptz_default_schema_change (id, created_at)
            VALUES (4, CAST(CURRENT_TIMESTAMP(6) AS TIMESTAMPTZ(6)))
        """
        qt_partial_update_after_alter checkBackfill

        // Batch ADD must materialize TIMESTAMP_NS backfills while retaining DATETIME's
        // existing conversion. Both types must evaluate defaults for a new partial-update key.
        sql "SET enable_unique_key_partial_update = false"
        sql "DROP TABLE IF EXISTS test_timestamp_batch_default_schema_change"
        sql """
            CREATE TABLE test_timestamp_batch_default_schema_change (
                id INT,
                created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            )
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true"
            )
        """
        sql "INSERT INTO test_timestamp_batch_default_schema_change (id) VALUES (1), (2)"
        sql """
            ALTER TABLE test_timestamp_batch_default_schema_change ADD COLUMN (
                backfill_ns_plain TIMESTAMP_NS NOT NULL DEFAULT CURRENT_TIMESTAMP,
                backfill_ns_nano TIMESTAMP_NS NOT NULL DEFAULT CURRENT_TIMESTAMP(9),
                backfill_datetime DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            )
        """
        waitForSchemaChangeDone({
            sql """
                SHOW ALTER TABLE COLUMN
                WHERE TableName = 'test_timestamp_batch_default_schema_change'
                ORDER BY CreateTime DESC LIMIT 1
            """
            time 600
        })
        qt_timestamp_ns_batch_backfill """
            SELECT id,
                backfill_ns_plain BETWEEN CAST(created_at - INTERVAL 1 SECOND AS TIMESTAMP_NS)
                    AND CAST(CURRENT_TIMESTAMP(6) AS TIMESTAMP_NS),
                backfill_ns_nano BETWEEN CAST(created_at AS TIMESTAMP_NS)
                    AND CAST(CURRENT_TIMESTAMP(6) AS TIMESTAMP_NS)
            FROM test_timestamp_batch_default_schema_change ORDER BY id
        """

        // Separate load time from ALTER time so a frozen default cannot pass this check.
        Thread.sleep(2000)
        sql "SET enable_unique_key_partial_update = true"
        sql """
            INSERT INTO test_timestamp_batch_default_schema_change (id, created_at)
            VALUES (3, CURRENT_TIMESTAMP(6))
        """
        qt_timestamp_batch_partial_insert """
            SELECT n.backfill_ns_plain > o.ns_plain,
                n.backfill_ns_nano > o.ns_nano,
                n.backfill_datetime > o.datetime_value
            FROM test_timestamp_batch_default_schema_change n
            CROSS JOIN (
                SELECT MAX(backfill_ns_plain) AS ns_plain, MAX(backfill_ns_nano) AS ns_nano,
                    MAX(backfill_datetime) AS datetime_value
                FROM test_timestamp_batch_default_schema_change WHERE id IN (1, 2)
            ) o
            WHERE n.id = 3
        """
    } finally {
        sql "SET enable_unique_key_partial_update = ${originalPartialUpdate}"
        sql "SET enable_insert_strict = ${originalInsertStrict}"
        sql "SET time_zone = '${originalTimeZone}'"
    }
}
