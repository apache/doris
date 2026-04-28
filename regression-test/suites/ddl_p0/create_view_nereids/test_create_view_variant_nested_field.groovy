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

suite("test_create_view_variant_nested_field") {
    sql "SET enable_nereids_planner=true;"
    sql "SET enable_fallback_to_original_planner=false;"

    String tableName = "test_create_view_variant_nested_field_events"
    String viewName = "test_create_view_variant_nested_field_view"
    String bracketViewName = "test_create_view_variant_nested_field_bracket_view"

    sql "DROP VIEW IF EXISTS ${viewName}"
    sql "DROP VIEW IF EXISTS ${bracketViewName}"
    sql "DROP TABLE IF EXISTS ${tableName}"

    sql """
        CREATE TABLE ${tableName} (
            event_key LARGEINT NOT NULL,
            event_value VARIANT<'video_id':largeint, 'duration':bigint>,
            user_connect_info VARIANT<'user_client':text>
        )
        DUPLICATE KEY(event_key)
        DISTRIBUTED BY HASH(event_key) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "storage_format" = "V3"
        )
    """

    sql """
        INSERT INTO ${tableName} VALUES
        (1, '{"video_id":100,"duration":15000}', '{"user_client":"ios"}')
    """

    sql """
        CREATE VIEW ${viewName} AS
        SELECT
            CAST(e.event_value.video_id AS LARGEINT) AS video_id,
            TRY_CAST(e.event_value.duration AS INT) AS duration_ms,
            CAST(e.user_connect_info.user_client AS VARCHAR) AS client
        FROM ${tableName} e
    """

    def viewCount = sql """
        SELECT COUNT(*) FROM ${viewName}
        WHERE video_id = 100 AND duration_ms > 0 AND client = 'ios'
    """
    assertEquals("1", viewCount[0][0].toString())

    def createViewSql = sql "SHOW CREATE VIEW ${viewName}"
    assertTrue(createViewSql[0][1].contains("`event_value`.`video_id`"))
    assertTrue(createViewSql[0][1].contains("`event_value`.`duration`"))
    assertTrue(createViewSql[0][1].contains("`user_connect_info`.`user_client`"))
    assertFalse(createViewSql[0][1].contains("`event_value` AS LARGEINT"))
    assertFalse(createViewSql[0][1].contains("`event_value` AS INT"))
    assertFalse(createViewSql[0][1].contains("`user_connect_info` AS VARCHAR"))

    sql """
        ALTER VIEW ${viewName} AS
        SELECT
            CAST(e.event_value.video_id AS LARGEINT) AS video_id,
            TRY_CAST(e.event_value.duration AS INT) AS duration_ms,
            CAST(e.user_connect_info.user_client AS VARCHAR) AS client
        FROM ${tableName} e
    """

    def alterViewCount = sql """
        SELECT COUNT(*) FROM ${viewName}
        WHERE video_id = 100 AND duration_ms > 0 AND client = 'ios'
    """
    assertEquals("1", alterViewCount[0][0].toString())

    sql """
        CREATE VIEW ${bracketViewName} AS
        SELECT CAST(event_value['video_id'] AS LARGEINT) AS video_id
        FROM ${tableName}
    """

    def bracketViewCount = sql "SELECT COUNT(*) FROM ${bracketViewName} WHERE video_id = 100"
    assertEquals("1", bracketViewCount[0][0].toString())

    def bracketCreateViewSql = sql "SHOW CREATE VIEW ${bracketViewName}"
    assertTrue(bracketCreateViewSql[0][1].contains("`event_value`['video_id']"))
    assertFalse(bracketCreateViewSql[0][1].contains("['video_id']['video_id']"))

    sql "DROP VIEW IF EXISTS ${viewName}"
    sql "DROP VIEW IF EXISTS ${bracketViewName}"
    sql "DROP TABLE IF EXISTS ${tableName}"
}
