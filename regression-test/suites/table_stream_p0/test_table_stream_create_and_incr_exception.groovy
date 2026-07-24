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

// Cover CREATE STREAM property validation, SHOW CREATE STREAM, and @incr
// parameter validation that is implemented by the current planner.
suite("test_table_stream_create_and_incr_exception", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    sql "DROP DATABASE IF EXISTS test_table_stream_create_and_incr_exception_db"
    sql "CREATE DATABASE test_table_stream_create_and_incr_exception_db"
    sql "USE test_table_stream_create_and_incr_exception_db"

    sql """
        CREATE TABLE create_incr_base (
            id INT,
            value INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """

    test {
        sql """
            CREATE STREAM create_incr_bad_type ON TABLE create_incr_base
            PROPERTIES ("type" = "unsupported_type")
        """
        exception "not supported type"
    }

    test {
        sql """
            CREATE STREAM create_incr_unknown_property ON TABLE create_incr_base
            PROPERTIES (
                "type" = "append_only",
                "unknown_property" = "value"
            )
        """
        exception "Unknown properties"
    }

    test {
        sql """
            CREATE STREAM create_incr_base ON TABLE create_incr_base
            PROPERTIES ("type" = "append_only")
        """
        exception "already exists"
    }

    sql """
        CREATE STREAM create_incr_stream ON TABLE create_incr_base
        COMMENT 'create and incr validation'
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "false"
        )
    """

    def showCreateRows = sql "SHOW CREATE STREAM create_incr_stream"
    assertEquals(1, showCreateRows.size())
    assertEquals("create_incr_stream", showCreateRows[0][0].toString())
    def createStatement = showCreateRows[0][1].toString()
    assertTrue(createStatement.contains("ON TABLE internal.test_table_stream_create_and_incr_exception_db.create_incr_base"))
    assertTrue(createStatement.contains("COMMENT 'create and incr validation'"))
    assertTrue(createStatement.contains("\"type\" = \"APPEND_ONLY\""))
    assertTrue(createStatement.contains("\"show_initial_rows\" = \"false\""))

    test {
        sql """
            CREATE OR REPLACE STREAM create_incr_stream ON TABLE create_incr_base
            PROPERTIES ("type" = "append_only")
        """
        exception "do not support replace currently"
    }

    sql "INSERT INTO create_incr_base VALUES (1, 10)"
    sql "sync"
    sleep(1200)

    test {
        sql """
            SELECT id FROM create_incr_base@incr(
                "incrementType" = "unsupported_type"
            )
        """
        exception "Unsupported parameter in incr query"
    }

    test {
        sql """
            SELECT id FROM create_incr_base@incr(
                "incrementType" = "DETAIL",
                "unknown_property" = "value"
            )
        """
        exception "Unsupported parameter in incr query"
    }

    test {
        sql """
            SELECT id FROM create_incr_base@incr(
                "incrementType" = "DETAIL",
                "startTimestamp" = "not-a-timestamp"
            )
        """
        exception "Invalid TIMESTAMP format in incr clause"
    }

    test {
        sql "SELECT id FROM create_incr_base@snapshot()"
        exception "Invalid param type for olap table"
    }

    test {
        sql "SELECT id FROM create_incr_stream@incr()"
        exception "Invalid param type for olap table stream"
    }
}
