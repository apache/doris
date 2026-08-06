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

suite("test_olap_table_stream_history_query") {
    if (isCloudMode()) {
        return
    }
    sql "DROP DATABASE IF EXISTS test_olap_table_stream_history_query_db"
    sql "CREATE DATABASE test_olap_table_stream_history_query_db"
    sql "USE test_olap_table_stream_history_query_db"

    // mow table
    sql """
        CREATE TABLE `tbl1` (
          `sid` int NULL,
          `sname` varchar(32) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`sid`)
        DISTRIBUTED BY HASH(`sid`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "binlog.enable" = "true",
        "binlog.format" = "ROW",
        "binlog.need_historical_value" = "true"
        ); 
    """
    sql """ 
        insert into tbl1 values (1, 's1'),(2, 's2'),(3, 's3')
    """

    sql """
        CREATE STREAM `s1` ON TABLE tbl1
        COMMENT 'test stream 1'
        PROPERTIES(
            'show_initial_rows' = 'true'
        );
    """

    // duplicate table
    sql """
        CREATE TABLE `tbl2` (
          `sid` int NULL,
          `sname` varchar(32) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`sid`)
        DISTRIBUTED BY HASH(`sid`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "binlog.enable" = "true",
        "binlog.format" = "ROW"
        ); 
    """

    sql """ 
        insert into tbl2 values (1, 's1'),(2, 's2'),(3, 's3')
    """

    sql """
        CREATE STREAM `s2` ON TABLE tbl2
        COMMENT 'test stream 3'
        PROPERTIES(
            'show_initial_rows' = 'true'
        );
    """
    // The stream sequence column of historical rows now carries the real commit tso of the
    // base table partition instead of a fixed -1. Rows inserted in one transaction share the
    // same tso, so fetch it at runtime and assert against the captured value rather than a
    // hard-coded .out, while the change type column stays the constant "APPEND".
    def fetchHistorySeq = { streamName ->
        def seqRows = sql "select distinct __DORIS_STREAM_SEQUENCE_COL__ from ${streamName}"
        assertEquals(1, seqRows.size())
        long seq = seqRows[0][0] as long
        assertTrue(seq > 0, "history stream sequence should be a positive tso but got ${seq}")
        return seq
    }

    def checkStreamHistory = { streamName ->
        long seq = fetchHistorySeq(streamName)

        // select the sequence column directly
        assertEquals([[seq], [seq], [seq]],
                sql("select __DORIS_STREAM_SEQUENCE_COL__ from ${streamName} order by sid"))

        // filter by the real sequence value
        assertEquals([[1, "s1"], [2, "s2"], [3, "s3"]],
                sql("select sid, sname from ${streamName} where __DORIS_STREAM_SEQUENCE_COL__=${seq} order by sid"))
        assertEquals([[1, "s1"], [2, "s2"], [3, "s3"]],
                sql("""select sid, sname from ${streamName} where __DORIS_STREAM_SEQUENCE_COL__=${seq}
                        order by __DORIS_STREAM_CHANGE_TYPE_COL__, sid"""))

        // change type column is still a constant, keep verifying it as before
        assertEquals([[1, "s1"], [2, "s2"], [3, "s3"]],
                sql("select sid, sname from ${streamName} where __DORIS_STREAM_CHANGE_TYPE_COL__='APPEND' order by sid"))
        assertEquals([[1, "s1"], [2, "s2"], [3, "s3"]],
                sql("""select sid, sname from ${streamName} where __DORIS_STREAM_CHANGE_TYPE_COL__='APPEND'
                        order by __DORIS_STREAM_SEQUENCE_COL__, sid"""))

        // group by sequence / change type
        assertEquals([[3L]], sql("select count(*) from ${streamName} group by __DORIS_STREAM_SEQUENCE_COL__"))
        assertEquals([[3L]], sql("select count(*) from ${streamName} group by __DORIS_STREAM_CHANGE_TYPE_COL__"))
        assertEquals([[3L]], sql("""select count(*) from ${streamName} group by __DORIS_STREAM_SEQUENCE_COL__
                        having __DORIS_STREAM_SEQUENCE_COL__=${seq}"""))
        assertEquals([[3L]], sql("""select count(*) from ${streamName} group by __DORIS_STREAM_CHANGE_TYPE_COL__
                        having __DORIS_STREAM_CHANGE_TYPE_COL__='APPEND'"""))
        assertEquals([[seq, 3L]],
                sql("select __DORIS_STREAM_SEQUENCE_COL__, count(*) from ${streamName} group by __DORIS_STREAM_SEQUENCE_COL__"))
        assertEquals([["APPEND", 3L]],
                sql("select __DORIS_STREAM_CHANGE_TYPE_COL__, count(*) from ${streamName} group by __DORIS_STREAM_CHANGE_TYPE_COL__"))
        assertEquals([[seq, 3L]],
                sql("select __DORIS_STREAM_SEQUENCE_COL__, count(*) from ${streamName} group by 1"))
        assertEquals([["APPEND", 3L]],
                sql("select __DORIS_STREAM_CHANGE_TYPE_COL__, count(*) from ${streamName} group by 1"))
    }

    checkStreamHistory("s1")
    checkStreamHistory("s2")

    sql "SET show_hidden_columns=true;"

    // verify select * exposes the hidden stream columns with the same real sequence value
    def checkStreamHistoryWithHiddenColumns = { streamName ->
        long seq = fetchHistorySeq(streamName)
        assertEquals([[1, "s1", seq, "APPEND"], [2, "s2", seq, "APPEND"], [3, "s3", seq, "APPEND"]],
                sql("select * from ${streamName} order by sid"))
    }

    checkStreamHistory("s1")
    checkStreamHistory("s2")
    checkStreamHistoryWithHiddenColumns("s1")
    checkStreamHistoryWithHiddenColumns("s2")

    sql "DROP DATABASE IF EXISTS test_olap_table_stream_history_query_db"
}
