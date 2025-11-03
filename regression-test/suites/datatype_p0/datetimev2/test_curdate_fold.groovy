
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

suite("test_curdate_fold") {
    def tbName = "test_curdate_fold"

    sql "DROP TABLE IF EXISTS test_curdate_fold"
    sql """
            CREATE TABLE IF NOT EXISTS test_curdate_fold (
                `rowid` int,
                `dt0` datetime,
                `dt1` datetime(1),
                `dt2` datetime(2),
                `dt3` datetime(3),
                `dt4` datetime(4),
                `dt5` datetime(5),
                `dt6` datetime(6)
            )
            UNIQUE KEY(`rowid`)
            DISTRIBUTED BY HASH(`rowid`) BUCKETS 5 properties("replication_num" = "1");
        """
    sql """
        INSERT INTO test_curdate_fold (rowid, dt0, dt1, dt2, dt3, dt4, dt5, dt6)
            SELECT 0,
                    CONCAT(CAST(DATE(CURRENT_TIMESTAMP()) AS STRING), " ", "23:59:59"),
                    CONCAT(CAST(DATE(CURRENT_TIMESTAMP()) AS STRING), " ", "23:59:59.9"),
                    CONCAT(CAST(DATE(CURRENT_TIMESTAMP()) AS STRING), " ", "23:59:59.99"),
                    CONCAT(CAST(DATE(CURRENT_TIMESTAMP()) AS STRING), " ", "23:59:59.999"),
                    CONCAT(CAST(DATE(CURRENT_TIMESTAMP()) AS STRING), " ", "23:59:59.9999"),
                    CONCAT(CAST(DATE(CURRENT_TIMESTAMP()) AS STRING), " ", "23:59:59.99999"),
                    CONCAT(CAST(DATE(CURRENT_TIMESTAMP()) AS STRING), " ", "23:59:59.999999");
    """
    sql """
        INSERT INTO test_curdate_fold (rowid, dt0, dt1, dt2, dt3, dt4, dt5, dt6)
            SELECT 1,
                    CONCAT(CAST(DATE(CURRENT_TIMESTAMP()) AS STRING), " ", "00:00:00"),
                    CONCAT(CAST(DATE(CURRENT_TIMESTAMP()) AS STRING), " ", "00:00:00.0"),
                    CONCAT(CAST(DATE(CURRENT_TIMESTAMP()) AS STRING), " ", "00:00:00.00"),
                    CONCAT(CAST(DATE(CURRENT_TIMESTAMP()) AS STRING), " ", "00:00:00.000"),
                    CONCAT(CAST(DATE(CURRENT_TIMESTAMP()) AS STRING), " ", "00:00:00.0000"),
                    CONCAT(CAST(DATE(CURRENT_TIMESTAMP()) AS STRING), " ", "00:00:00.00000"),
                    CONCAT(CAST(DATE(CURRENT_TIMESTAMP()) AS STRING), " ", "00:00:00.000000");
    """

    qt_sql0 """
        SELECT rowid FROM test_curdate_fold WHERE Date(dt0) <= curDate() ORDER BY rowid;
    """
    qt_sql1 """
        SELECT rowid FROM test_curdate_fold WHERE Date(dt1) <= curDate() ORDER BY rowid;
    """
    qt_sql2 """
        SELECT rowid FROM test_curdate_fold WHERE Date(dt2) <= curDate() ORDER BY rowid;
    """
    qt_sql3 """
        SELECT rowid FROM test_curdate_fold WHERE Date(dt3) <= curDate() ORDER BY rowid;
    """
    qt_sql4 """
        SELECT rowid FROM test_curdate_fold WHERE Date(dt4) <= curDate() ORDER BY rowid;
    """
    qt_sql5 """
        SELECT rowid FROM test_curdate_fold WHERE Date(dt5) <= curDate() ORDER BY rowid;
    """
    qt_sql6 """
        SELECT rowid FROM test_curdate_fold WHERE Date(dt6) <= curDate() ORDER BY rowid;
    """
    qt_sql7 """
        SELECT rowid FROM test_curdate_fold WHERE Date(dt0) = curDate() ORDER BY rowid;
    """
    qt_sql8 """
        SELECT rowid FROM test_curdate_fold WHERE Date(dt1) = curDate() ORDER BY rowid;
    """
    qt_sql9 """
        SELECT rowid FROM test_curdate_fold WHERE Date(dt2) = curDate() ORDER BY rowid;
    """
    qt_sql10 """
        SELECT rowid FROM test_curdate_fold WHERE Date(dt3) = curDate() ORDER BY rowid;
    """
    qt_sql11 """
        SELECT rowid FROM test_curdate_fold WHERE Date(dt4) = curDate() ORDER BY rowid;
    """
    qt_sql12 """
        SELECT rowid FROM test_curdate_fold WHERE Date(dt5) = curDate() ORDER BY rowid;
    """
    qt_sql13 """
        SELECT rowid FROM test_curdate_fold WHERE Date(dt6) = curDate() ORDER BY rowid;
    """
}
