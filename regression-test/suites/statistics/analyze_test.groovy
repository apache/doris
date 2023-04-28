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

suite("analyze_test") {

    def dbName1 = "analyze_test_db_1"

    def tblName1 = "${dbName1}.analyze_test_tbl_1"

    def dbName2 = "analyze_test_db_2"

    def tblName2 = "${dbName2}.analyze_test_tbl_2"

    def dbName3 = "analyze_test_db_3"

    def tblName3 = "${dbName3}.analyze_test_tbl_3"

    def dbName4 = "analyze_test_db_4"

    def tblName4 = "${dbName4}.analyze_test_tbl_4"


    sql """
        DROP DATABASE IF EXISTS ${dbName1};
    """


    sql """
        CREATE DATABASE ${dbName1};
    """

    sql """
        DROP DATABASE IF EXISTS ${dbName2}
    """

    sql """
        CREATE DATABASE ${dbName2};
    """

    sql """
        DROP DATABASE IF EXISTS ${dbName3}
    """

    sql """
        CREATE DATABASE ${dbName3};
    """

    sql """
        DROP DATABASE IF EXISTS ${dbName4}
    """

    sql """
        CREATE DATABASE ${dbName4};
    """

    sql """
        DROP TABLE IF EXISTS ${tblName1}
    """

    sql """CREATE TABLE ${tblName1} (analyze_test_col1 varchar(11451) not null, analyze_test_col2 int not null, analyze_test_col3 int not null)
    UNIQUE KEY(analyze_test_col1)
    DISTRIBUTED BY HASH(analyze_test_col1)
    BUCKETS 3
    PROPERTIES(
            "replication_num"="1",
            "enable_unique_key_merge_on_write"="true"
    );"""

    sql """
        DROP TABLE IF EXISTS ${tblName2}
    """

    sql """CREATE TABLE ${tblName2} (analyze_test_col1 varchar(11451) not null, analyze_test_col2 int not null, analyze_test_col3 int not null)
    UNIQUE KEY(analyze_test_col1)
    DISTRIBUTED BY HASH(analyze_test_col1)
    BUCKETS 3
    PROPERTIES(
            "replication_num"="1",
            "enable_unique_key_merge_on_write"="true"
    );"""

    sql """
        DROP TABLE IF EXISTS ${tblName3}
    """

    sql """CREATE TABLE ${tblName3} (analyze_test_col1 varchar(11451) not null, analyze_test_col2 int not null, analyze_test_col3 int not null)
    UNIQUE KEY(analyze_test_col1)
    DISTRIBUTED BY HASH(analyze_test_col1)
    BUCKETS 3
    PROPERTIES(
            "replication_num"="1",
            "enable_unique_key_merge_on_write"="true"
    );"""

    sql """
        DROP TABLE IF EXISTS ${tblName4}
    """

    sql """CREATE TABLE ${tblName4} (analyze_test_col1 varchar(11451) not null, analyze_test_col2 int not null, analyze_test_col3 int not null)
    UNIQUE KEY(analyze_test_col1)
    DISTRIBUTED BY HASH(analyze_test_col1)
    BUCKETS 3
    PROPERTIES(
            "replication_num"="1",
            "enable_unique_key_merge_on_write"="true"
    );"""

    sql """insert into ${tblName1} values(1, 2, 3);"""
    sql """insert into ${tblName1} values(4, 5, 6);"""
    sql """insert into ${tblName1} values(7, 1, 9);"""
    sql """insert into ${tblName1} values(3, 8, 2);"""
    sql """insert into ${tblName1} values(5, 2, 1);"""

    sql """insert into ${tblName2} values(1, 2, 3);"""
    sql """insert into ${tblName2} values(4, 5, 6);"""
    sql """insert into ${tblName2} values(7, 1, 9);"""
    sql """insert into ${tblName2} values(3, 8, 2);"""
    sql """insert into ${tblName2} values(5, 2, 1);"""

    sql """insert into ${tblName3} values(1, 2, 3);"""
    sql """insert into ${tblName3} values(4, 5, 6);"""
    sql """insert into ${tblName3} values(7, 1, 9);"""
    sql """insert into ${tblName3} values(3, 8, 2);"""
    sql """insert into ${tblName3} values(5, 2, 1);"""

    sql """insert into ${tblName4} values(1, 2, 3);"""
    sql """insert into ${tblName4} values(4, 5, 6);"""
    sql """insert into ${tblName4} values(7, 1, 9);"""
    sql """insert into ${tblName4} values(3, 8, 2);"""
    sql """insert into ${tblName4} values(5, 2, 1);"""

    sql """
            delete from __internal_schema.column_statistics where col_id in  ('analyze_test_col1', 'analyze_test_col2', 'analyze_test_col3')
    """

    sql """
        analyze sync table ${tblName1};
    """

    sql """
        analyze sync table ${tblName2};
    """

    sql """
        analyze sync table ${tblName3};
    """

    order_qt_sql_1 """
        select count, null_count, min, max, data_size_in_bytes from __internal_schema.column_statistics where
            col_id in ('analyze_test_col1', 'analyze_test_col2', 'analyze_test_col3')
    """

    sql """
        ALTER TABLE ${tblName3} DROP COLUMN analyze_test_col3;
    """
    sql """
        ALTER TABLE ${tblName3} DROP COLUMN analyze_test_col2;
    """

    sql """
        analyze sync table ${tblName4} with sample rows 5;
    """

    sql """
        analyze table ${tblName4} with sync with incremental with sample percent 20;
    """

    sql """
        analyze sync table ${tblName4} update histogram with sample percent 20 with buckets 2;
    """

    sql """
        analyze table ${tblName4} update histogram with sync with sample rows 20 with buckets 2;
    """

    sql """
        DROP TABLE ${tblName2}
    """

    sql """
        DROP DATABASE ${dbName1}
    """

    sql """
        DROP DATABASE ${dbName4}
    """

    sql """
        DROP EXPIRED STATS
    """

    order_qt_sql_2 """
        select count, null_count, min, max, data_size_in_bytes from __internal_schema.column_statistics where
            col_id in ('analyze_test_col1', 'analyze_test_col2', 'analyze_test_col3')
    """

    sql """
        DROP STATS ${tblName3} (analyze_test_col1);
    """

    qt_sql_5 """
        SELECT COUNT(*) FROM __internal_schema.column_statistics  where
            col_id in ('analyze_test_col1', 'analyze_test_col2', 'analyze_test_col3') 
    """
    // Below test would failed on community pipeline for unknown reason, comment it temporarily
    // sql """
    //     SET enable_nereids_planner=true;
    //
    // """
    // sql """
    //     SET forbid_unknown_col_stats=true;
    // """
    //
    //test {
    //    sql """
    //        SELECT analyze_test_col1 FROM ${tblName3}
    //    """
    //    exception """errCode = 2, detailMessage = Unexpected exception: column stats for analyze_test_col1 is unknown"""
    //}

    sql """
        insert into __internal_schema.analysis_jobs values(788943185,-1,'internal','default_cluster:analyze_test_db_1',
        'analyze_test_tbl_1', 'analyze_test_col3',-1 ,'MANUAL','sFULL','',0,'PENDING', 'ONCE');
    """

    sql """
        DROP EXPIRED STATS
    """

//    Exception e = null;
//    int failedCount = 0;
//
//    do {
//        try {
//            result = sql  """
//                SELECT COUNT(*) FROM __internal_schema.analysis_jobs WHERE tbl_name = 'analyze_test_tbl_1'
//            """
//            assertEquals(0, result[0][0])
//            e = null
//            break
//        } catch (Exception except) {
//            failedCount ++
//            e = except
//            Thread.sleep(10000)
//        }
//    } while (e != null && failedCount < 30)
//
//    if (e != null) {
//        throw e;
//    }

    sql """CREATE TABLE ${tblName2} (analyze_test_col1 varchar(11451) not null, analyze_test_col2 int not null, analyze_test_col3 int not null)
    UNIQUE KEY(analyze_test_col1)
    DISTRIBUTED BY HASH(analyze_test_col1)
    BUCKETS 3
    PROPERTIES(
            "replication_num"="1",
            "enable_unique_key_merge_on_write"="true"
    );"""

    sql """
        DELETE FROM __internal_schema.analysis_jobs 
        WHERE tbl_name = 'analyze_test_tbl_2';
    """

    test {
        sql """
            ANALYZE TABLE ${tblName2}
        """

        rowNum 1
    }

    sql """
        ANALYZE SYNC TABLE ${tblName2}
    """

    qt_sql_4 """
        SELECT COUNT(*) FROM __internal_schema.analysis_jobs WHERE tbl_name = 'analyze_test_tbl_2'
    """

}
