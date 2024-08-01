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

suite("test_list_partition_datatype", "p0") {
    def aggCol = """
        k0 boolean NOT NULL,
        k1 tinyint NOT NULL, 
        k2 smallint NOT NULL, 
        k3 int NOT NULL, 
        k4 bigint NOT NULL, 
        k13 largeint NOT NULL,
        k5 decimal(9, 3) NOT NULL, 
        k6 char(5) NOT NULL, 
        k10 date NOT NULL, 
        k11 datetime NOT NULL, 
        k7 varchar(20) NOT NULL, 
        k8 double max NOT NULL, 
        k9 float sum NOT NULL,
        k12 string replace NOT NULL
        """
    def nonAggCol = """
        k0 boolean NOT NULL,
        k1 tinyint NOT NULL, 
        k2 smallint NOT NULL, 
        k3 int NOT NULL, 
        k4 bigint NOT NULL, 
        k13 largeint NOT NULL,
        k5 decimal(9, 3) NOT NULL, 
        k6 char(5) NOT NULL, 
        k10 date NOT NULL, 
        k11 datetime NOT NULL, 
        k7 varchar(20) NOT NULL, 
        k8 double NOT NULL, 
        k9 float NOT NULL,
        k12 string NOT NULL
        """
    def dupKey = "DUPLICATE KEY(k0,k1,k2,k3,k4,k13,k5,k6,k10,k11,k7) "
    def uniqKey = "UNIQUE KEY(k0,k1,k2,k3,k4,k13,k5,k6,k10,k11,k7) "
    sql "set enable_insert_strict=true"
    def initTableAndCheck = { String tableName, String column, String keyDesc, String partitionInfo /* param */ ->
        sql """DROP TABLE IF EXISTS ${tableName}"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (${column}) ${keyDesc} ${partitionInfo} 
            DISTRIBUTED BY HASH(k1) BUCKETS 5
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        streamLoad {
            table tableName
            set "column_separator", ","
            set "columns", "k0, k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, k9, k12, k13"
            set "max_filter_ratio", "0.1"
            file "../../query_p0/baseall.txt"
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(15, json.NumberLoadedRows)
            }
        }
        sql """sync"""
        test {
            sql "SELECT * FROM ${tableName} ORDER BY k1, k2"
            resultFile "expect_baseall.out"
        }
        def result = sql "SHOW PARTITIONS FROM ${tableName}"
        assertTrue(result.size() > 1)
        try_sql """DROP TABLE ${tableName}"""
    }
    def tblTypesCheck = { String tableName, String partitionInfo /* param */ ->
        initTableAndCheck("${tableName}_agg", aggCol, '', partitionInfo)
        initTableAndCheck("${tableName}_dup", nonAggCol, dupKey, partitionInfo)
        initTableAndCheck("${tableName}_uniq", nonAggCol, uniqKey, partitionInfo)
    }
    // aggregate/duplicate/unique table with tinyint list partition key, check create, load and select
    tblTypesCheck("test_list_partition_tinyint",
            """
            PARTITION BY LIST(k1) ( 
              PARTITION p1 VALUES IN ("1","2","3","4"), 
              PARTITION p2 VALUES IN ("5","6","7","8","9","10","11","12","13","14"), 
              PARTITION p3 VALUES IN ("15") )
            """)
    // aggregate/duplicate/unique table with smallint list partition key, check create, load and select
    tblTypesCheck("test_list_partition_smallint",
            """
            PARTITION BY LIST(k2) ( 
              PARTITION p1 VALUES IN ("-32767","32767"), 
              PARTITION p2 VALUES IN ("255"), PARTITION 
              p3 VALUES IN ("1985","1986","1989","1991","1992") )
            """)
    // aggregate/duplicate/unique table with int list partition key, check create, load and select
    tblTypesCheck("test_list_partition_int",
            """
            PARTITION BY LIST(k3) ( 
              PARTITION p1 VALUES IN ("-2147483647","2147483647"), 
              PARTITION p2 VALUES IN ("1001","3021"), 
              PARTITION p3 VALUES IN ("1002","25699","103","5014","1992") )
            """)
    // aggregate/duplicate/unique table with bigint list partition key, check create, load and select
    tblTypesCheck("test_list_partition_bigint",
            """
            PARTITION BY LIST(k4) ( 
              PARTITION p1 VALUES IN ("11011905","11011903"), 
              PARTITION p2 VALUES IN ("-11011903","11011920","-11011907"), 
              PARTITION p3 VALUES IN ("9223372036854775807","-9223372036854775807","11011902","7210457","123456") )
            """)
    // aggregate/duplicate/unique table with largeint list partition key, check create, load and select
    tblTypesCheck("test_list_partition_largeint",
            """
            PARTITION BY LIST(k13) ( 
              PARTITION p1 VALUES IN ("-170141183460469231731687303715884105727", "-20220101", "-11011903", "-2022"), 
              PARTITION p2 VALUES IN ("0", "11011903", "20220101", "20220102", "20220104", "701411834604692317"), 
              PARTITION p3 VALUES IN ("701411834604692317316873", "1701604692317316873037158", "701411834604692317316873037158"),
              PARTITION p4 VALUES IN ("1701411834604692317316873037158", "170141183460469231731687303715884105727"))
            """)
    // aggregate/duplicate/unique table with date list partition key, check create, load and select
    tblTypesCheck("test_list_partition_date",
            """
            PARTITION BY LIST(k10) ( 
              PARTITION p1 VALUES IN ("1901-12-31","1988-03-21"), 
              PARTITION p2 VALUES IN ("1989-03-21","1991-08-11","2012-03-14"), 
              PARTITION p3 VALUES IN ("2014-11-11","2015-01-01","2015-04-02","3124-10-10","9999-12-12") )
            """)
    // aggregate/duplicate/unique table with datetime list partition key, check create, load and select
    tblTypesCheck("test_list_partition_datetime",
            """
            PARTITION BY LIST(k11) ( 
              PARTITION p1 VALUES IN ("1901-01-01 00:00:00","1989-03-21 13:00:00","1989-03-21 13:11:00"), 
              PARTITION p2 VALUES IN ("2000-01-01 00:00:00","2013-04-02 15:16:52","2015-03-13 10:30:00"), 
              PARTITION p3 VALUES IN ("2015-03-13 12:36:38","2015-04-02 00:00:00","9999-11-11 12:12:00") )
            """)
    // aggregate/duplicate/unique table with char list partition key, check create, load and select
    tblTypesCheck("test_list_partition_char",
            """
            PARTITION BY LIST(k6) ( 
              PARTITION p1 VALUES IN ("true"), 
              PARTITION p2 VALUES IN ("false"), 
              PARTITION p3 VALUES IN ("") )
            """)
    // aggregate/duplicate/unique table with varchar list partition key, check create, load and select
    tblTypesCheck("test_list_partition_varchar",
            """
            PARTITION BY LIST(k7) ( 
              PARTITION p1 VALUES IN (""," "), 
              PARTITION p2 VALUES IN ("du3lnvl","jiw3n4","lifsno","wangjuoo4","wangjuoo5"), 
              PARTITION p3 VALUES IN ("wangynnsf","wenlsfnl","yanavnd","yanvjldjlll","yunlj8@nk") )
            """)
    // aggregate/duplicate/unique table with boolean list partition key, check create, load and select
    tblTypesCheck("test_list_partition_boolean",
            """
            PARTITION BY LIST(k0) (               
              PARTITION p1 VALUES IN ("true"), 
              PARTITION p2 VALUES IN ("false"))
            """)
    // not support decimal as list partition key
    test {
        sql "CREATE TABLE test_list_partition_err_tbl_1 ( k1 tinyint NOT NULL, k5 decimal(9, 3) NOT NULL, " +
                "k8 double max NOT NULL, k9 float sum NOT NULL ) " +
                "PARTITION BY LIST(k5) ( " +
                " PARTITION p1 VALUES IN (\"-654.654\")," +
                " PARTITION p2 VALUES IN (\"0\"), " +
                " PARTITION p3 VALUES IN (\"243243.325\") ) " +
                "DISTRIBUTED BY HASH(k1) BUCKETS 5"
        exception "Column[k5] type[DECIMAL32] cannot be a list partition key"
    }
    // aggregate/duplicate/unique table with multi column list partition keys, check create, load and select
    tblTypesCheck(
            "test_list_partition_basic_tbl",
            """
            PARTITION BY LIST(k2,k10,k7) ( 
              PARTITION p1 VALUES IN (("-32767","1988-03-21","jiw3n4"),("-32767","2015-04-02","wenlsfnl"),
                ("255","1989-03-21","wangjuoo5"),("255","2015-04-02"," "),("1985","2015-01-01","du3lnvl")), 
              PARTITION p2 VALUES IN (("1986","1901-12-31","wangynnsf"),("1989","1989-03-21","wangjuoo4"),
                ("1989","2012-03-14","yunlj8@nk"),("1989","2015-04-02","yunlj8@nk"),("1991","1991-08-11","wangjuoo4")), 
              PARTITION p3 VALUES IN (("1991","2015-04-02","wangynnsf"),("1991","3124-10-10","yanvjldjlll"),
                ("1992","9999-12-12",""),("32767","1991-08-11","lifsno"),("32767","2014-11-11","yanavnd")) )
            """
    )
    // int list partition errors like: duplicate key, conflict, invalid format
    test {
        sql """CREATE TABLE test_list_partition_err_tbl_1 ( 
                 k1 INT NOT NULL, 
                 v1 INT SUM NOT NULL, 
                 v2 INT MAX NOT NULL, 
                 v3 INT MIN NOT NULL, 
                 v4 INT REPLACE NOT NULL ) 
               AGGREGATE KEY(k1) 
               PARTITION BY LIST(k1) ( 
                 PARTITION p1 VALUES IN ("1","2","3"), 
                 PARTITION p2 VALUES IN ("10","10"), 
                 PARTITION p3 VALUES IN ("100"), 
                 PARTITION p4 VALUES IN ("-100","0") ) 
               DISTRIBUTED BY HASH(k1) BUCKETS 5
               PROPERTIES ("replication_allocation" = "tag.location.default: 1")"""
        exception "The partition key[('10','10')] has duplicate item [(\"10\")]."
    }
    test {
        sql """
            CREATE TABLE test_list_partition_err_tbl_2 ( 
              k1 INT NOT NULL, 
              v1 INT SUM NOT NULL, 
              v2 INT MAX NOT NULL, 
              v3 INT MIN NOT NULL, 
              v4 INT REPLACE NOT NULL ) 
            AGGREGATE KEY(k1) 
            PARTITION BY LIST(k1) ( 
              PARTITION p1 VALUES IN ("1","2","3"), 
              PARTITION p2 VALUES IN ("10"), 
              PARTITION p3 VALUES IN ("100","-0"), 
              PARTITION p4 VALUES IN ("-100","0") ) 
            DISTRIBUTED BY HASH(k1) BUCKETS 5
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
            """
        exception """ Invalid list value format: errCode = 2, detailMessage = The partition key[("0")] in partition item[('-100','0')] is conflict with current partitionKeys[(("100"),("0"))]"""
    }
    test {
        sql """
            CREATE TABLE test_list_partition_err_tbl_3 ( 
              k1 INT NOT NULL, 
              v1 INT SUM NOT NULL, 
              v2 INT MAX NOT NULL, 
              v3 INT MIN NOT NULL, 
              v4 INT REPLACE NOT NULL ) 
            AGGREGATE KEY(k1) 
            PARTITION BY LIST(k1) ( 
              PARTITION p1 VALUES IN ("1","2","3"), 
              PARTITION p2 VALUES IN ("10"), 
              PARTITION p3 VALUES IN ("100","-10","NULL"), 
              PARTITION p4 VALUES IN ("-100","0") ) 
            DISTRIBUTED BY HASH(k1) BUCKETS 5
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")"""
        exception "Invalid list value format: errCode = 2, detailMessage = Invalid number format: NULL"
    }
    test {
        sql """
        CREATE TABLE test_list_partition_err_tbl_4 ( 
          k1 INT NOT NULL, 
          v1 INT SUM NOT NULL, 
          v2 INT MAX NOT NULL, 
          v3 INT MIN NOT NULL, 
          v4 INT REPLACE NOT NULL )
        AGGREGATE KEY(k1) 
        PARTITION BY LIST(k1) ( 
          PARTITION p1 VALUES IN ("1","2","3"), 
          PARTITION p2 VALUES IN ("10"), 
          PARTITION p3 VALUES IN ("100","-10","2147483648"), 
          PARTITION p4 VALUES IN ("-100","0") ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        exception "Invalid list value format: errCode = 2, detailMessage = Number out of range[2147483648]. type: int"
    }
    test {
        sql """
            CREATE TABLE test_list_partition_err_tbl_5 ( 
              k1 INT NOT NULL, 
              v1 INT SUM NOT NULL, 
              v2 INT MAX NOT NULL, 
              v3 INT MIN NOT NULL, 
              v4 INT REPLACE NOT NULL ) 
            AGGREGATE KEY(k1) 
            PARTITION BY LIST(k1) ( 
              PARTITION p1 VALUES IN ("1","2","3"), 
              PARTITION p2 VALUES IN ("10"), 
              PARTITION p3 VALUES IN ("100","-10.01"), 
              PARTITION p4 VALUES IN ("-100","0") ) 
            DISTRIBUTED BY HASH(k1) BUCKETS 5
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
            """
        exception "Invalid list value format: errCode = 2, detailMessage = Invalid number format: -10.01"
    }
    // date/datetime list partition errors like: conflict, invalid format
    test {
        sql """
        CREATE TABLE test_list_partition_err_tbl_7 ( 
          k1 DATE NOT NULL, 
          v1 DATE REPLACE NOT NULL, 
          v2 DATE MAX NOT NULL, 
          v3 DATE MIN NOT NULL ) 
        AGGREGATE KEY(k1) 
        PARTITION BY LIST(k1) ( 
          PARTITION p1 VALUES IN ("1990-01-01","2000-01-01"), 
          PARTITION p2 VALUES IN ("2000-01-01") ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        exception "Invalid list value format: errCode = 2, detailMessage = The partition key[('2000-01-01')] " +
                "in partition item[('2000-01-01')] is conflict with current partitionKeys[(('1990-01-01'),('2000-01-01'))]"
    }
    test {
        sql """
        CREATE TABLE test_list_partition_err_tbl_8 ( 
          k1 DATE NOT NULL, 
          v1 DATE REPLACE NOT NULL, 
          v2 DATE MAX NOT NULL, 
          v3 DATE MIN NOT NULL ) 
        AGGREGATE KEY(k1) 
        PARTITION BY LIST(k1) ( 
          PARTITION p1 VALUES IN ("1990-01-01","2000-01-01"), 
          PARTITION p2 VALUES IN ("2000-01-02 08:00:00") ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        exception "Invalid list value format: errCode = 2, detailMessage = date literal [2000-01-02 08:00:00] " +
                "is invalid: errCode = 2, detailMessage = Invalid date value: 2000-01-02 08:00:00"
    }
    test {
        sql """
        CREATE TABLE test_list_partition_err_tbl_9 ( 
          k1 DATETIME NOT NULL, 
          v1 DATETIME REPLACE NOT NULL, 
          v2 DATETIME MAX NOT NULL, 
          v3 DATETIME MIN NOT NULL ) 
        AGGREGATE KEY(k1) 
        PARTITION BY LIST(k1) ( 
          PARTITION p1 VALUES IN ("1990-01-01","2000-01-01"), 
          PARTITION p2 VALUES IN ("2000-01-02 08:00:00") ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        exception "Invalid list value format: errCode = 2, detailMessage = date literal [1990-01-01] is invalid: " +
                "errCode = 2, detailMessage = Invalid datetime value: 1990-01-01"
    }
    // date boundary value & format test
    sql "DROP TABLE IF EXISTS test_list_partition_ddl_tbl_1"
    sql "DROP TABLE IF EXISTS test_list_partition_ddl_tbl_2"
    sql """
        CREATE TABLE test_list_partition_ddl_tbl_1 ( 
          k1 DATE NOT NULL, 
          v1 DATE REPLACE NOT NULL ) 
        AGGREGATE KEY(k1) PARTITION BY LIST(k1) ( 
          PARTITION p1 VALUES IN ("0000-01-01","2000-01-01"), 
          PARTITION p2 VALUES IN ("20001102"), 
          PARTITION p3 VALUES IN ("9999-12-31") ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
    sql """INSERT INTO test_list_partition_ddl_tbl_1 VALUES("0000-01-01", "0000-01-01"), ("9999-12-31", "9999-12-31")"""
    def exception_str = isGroupCommitMode() ? "too many filtered rows" : "Insert has filtered data in strict mode"
    test {
        sql """INSERT INTO test_list_partition_ddl_tbl_1 VALUES("2000-01-02", "2000-01-03")"""
        exception exception_str
    }
    qt_sql1 "SELECT * FROM test_list_partition_ddl_tbl_1 order by k1"
    sql """INSERT INTO test_list_partition_ddl_tbl_1 VALUES("2000-11-02", "2000-11-03")"""
    qt_sql2 "SELECT * FROM test_list_partition_ddl_tbl_1 order by k1"

    test {
        sql """
        CREATE TABLE test_list_partition_err_tbl_10 ( 
          k1 DATE NOT NULL, 
          v1 DATE REPLACE NOT NULL, 
          v2 DATE MAX NOT NULL, 
          v3 DATE MIN NOT NULL ) 
        AGGREGATE KEY(k1) 
        PARTITION BY LIST(k1) ( 
          PARTITION p1 VALUES IN ("1990-01-01","2000-01-41"), 
          PARTITION p2 VALUES IN ("0000-01-01") ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        exception "Invalid list value format: errCode = 2, detailMessage = date literal [2000-01-41] is invalid: " +
                "Text '2000-01-41' could not be parsed"
    }
    test {
        sql """
        CREATE TABLE test_list_partition_err_tbl_11 ( 
          k1 DATE NOT NULL, 
          v1 DATE REPLACE NOT NULL, 
          v2 DATE MAX NOT NULL, 
          v3 DATE MIN NOT NULL ) 
        AGGREGATE KEY(k1) 
        PARTITION BY LIST(k1) ( 
          PARTITION p1 VALUES IN ("1990-01-01","2000-01-01"), 
          PARTITION p2 VALUES IN ("10000-01-01") ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        exception "Invalid list value format: errCode = 2, detailMessage = date literal [10000-01-01] is invalid: " +
                "errCode = 2, detailMessage = Datetime value is out of range"
    }
    test {
        sql """
        CREATE TABLE test_list_partition_err_tbl_12 ( 
          k1 DATE NOT NULL, 
          v1 DATE REPLACE NOT NULL, 
          v2 DATE MAX NOT NULL, 
          v3 DATE MIN NOT NULL ) 
        AGGREGATE KEY(k1) 
        PARTITION BY LIST(k1) ( 
          PARTITION p1 VALUES IN ("1990-01-01","2000-01-01"), 
          PARTITION p2 VALUES IN ("00-01-01") ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        exception "Invalid list value format: errCode = 2, detailMessage = The partition key[('2000-01-01')] " +
                "in partition item[('00-01-01')] is conflict with current partitionKeys[(('1990-01-01'),('2000-01-01'))]"
    }
    test {
        sql """
        CREATE TABLE test_list_partition_err_tbl_13 ( 
          k1 DATE NOT NULL, 
          v1 DATE REPLACE NOT NULL, 
          v2 DATE MAX NOT NULL, 
          v3 DATE MIN NOT NULL ) 
        AGGREGATE KEY(k1) 
        PARTITION BY LIST(k1) ( 
          PARTITION p1 VALUES IN ("1990-01-01","2000-01-01"), 
          PARTITION p2 VALUES IN ("2000-1-1") ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        exception "The partition key[('2000-01-01')] in partition item[('2000-1-1')] is conflict with current " +
                "partitionKeys[(('1990-01-01'),('2000-01-01'))]"
    }
    // 分区列的值溢出，string，前缀匹配，empty等
    // string list partition errors like: duplicate key, conflict, invalid format
    test {
        sql """
        CREATE TABLE test_list_partition_err_tbl_14 ( 
          k1 CHAR NOT NULL, v1 CHAR REPLACE NOT NULL ) 
        AGGREGATE KEY(k1) 
        PARTITION BY LIST(k1) ( 
          PARTITION p1 VALUES IN ("a","b"), 
          PARTITION p2 VALUES IN ("c"," ","?","a") ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        exception "is conflict with current partitionKeys"
    }
    sql "DROP TABLE IF EXISTS test_list_partition_tb2_char"
    sql """
        CREATE TABLE test_list_partition_tb2_char (k1 CHAR(5) NOT NULL, v1 CHAR(5) REPLACE NOT NULL ) 
        AGGREGATE KEY(k1) 
        PARTITION BY LIST(k1) ( PARTITION p1 VALUES IN ("a","b"), PARTITION p2 VALUES IN ("c"," ","?","ddd") ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5 PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
    test {
        sql """insert into test_list_partition_tb2_char values('d', '1')"""
        exception exception_str
    }
    sql """alter table test_list_partition_tb2_char add partition partition_add_1 values in ("aaa","bbb")"""
    def ret = sql "show partitions from test_list_partition_tb2_char where PartitionName='partition_add_1'"
    assertTrue(ret.size() == 1)

    test {
        sql """ insert into test_list_partition_tb2_char values('aa', '1')"""
        exception exception_str
    }
    sql "insert into test_list_partition_tb2_char values('a', 'a')"
    sql "insert into test_list_partition_tb2_char values('aaa', 'a')"
    sql "insert into test_list_partition_tb2_char values('aaa', 'b')"
    sql "insert into test_list_partition_tb2_char values('?', 'a')"
    qt_sql3 "select * from test_list_partition_tb2_char order by k1"

    sql "DROP TABLE IF EXISTS test_list_partition_empty_tb"
    sql """
        CREATE TABLE test_list_partition_empty_tb ( 
          k1 char(5) NOT NULL, 
          v1 char(5) REPLACE NOT NULL ) 
        AGGREGATE KEY(k1) 
        PARTITION BY LIST(k1) ( ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5 properties ("replication_allocation" = "tag.location.default: 1")
        """
    ret = sql "show partitions from test_list_partition_empty_tb"
    assertTrue(ret.size() == 0)
    sql """alter table test_list_partition_empty_tb add partition partition_add_2 values in ("ccc"," ","?","aaaa")"""
    ret = sql "show partitions from test_list_partition_empty_tb"
    assertTrue(ret.size() == 1)
    sql "insert into test_list_partition_empty_tb values (' ', 'empty' ), ('ccc', '3c'), ('aaaa', '4a')"
    qt_sql4 "select * from test_list_partition_empty_tb order by k1"

    // test add partition
    sql "DROP TABLE IF EXISTS test_list_partition_tb3_char"
    sql """
        CREATE TABLE test_list_partition_tb3_char (k1 CHAR NOT NULL, v1 CHAR REPLACE NOT NULL ) 
        AGGREGATE KEY(k1) 
        PARTITION BY LIST(k1) ( PARTITION p1 VALUES IN ("a","b"), PARTITION p2 VALUES IN ("c"," ","?","ddd") ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5 PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
    // todo: char(1) add partition value "aaa","bbb", should failed?
    sql """alter table test_list_partition_tb3_char add partition partition_add_1 values in ("aaa","bbb")"""
    ret = sql "show partitions from test_list_partition_tb3_char where PartitionName='partition_add_1'"
    assertTrue(ret.size() == 1)
    try_sql "DROP TABLE IF EXISTS test_list_partition_ddl_tbl_1"
    try_sql "DROP TABLE IF EXISTS test_list_partition_empty_tb"
    try_sql "DROP TABLE IF EXISTS test_list_partition_tb2_char"
    // try_sql "DROP TABLE IF EXISTS test_list_partition_tb3_char"
}
