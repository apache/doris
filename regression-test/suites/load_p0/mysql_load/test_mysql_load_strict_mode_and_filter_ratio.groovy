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

suite("test_mysql_load_strict_mode_and_filter_ratio", "p0") {
    // 1. number overflow
    def csvFile = getLoalFilePath "test_decimal_overflow.csv"
    // 1.1 strict_mode=false, load success
    sql """ DROP TABLE IF EXISTS test_mysql_load_strict_mode_and_filter_ratio"""
    sql """
    CREATE TABLE test_mysql_load_strict_mode_and_filter_ratio
    (
        k00 DECIMALV3(10,0)
    )
    PROPERTIES ("replication_num" = "1");
    """
    sql "set enable_insert_strict=true"
    sql "set enable_strict_cast=true"
    sql "set insert_max_filter_ratio=0"
    sql """
        LOAD DATA 
        LOCAL
        INFILE '${csvFile}'
        INTO TABLE test_mysql_load_strict_mode_and_filter_ratio
        COLUMNS TERMINATED BY '|'
        (k00)
        PROPERTIES ("strict_mode"="false", "max_filter_ratio"="0");
    """
    qt_sql_number_overflow_non_strict "select * from test_mysql_load_strict_mode_and_filter_ratio order by 1"

    // 1.2 strict_mode=true, max_filter_ratio=0.2, load fail
    sql "set enable_insert_strict=false"
    sql "set enable_strict_cast=false"
    sql "set insert_max_filter_ratio=1"
    sql """
        truncate TABLE test_mysql_load_strict_mode_and_filter_ratio;
    """
    test {
        sql """
            LOAD DATA 
            LOCAL
            INFILE '${csvFile}'
            INTO TABLE test_mysql_load_strict_mode_and_filter_ratio
            COLUMNS TERMINATED BY '|'
            (k00)
            PROPERTIES ("strict_mode"="true", "max_filter_ratio"="0.2");
        """
        exception "too many filtered rows"
    }
    qt_sql_number_overflow_strict0 "select * from test_mysql_load_strict_mode_and_filter_ratio order by 1"

    // 1.3 strict_mode=true, max_filter_ratio=0.3, load success
    sql "set enable_insert_strict=true"
    sql "set enable_strict_cast=true"
    sql "set insert_max_filter_ratio=0"
    sql """
        truncate TABLE test_mysql_load_strict_mode_and_filter_ratio;
    """
    sql """
        LOAD DATA 
        LOCAL
        INFILE '${csvFile}'
        INTO TABLE test_mysql_load_strict_mode_and_filter_ratio
        COLUMNS TERMINATED BY '|'
        (k00)
        PROPERTIES ("strict_mode"="true", "max_filter_ratio"="0.3");
    """
    qt_sql_number_overflow_strict1 "select * from test_mysql_load_strict_mode_and_filter_ratio order by 1"

    // 2. not number to number
    csvFile = getLoalFilePath """test_not_number.csv"""

    // 2.1 strict_mode=false, load success
    sql """ DROP TABLE IF EXISTS test_mysql_load_strict_mode_and_filter_ratio """
    sql """
    CREATE TABLE test_mysql_load_strict_mode_and_filter_ratio 
    (
        k00 DECIMALV3(10,0)
    )
    PROPERTIES ("replication_num" = "1");
    """
    sql "set enable_insert_strict=true"
    sql "set enable_strict_cast=true"
    sql "set insert_max_filter_ratio=0"
    sql """
        LOAD DATA 
        LOCAL
        INFILE '${csvFile}'
        INTO TABLE test_mysql_load_strict_mode_and_filter_ratio
        COLUMNS TERMINATED BY '|'
        (k00)
        PROPERTIES ("strict_mode"="false", "max_filter_ratio"="0");
    """
    qt_sql_not_number_non_strict "select * from test_mysql_load_strict_mode_and_filter_ratio order by 1"

    // 2.2 strict_mode=true, max_filter_ratio=0.3, load success
    sql """ truncate TABLE test_mysql_load_strict_mode_and_filter_ratio """
    sql "set enable_insert_strict=true"
    sql "set enable_strict_cast=true"
    sql "set insert_max_filter_ratio=0"
    sql """
        LOAD DATA 
        LOCAL
        INFILE '${csvFile}'
        INTO TABLE test_mysql_load_strict_mode_and_filter_ratio
        COLUMNS TERMINATED BY '|'
        (k00)
        PROPERTIES ("strict_mode"="true", "max_filter_ratio"="0.3");
    """
    qt_sql_not_number_strict0 "select * from test_mysql_load_strict_mode_and_filter_ratio order by 1"

    // 2.3 strict_mode=true, max_filter_ratio=0.2, load fail
    sql "set enable_insert_strict=false"
    sql "set enable_strict_cast=false"
    sql "set insert_max_filter_ratio=1"
    sql """ truncate TABLE test_mysql_load_strict_mode_and_filter_ratio """
    test {
        sql """
            LOAD DATA 
            LOCAL
            INFILE '${csvFile}'
            INTO TABLE test_mysql_load_strict_mode_and_filter_ratio
            COLUMNS TERMINATED BY '|'
            (k00)
            PROPERTIES ("strict_mode"="true", "max_filter_ratio"="0.2");
        """
        exception "too many filtered rows"
    }
    qt_sql_not_number_strict1 "select * from test_mysql_load_strict_mode_and_filter_ratio order by 1"

    // 3. null value to not null column
    // 3.1 strict_mode=false, max_filter_ratio=0.2, load fail
    csvFile = getLoalFilePath """test_null_number.csv"""
    sql """ drop table if exists test_mysql_load_strict_mode_and_filter_ratio"""
    sql """
    CREATE TABLE IF NOT EXISTS test_mysql_load_strict_mode_and_filter_ratio
    (
        k00 DECIMALV3(10,0) NOT NULL
    )
    PROPERTIES ("replication_num" = "1");
    """
    sql "set enable_insert_strict=false"
    sql "set enable_strict_cast=false"
    sql "set insert_max_filter_ratio=1"
    test {
        sql """
            LOAD DATA 
            LOCAL
            INFILE '${csvFile}'
            INTO TABLE test_mysql_load_strict_mode_and_filter_ratio
            COLUMNS TERMINATED BY '|'
            (k00)
            PROPERTIES ("strict_mode"="false", "max_filter_ratio"="0.2");
        """
        exception "too many filtered rows"
    }
    qt_sql_null_to_not_null_non_strict0 "select * from test_mysql_load_strict_mode_and_filter_ratio order by 1"

    // 3.2 strict_mode=false, max_filter_ratio=0.3, load success
    sql "set enable_insert_strict=true"
    sql "set enable_strict_cast=true"
    sql "set insert_max_filter_ratio=0"
    sql """ truncate table test_mysql_load_strict_mode_and_filter_ratio"""
    sql """
        LOAD DATA 
        LOCAL
        INFILE '${csvFile}'
        INTO TABLE test_mysql_load_strict_mode_and_filter_ratio
        COLUMNS TERMINATED BY '|'
        (k00)
        PROPERTIES ("strict_mode"="false", "max_filter_ratio"="0.3");
    """
    qt_sql_null_to_not_null_non_strict1 "select * from test_mysql_load_strict_mode_and_filter_ratio order by 1"

    // 3.3 strict_mode=true, max_filter_ratio=0.3, load success
    sql "set enable_insert_strict=true"
    sql "set enable_strict_cast=true"
    sql "set insert_max_filter_ratio=0"
    sql """ truncate table test_mysql_load_strict_mode_and_filter_ratio"""
    sql """
        LOAD DATA 
        LOCAL
        INFILE '${csvFile}'
        INTO TABLE test_mysql_load_strict_mode_and_filter_ratio
        COLUMNS TERMINATED BY '|'
        (k00)
        PROPERTIES ("strict_mode"="true", "max_filter_ratio"="0.3");
    """
    qt_sql_null_to_not_null_strict0 "select * from test_mysql_load_strict_mode_and_filter_ratio order by 1"

    // 3.4 strict_mode=true, max_filter_ratio=0.2, load fail
    sql """ truncate table test_mysql_load_strict_mode_and_filter_ratio"""
    test {
        sql """
            LOAD DATA 
            LOCAL
            INFILE '${csvFile}'
            INTO TABLE test_mysql_load_strict_mode_and_filter_ratio
            COLUMNS TERMINATED BY '|'
            (k00)
            PROPERTIES ("strict_mode"="true", "max_filter_ratio"="0.2");
        """
        exception "too many filtered rows"
    }
    qt_sql_null_to_not_null_strict1 "select * from test_mysql_load_strict_mode_and_filter_ratio order by 1"

    // 4. no partition
    // 4.1 no partition, strict_mode=false, max_filter_ratio=0.2, load fail
    csvFile = getLoalFilePath """test_no_partition.csv"""
    sql """ drop table if exists test_mysql_load_strict_mode_and_filter_ratio """
    sql """
        create table test_mysql_load_strict_mode_and_filter_ratio (
          id int,
          name string
        ) PARTITION BY RANGE(`id`)
          (
              PARTITION `p0` VALUES LESS THAN ("60"),
              PARTITION `p1` VALUES LESS THAN ("80")
          )
        properties (
          'replication_num' = '1'
        );
    """
    sql "set enable_insert_strict=false"
    sql "set enable_strict_cast=false"
    sql "set insert_max_filter_ratio=1"
    test {
        sql """
            LOAD DATA 
            LOCAL
            INFILE '${csvFile}'
            INTO TABLE test_mysql_load_strict_mode_and_filter_ratio
            COLUMNS TERMINATED BY '|'
            (id, name)
            PROPERTIES ("strict_mode"="false", "max_filter_ratio"="0.2");
        """
        exception "too many filtered rows"
    }
    qt_sql_no_partition_non_strict0 "select * from test_mysql_load_strict_mode_and_filter_ratio order by 1"

    // 4.2 no partition, strict_mode=false, max_filter_ratio=0.3, load success
    sql """ truncate table test_mysql_load_strict_mode_and_filter_ratio"""
    sql "set enable_insert_strict=true"
    sql "set enable_strict_cast=true"
    sql "set insert_max_filter_ratio=0"
    sql """
        LOAD DATA 
        LOCAL
        INFILE '${csvFile}'
        INTO TABLE test_mysql_load_strict_mode_and_filter_ratio
        COLUMNS TERMINATED BY '|'
        (id, name)
        PROPERTIES ("strict_mode"="false", "max_filter_ratio"="0.3");
    """
    qt_sql_no_partition_non_strict1 "select * from test_mysql_load_strict_mode_and_filter_ratio order by 1"

    // 4.3 no partition, strict_mode=true, max_filter_ratio=0.2, load fail
    sql """ truncate table test_mysql_load_strict_mode_and_filter_ratio"""
    sql "set enable_insert_strict=false"
    sql "set enable_strict_cast=false"
    sql "set insert_max_filter_ratio=1"
    test {
        sql """
            LOAD DATA 
            LOCAL
            INFILE '${csvFile}'
            INTO TABLE test_mysql_load_strict_mode_and_filter_ratio
            COLUMNS TERMINATED BY '|'
            (id, name)
            PROPERTIES ("strict_mode"="true", "max_filter_ratio"="0.2");
        """
        exception "too many filtered rows"
    }
    qt_sql_no_partition_strict0 "select * from test_mysql_load_strict_mode_and_filter_ratio order by 1"

    // 4.4 no partition, strict_mode=true, max_filter_ratio=0.3, load success
    sql """ truncate table test_mysql_load_strict_mode_and_filter_ratio"""
    sql "set enable_insert_strict=true"
    sql "set enable_strict_cast=true"
    sql "set insert_max_filter_ratio=0"
    sql """
        LOAD DATA 
        LOCAL
        INFILE '${csvFile}'
        INTO TABLE test_mysql_load_strict_mode_and_filter_ratio
        COLUMNS TERMINATED BY '|'
        (id, name)
        PROPERTIES ("strict_mode"="true", "max_filter_ratio"="0.3");
    """
    qt_sql_no_partition_strict1 "select * from test_mysql_load_strict_mode_and_filter_ratio order by 1"

    // 5. string exceed schema length
    csvFile = getLoalFilePath """test_no_partition.csv"""
    // 5.1 string exceed schema length, strict_mode=false, max_filter_ratio=0, load success
    sql """
        drop table if exists test_mysql_load_strict_mode_and_filter_ratio ;
    """
    sql """
        create table test_mysql_load_strict_mode_and_filter_ratio (
          id int,
          name char(10)
        ) properties ('replication_num' = '1');
    """
    sql "set enable_insert_strict=true"
    sql "set enable_strict_cast=true"
    sql "set insert_max_filter_ratio=0"
    sql """
        LOAD DATA 
        LOCAL
        INFILE '${csvFile}'
        INTO TABLE test_mysql_load_strict_mode_and_filter_ratio
        COLUMNS TERMINATED BY '|'
        (id, name)
        PROPERTIES ("strict_mode"="false", "max_filter_ratio"="0");
    """
    qt_sql_string_exceed_len_non_strict0 "select * from test_mysql_load_strict_mode_and_filter_ratio order by 1"

    // 5.2 string exceed schema length, strict_mode=true, max_filter_ratio=0.2, load fail
    sql """ truncate table test_mysql_load_strict_mode_and_filter_ratio"""
    sql "set enable_insert_strict=false"
    sql "set enable_strict_cast=false"
    sql "set insert_max_filter_ratio=1"
    test {
        sql """
            LOAD DATA 
            LOCAL
            INFILE '${csvFile}'
            INTO TABLE test_mysql_load_strict_mode_and_filter_ratio
            COLUMNS TERMINATED BY '|'
            (id, name)
            PROPERTIES ("strict_mode"="true", "max_filter_ratio"="0.2");
        """
        exception "too many filtered rows"
    }
    qt_sql_string_exceed_len_strict0 "select * from test_mysql_load_strict_mode_and_filter_ratio order by 1"

    // 5.3 string exceed schema length, strict_mode=true, max_filter_ratio=0.3, load success
    sql """ truncate table test_mysql_load_strict_mode_and_filter_ratio"""
    sql "set enable_insert_strict=true"
    sql "set enable_strict_cast=true"
    sql "set insert_max_filter_ratio=0"
    sql """
        LOAD DATA 
        LOCAL
        INFILE '${csvFile}'
        INTO TABLE test_mysql_load_strict_mode_and_filter_ratio
        COLUMNS TERMINATED BY '|'
        (id, name)
        PROPERTIES ("strict_mode"="true", "max_filter_ratio"="0.3");
    """
    qt_sql_string_exceed_len_strict0 "select * from test_mysql_load_strict_mode_and_filter_ratio order by 1"
}
