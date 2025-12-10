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

suite("test_insert_strict_mode_and_filter_ratio","p0") {
    // 1. number overflow
    // 1.1 number overflow, enable_insert_strict=false, insert_max_filter_ratio=0, success
    sql """ DROP TABLE IF EXISTS test_insert_strict_mode_and_filter_ratio """
    sql """
    CREATE TABLE test_insert_strict_mode_and_filter_ratio
    (
        k00 DECIMALV3(10,0)
    )
    PROPERTIES ("replication_num" = "1");
    """
    sql "set enable_insert_strict=false"
    sql "set enable_strict_cast=true"
    sql "set insert_max_filter_ratio=0"
    sql """
        INSERT INTO test_insert_strict_mode_and_filter_ratio VALUES 
            (1234567890),
            (1234567891),
            (1234567892),
            (1234567893),
            (1234567894),
            (1234567895),
            (1234567896),
            (12345678971),
            (12345678902),
            (12345678903);
    """
    qt_sql_number_overflow_non_strict "select * from test_insert_strict_mode_and_filter_ratio order by 1"

    // 1.2 number overflow, enable_insert_strict=true, insert_max_filter_ratio=1, fail
    sql """
        truncate table test_insert_strict_mode_and_filter_ratio;
    """
    sql "set enable_insert_strict=true"
    sql "set enable_strict_cast=false"
    sql "set insert_max_filter_ratio=1"
    test {
        sql """
            INSERT INTO test_insert_strict_mode_and_filter_ratio VALUES 
                (1234567890),
                (1234567891),
                (1234567892),
                (1234567893),
                (1234567894),
                (1234567895),
                (1234567896),
                (12345678971),
                (12345678902),
                (12345678903);
        """
        exception """can't cast"""
    }
    qt_sql_number_overflow_strict "select * from test_insert_strict_mode_and_filter_ratio order by 1"

    // 2. not number to number
    // 2.1 not number to number, enable_insert_strict=false, insert_max_filter_ratio=0, success
    sql """ DROP TABLE IF EXISTS test_insert_strict_mode_and_filter_ratio """
    sql """
    CREATE TABLE test_insert_strict_mode_and_filter_ratio
    (
        k00 DECIMALV3(10,0)
    )
    PROPERTIES ("replication_num" = "1");
    """
    sql "set enable_insert_strict=false"
    sql "set enable_strict_cast=true"
    sql "set insert_max_filter_ratio=0"
    sql """
        INSERT INTO test_insert_strict_mode_and_filter_ratio VALUES 
            ("1234567abc"),
            ("abc4567891"),
            ("1234567xxx"),
            (1234567893),
            (1234567894),
            (1234567895),
            (1234567896),
            (1234567897),
            (1234567890),
            (1234567890);
    """
    qt_sql_not_number_to_number_non_strict "select * from test_insert_strict_mode_and_filter_ratio order by 1"

    // 2.2 not number to number, enable_insert_strict=true, insert_max_filter_ratio=1, fail
    sql """
        truncate TABLE test_insert_strict_mode_and_filter_ratio;
    """
    sql "set enable_insert_strict=true"
    sql "set enable_strict_cast=false"
    sql "set insert_max_filter_ratio=1"
    test {
        sql """
            INSERT INTO test_insert_strict_mode_and_filter_ratio VALUES 
                ("1234567abc"),
                ("abc4567891"),
                ("1234567xxx"),
                (1234567893),
                (1234567894),
                (1234567895),
                (1234567896),
                (1234567897),
                (1234567890),
                (1234567890);
        """
        exception """can't cast"""
    }
    qt_sql_not_number_to_number_strict "select * from test_insert_strict_mode_and_filter_ratio order by 1"

    // 3. null value to not null column
    // 3.1 null value to not null column, enable_insert_strict=false, insert_max_filter_ratio=0.2, fail
    sql """
        DROP TABLE IF EXISTS test_insert_strict_mode_and_filter_ratio;
    """
    sql """
    CREATE TABLE test_insert_strict_mode_and_filter_ratio
    (
        k00 DECIMALV3(10,0) NOT NULL
    )
    PROPERTIES ("replication_num" = "1");
    """
    sql "set enable_insert_strict=false"
    sql "set enable_strict_cast=true"
    sql "set insert_max_filter_ratio=0.2"
    test {
        sql """
            INSERT INTO test_insert_strict_mode_and_filter_ratio VALUES 
                ("1234567abc"),
                ("abc4567891"),
                ("1234567xxx"),
                (1234567893),
                (1234567894),
                (1234567895),
                (1234567896),
                (1234567897),
                (1234567890),
                (1234567890);
        """
        exception """Insert has too many filtered data"""
        exception """url"""
    }
    qt_sql_not_null_to_null_non_strict0 "select * from test_insert_strict_mode_and_filter_ratio order by 1"

    sql """
    truncate TABLE test_insert_strict_mode_and_filter_ratio;
    """
    test {
        sql """
            INSERT INTO test_insert_strict_mode_and_filter_ratio VALUES 
                (NULL),
                (NULL),
                (NULL),
                (1234567893),
                (1234567894),
                (1234567895),
                (1234567896),
                (1234567897),
                (1234567890),
                (1234567890);
        """
        exception """Insert has too many filtered data"""
        exception """url"""
    }
    qt_sql_not_null_to_null_non_strict1 "select * from test_insert_strict_mode_and_filter_ratio order by 1"

    // 3.2 null value to not null column, enable_insert_strict=false, insert_max_filter_ratio=0.3, success
    sql """
        truncate TABLE test_insert_strict_mode_and_filter_ratio;
    """
    sql "set enable_insert_strict=false"
    sql "set enable_strict_cast=true"
    sql "set insert_max_filter_ratio=0.3"
    sql """
        INSERT INTO test_insert_strict_mode_and_filter_ratio VALUES 
            ("1234567abc"),
            ("abc4567891"),
            ("1234567xxx"),
            (1234567893),
            (1234567894),
            (1234567895),
            (1234567896),
            (1234567897),
            (1234567890),
            (1234567890);
    """
    qt_sql_not_null_to_null_non_strict2 "select * from test_insert_strict_mode_and_filter_ratio order by 1"

    sql """
    truncate TABLE test_insert_strict_mode_and_filter_ratio;
    """
    sql """
        INSERT INTO test_insert_strict_mode_and_filter_ratio VALUES 
            (NULL),
            (NULL),
            (NULL),
            (1234567893),
            (1234567894),
            (1234567895),
            (1234567896),
            (1234567897),
            (1234567890),
            (1234567890);
    """
    qt_sql_not_null_to_null_non_strict3 "select * from test_insert_strict_mode_and_filter_ratio order by 1"

    // 3.3 null value to not null column, enable_insert_strict=true, insert_max_filter_ratio=1, fail
    sql """
        truncate TABLE test_insert_strict_mode_and_filter_ratio;
    """
    sql "set enable_insert_strict=true"
    sql "set enable_strict_cast=false"
    sql "set insert_max_filter_ratio=1"
    test {
        sql """
            INSERT INTO test_insert_strict_mode_and_filter_ratio VALUES 
                ("1234567abc"),
                ("abc4567891"),
                ("1234567xxx"),
                (1234567893),
                (1234567894),
                (1234567895),
                (1234567896),
                (1234567897),
                (1234567890),
                (1234567890);
        """
        exception """can't cast"""
    }
    qt_sql_not_null_to_null_strict1 "select * from test_insert_strict_mode_and_filter_ratio order by 1"

    sql """
    truncate table test_insert_strict_mode_and_filter_ratio;
    """
    test {
        sql """
        INSERT INTO test_insert_strict_mode_and_filter_ratio VALUES 
            (NULL),
            (NULL),
            (NULL),
            (1234567893),
            (1234567894),
            (1234567895),
            (1234567896),
            (1234567897),
            (1234567890),
            (1234567890);
        """
        exception """Encountered unqualified data, stop processing"""
        exception """url"""
    }

    // 4. no partition
    // 4.1 no partition, enable_insert_strict=false, insert_max_filter_ratio=0.2, load fail
    sql """
        drop table if exists test_insert_strict_mode_and_filter_ratio;
    """
    sql """
        create table test_insert_strict_mode_and_filter_ratio (
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
    sql "set enable_strict_cast=true"
    sql "set insert_max_filter_ratio=0.2"
    test {
        sql """
            insert into test_insert_strict_mode_and_filter_ratio values
                (1, "a1"),
                (20, "a20"),
                (30, "a30"),
                (59, "a59"),
                (60, "a60"),
                (70, "a70"),
                (79, "a79"),
                (81, "a81"),
                (91, "a91"),
                (100, "a100");
        """
        exception """Insert has too many filtered data"""
        exception """url"""
    }
    qt_sql_no_partition_non_strict0 "select * from test_insert_strict_mode_and_filter_ratio order by 1"

    // 4.2 no partition, enable_insert_strict=false, insert_max_filter_ratio=0.3, load success
    sql """
        truncate table test_insert_strict_mode_and_filter_ratio;
    """
    sql "set enable_insert_strict=false"
    sql "set enable_strict_cast=true"
    sql "set insert_max_filter_ratio=0.3"
    sql """
        insert into test_insert_strict_mode_and_filter_ratio values
            (1, "a1"),
            (20, "a20"),
            (30, "a30"),
            (59, "a59"),
            (60, "a60"),
            (70, "a70"),
            (79, "a79"),
            (81, "a81"),
            (91, "a91"),
            (100, "a100");
    """
    qt_sql_no_partition_non_strict1 "select * from test_insert_strict_mode_and_filter_ratio order by 1"

    // 4.3 no partition, enable_insert_strict=true, insert_max_filter_ratio=1, load fail
    sql """
        truncate table test_insert_strict_mode_and_filter_ratio;
    """
    sql "set enable_insert_strict=true"
    sql "set enable_strict_cast=false"
    sql "set insert_max_filter_ratio=1"
    test {
        sql """
            insert into test_insert_strict_mode_and_filter_ratio values
                (1, "a1"),
                (20, "a20"),
                (30, "a30"),
                (59, "a59"),
                (60, "a60"),
                (70, "a70"),
                (79, "a79"),
                (81, "a81"),
                (91, "a91"),
                (100, "a100");
        """
        exception """Encountered unqualified data, stop processing"""
        exception """url"""
    }
    qt_sql_no_partition_strict0 "select * from test_insert_strict_mode_and_filter_ratio order by 1"

    // 5. string exceed schema length
    // 5.1 string exceed schema length, enable_insert_strict=false, insert_max_filter_ratio=0, load success
    sql """
        drop table if exists test_insert_strict_mode_and_filter_ratio;
    """
    sql """
        create table test_insert_strict_mode_and_filter_ratio (
          id int,
          name char(10)
        ) properties ('replication_num' = '1');
    """
    sql "set enable_insert_strict=false"
    sql "set enable_strict_cast=true"
    sql "set insert_max_filter_ratio=0"
    sql """
    insert into test_insert_strict_mode_and_filter_ratio  values
        (1, "a1"),
        (20, "a20"),
        (30, "a30"),
        (59, "a59"),
        (60, "a60"),
        (70, "a70"),
        (79, "a79"),
        (81, "a1234567890"),
        (91, "a9234567890"),
        (100, "a10234567890");
    """
    qt_sql_string_exceed_len_non_strict0 "select * from test_insert_strict_mode_and_filter_ratio order by 1"

    // 5.2 string exceed schema length, enable_insert_strict=true, insert_max_filter_ratio=1, load fail
     sql """
        truncate table test_insert_strict_mode_and_filter_ratio;
    """
    sql "set enable_insert_strict=true"
    sql "set enable_strict_cast=false"
    sql "set insert_max_filter_ratio=1"
    test {
        sql """
        insert into test_insert_strict_mode_and_filter_ratio  values
            (1, "a1"),
            (20, "a20"),
            (30, "a30"),
            (59, "a59"),
            (60, "a60"),
            (70, "a70"),
            (79, "a79"),
            (81, "a1234567890"),
            (91, "a9234567890"),
            (100, "a10234567890");
        """
        exception """Encountered unqualified data, stop processing"""
        exception """url"""
    }
    qt_sql_string_exceed_len_strict1 "select * from test_insert_strict_mode_and_filter_ratio order by 1"

    // TODO: change the following test case when BE support mbstring length check
    // 6 test Chinese char
    // 6.1 string exceed schema length, enable_insert_strict=false, insert_max_filter_ratio=0.3, load fail
    sql """
        drop table if exists test_insert_strict_mode_and_filter_ratio;
    """
    sql """
        create table test_insert_strict_mode_and_filter_ratio (
          id int,
          name char(1)
        ) properties ('replication_num' = '1');
    """
    sql "set enable_insert_strict=false"
    sql "set enable_strict_cast=true"
    sql "set insert_max_filter_ratio=0.3"
    test {
        sql """
        insert into test_insert_strict_mode_and_filter_ratio  values
            (1, "a"),
            (2, "b"),
            (3, "c"),
            (4, "d"),
            (5, "e"),
            (6, "f"),
            (7, "宅z"),
            (8, "兹z"),
            (9, "中z"),
            (10, "国g");
        """
        exception """Insert has too many filtered data"""
    }
    qt_sql_mb_string_exceed_len_non_strict0 "select * from test_insert_strict_mode_and_filter_ratio order by 1"

    // 6.2 string exceed schema length, enable_insert_strict=false, insert_max_filter_ratio=0.4, load success
    sql """
        truncate table test_insert_strict_mode_and_filter_ratio;
    """
    sql "set insert_max_filter_ratio=0.4"
    sql """
    insert into test_insert_strict_mode_and_filter_ratio  values
        (1, "a"),
        (2, "b"),
        (3, "c"),
        (4, "d"),
        (5, "e"),
        (6, "f"),
        (7, "宅z"),
        (8, "兹z"),
        (9, "中z"),
        (10, "国g");
    """
    qt_sql_mb_string_exceed_len_non_strict1 "select * from test_insert_strict_mode_and_filter_ratio order by 1"

    // 6.3 string exceed schema length, enable_insert_strict=true, insert_max_filter_ratio=1, load fail
    sql """
        truncate table test_insert_strict_mode_and_filter_ratio;
    """
    sql "set enable_insert_strict=true"
    sql "set enable_strict_cast=false"
    sql "set insert_max_filter_ratio=1"
    test {
        sql """
        insert into test_insert_strict_mode_and_filter_ratio  values
            (1, "a"),
            (2, "b"),
            (3, "c"),
            (4, "d"),
            (5, "e"),
            (6, "f"),
            (7, "宅z"),
            (8, "兹z"),
            (9, "中z"),
            (10, "国g");
        """
        exception """Insert has filtered data in strict mode"""
    }
    qt_sql_mb_string_exceed_len_strict0 "select * from test_insert_strict_mode_and_filter_ratio order by 1"
}