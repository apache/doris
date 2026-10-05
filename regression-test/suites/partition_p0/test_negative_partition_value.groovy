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

suite("test_negative_partition_value", "p0") {
    sql "drop table if exists test_negative_partition_value_range"
    sql """CREATE TABLE test_negative_partition_value_range
        (a VARCHAR(5) NOT NULL,
        b TINYINT NOT NULL)
        ENGINE=olap UNIQUE KEY(a,b)
        PARTITION BY RANGE(b) (
            PARTITION p__32 VALUES LESS THAN (-32),
            PARTITION p_0 VALUES LESS THAN (0),
            PARTITION p_16 VALUES LESS THAN (16),
            PARTITION p_32 VALUES LESS THAN (32),
            PARTITION p_64 VALUES LESS THAN (64)
        )
        DISTRIBUTED BY HASH (a) BUCKETS 1 PROPERTIES('replication_num' = '1')
    """
    sql """insert into test_negative_partition_value_range values ("1", 1), ("2", 2), ("16", 16), ("17", 17), ("32", 32), ("33", 33), ("63", 63), ("-1", -1), ("-32", -32), ("-33", -33) """
    qt_range1 "select * from test_negative_partition_value_range order by b"
    qt_range2 "select * from test_negative_partition_value_range partition(p__32) order by b"
    qt_range3 "select * from test_negative_partition_value_range partition(p_0) order by b"
    qt_range4 "select * from test_negative_partition_value_range partition(p_16) order by b"
    qt_range5 "select * from test_negative_partition_value_range partition(p_32) order by b"
    qt_range6 "select * from test_negative_partition_value_range partition(p_64) order by b"


    sql "drop table if exists test_negative_partition_value_list"
    sql """CREATE TABLE test_negative_partition_value_list
        (a VARCHAR(5) NOT NULL,
        b TINYINT NOT NULL)
        ENGINE=olap
        UNIQUE KEY(a,b)
        PARTITION BY LIST(b) (
            PARTITION p1 VALUES IN ("1","2","3","4"),
            PARTITION p2 VALUES IN ("-1","-2","-3","-4")
        )
        DISTRIBUTED BY HASH (a) BUCKETS 1 PROPERTIES('replication_num' = '1')
    """
    sql """insert into test_negative_partition_value_list values ("1", 1), ("2", 2), ("-1", -1)"""
    qt_list1 "select * from test_negative_partition_value_list order by b"
    qt_list2 "select * from test_negative_partition_value_list partition(p1) order by b"
    qt_list3 "select * from test_negative_partition_value_list partition(p2) order by b"
}
