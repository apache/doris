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

import java.math.BigDecimal;

suite("test_point_IN_query") {
    def tableName = "in_table_1"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    // Case 1: Default partitioning, part of the primary key is a bucket column
    sql """
            create table ${tableName} (
                a int not null,
                b int not null,
                c string not null
            )
            unique key(a, b)
            distributed by hash(a) buckets 16
            PROPERTIES(
                "replication_num" = "1",
                "store_row_column" = "true"
            );
        """
    sql """
            insert into ${tableName} values(123, 132, "a");
            insert into ${tableName} values(123, 222, "b");
            insert into ${tableName} values(22, 2, "c");
            insert into ${tableName} values(1, 1, "d");
            insert into ${tableName} values(2, 2, "e");
            insert into ${tableName} values(3, 3, "f");
            insert into ${tableName} values(4, 4, "i");
        """
    qt_case_1_sql "select * from ${tableName} where a = 123 and b = 132;"
    explain {
        sql("select * from ${tableName} where a = 123 and b in (132, 1, 222, 333);")
        contains "SHORT-CIRCUIT"
    }
    order_qt_case_1_sql "select * from ${tableName} where a = 123 and b in (132, 1, 222, 333);"
    explain {
        sql("select * from ${tableName} where a in (123, 1, 222) and b in (132, 1, 222, 333);")
        contains "SHORT-CIRCUIT"
    }
    order_qt_case_1_sql "select * from ${tableName} where a in (123, 1, 222) and b in (132, 1, 222, 333);"

    sql """DROP TABLE IF EXISTS ${tableName}"""

    // Case 2: Partition columns, bucket columns, and primary keys are the same
    tableName = "in_table_2"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
            create table ${tableName} (
                a int not null,
                b int not null,
                c string not null
            )
            unique key(a, b)
            partition by RANGE(a, b)
            (
                partition p0 values [(100, 100), (200, 140)),
                partition p1 values [(200, 140), (300, 170)),
                partition p2 values [(300, 170), (400, 250)),
                partition p3 values [(400, 250), (420, 300)),
                partition p4 values [(420, 300), (500, 400))
            )
            distributed by hash(a, b) buckets 16
            PROPERTIES(
                "replication_num" = "1",
                "store_row_column" = "true"
            );
            """
    sql """
            insert into ${tableName} values(123, 120, "a");
            insert into ${tableName} values(150, 120, "b");
            insert into ${tableName} values(222, 150, "c");
            insert into ${tableName} values(333, 200, "e");
            insert into ${tableName} values(400, 260, "f");
            insert into ${tableName} values(400, 250, "g");
            insert into ${tableName} values(440, 350, "h");
            insert into ${tableName} values(450, 320, "i");
        """

    qt_case_2_sql "select * from ${tableName} where a = 123 and b = 100;"
    qt_case_2_sql "select * from ${tableName} where a = 222 and b = 150;"
    order_qt_case_2_sql "select * from ${tableName} where a = 123 and b in (132, 120, 222, 333);"
    order_qt_case_2_sql "select * from ${tableName} where a = 400 and b in (260, 250, 300);"
    order_qt_case_2_sql "select * from ${tableName} where a in (400, 222, 100) and b in (260, 250, 100, 150);"

    sql """DROP TABLE IF EXISTS ${tableName}"""

    tableName = "in_table_3"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    // Case 3: The partition column is the same as the primary key, and the bucket column is part of the primary key.
    sql """    
            create table ${tableName} (
                a int not null,
                b int not null,
                c string not null
            )
            unique key(a, b)
            partition by RANGE(a, b)
            (
                partition p0 values [(100, 100), (100, 140)),
                partition p1 values [(100, 140), (200, 140)),
                partition p2 values [(200, 140), (300, 170)),
                partition p3 values [(300, 170), (400, 250)),
                partition p4 values [(400, 250), (420, 300)),
                partition p5 values [(420, 300), (500, 400))
            )
            distributed by hash(a)
            buckets 16
            PROPERTIES(
                "replication_num" = "1",
                "store_row_column" = "true"
            );
        """
    sql """
            insert into ${tableName} values(100, 100, "aaa");
            insert into ${tableName} values(100, 120, "aaaaa");
            insert into ${tableName} values(123, 100, "a");
            insert into ${tableName} values(150, 100, "b");
            insert into ${tableName} values(350, 200, "c");
            insert into ${tableName} values(400, 250, "d");
            insert into ${tableName} values(400, 280, "e");
            insert into ${tableName} values(450, 350, "f");
        """
    qt_case_3_sql "select * from ${tableName} where a = 123 and b = 100;"
    qt_case_3_sql "select * from ${tableName} where a = 222 and b = 100;"
    order_qt_case_3_sql "select * from ${tableName} where a = 100 and b in (132, 100, 222, 120);"
    order_qt_case_3_sql "select * from ${tableName} where a = 123 and b in (132, 100, 222, 333);"
    order_qt_case_3_sql "select * from ${tableName} where a = 400 and b in (250, 280, 300);"
    order_qt_case_3_sql "select * from ${tableName} where a in (123, 1, 350, 400, 420, 500, 1000) and b in (132, 100, 222, 200, 350, 250);"

    sql """DROP TABLE IF EXISTS ${tableName}"""

    tableName = "in_table_4"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    // Case 4: Bucket columns and partition columns are both partial primary keys,
    // and there is no overlap between bucket columns and partition columns
    sql """
            create table ${tableName} (
                a int not null,
                b int not null,
                c int not null,
                d string not null
            )
            unique key(a, b, c)
            partition by RANGE(c)
            (
                partition p0 values [(100), (200)),
                partition p1 values [(200), (300)),
                partition p2 values [(300), (400)),
                partition p3 values [(400), (500))
            )
            distributed by hash(a, b)
            buckets 16
            PROPERTIES(
                "replication_num" = "1",
                "store_row_column" = "true"
            );
        """

    sql """
            insert into ${tableName} values(123, 100, 110, "a");
            insert into ${tableName} values(222, 100, 115, "b");
            insert into ${tableName} values(12, 12, 120, "c");
            insert into ${tableName} values(1231, 1220, 210, "d");
            insert into ${tableName} values(323, 49, 240, "e");
            insert into ${tableName} values(843, 7342, 370, "f");
            insert into ${tableName} values(633, 2642, 480, "g");
            insert into ${tableName} values(6333, 2642, 480, "h");
        """

    qt_case_4_sql "select * from ${tableName} where a = 123 and b = 100 and c = 110;"
    qt_case_4_sql "select * from ${tableName} where a = 123 and b = 101 and c = 110;"
    qt_case_4_sql "select * from ${tableName} where a = 1231 and b = 1220 and c = 210;"
    order_qt_case_4_sql "select * from ${tableName} where a = 123 and b in (132, 100, 222, 333) and c in (110, 115, 120);"
    order_qt_case_4_sql "select * from ${tableName} where a in (123, 1, 222, 1231, 420, 500) and b in (132, 100, 222, 1220, 300) and c in (210, 110, 115, 210);"

    sql """DROP TABLE IF EXISTS ${tableName}"""

    tableName = "in_table_5"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    // Case 5: Bucket columns and partition columns are both partial primary keys,
    // and bucket columns and partition columns overlap
    sql """
            create table ${tableName} (
                a int not null,
                b int not null,
                c int not null,
                d string not null
            )
            unique key(a, b, c)
            partition by RANGE(a)
            (
                partition p0 values [(0), (100)),
                partition p1 values [(100), (200)),
                partition p2 values [(200), (300)),
                partition p3 values [(300), (400)),
                partition p4 values [(400), (500)),
                partition p5 values [(500), (900)),
                partition p6 values [(900), (1200)),
                partition p7 values [(1200), (9000))
            )
            distributed by hash(a, b)
            buckets 16
            PROPERTIES(
                "replication_num" = "1",
                "store_row_column" = "true"
            );
        """

    sql """
            insert into ${tableName} values(123, 100, 110, "a");
            insert into ${tableName} values(222, 100, 115, "b");
            insert into ${tableName} values(12, 12, 120, "c");
            insert into ${tableName} values(1231, 1220, 210, "d");
            insert into ${tableName} values(323, 49, 240, "e");
            insert into ${tableName} values(843, 7342, 370, "f");
            insert into ${tableName} values(633, 2642, 480, "g");
            insert into ${tableName} values(6333, 2642, 480, "h");
        """

    qt_case_5_sql "select * from ${tableName} where a = 123 and b = 100 and c = 110;"
    qt_case_5_sql "select * from ${tableName} where a = 123 and b = 101 and c = 110;"
    qt_case_5_sql "select * from ${tableName} where a = 1231 and b = 1220 and c = 210;"
    order_qt_case_5_sql "select * from ${tableName} where a = 123 and b in (132, 100, 222, 333) and c in (110, 115, 120);"
    order_qt_case_5_sql "select * from ${tableName} where a in (123, 12, 222, 1231, 420, 500, 6333, 633, 843) and b in (132, 100, 222, 1220, 300, 2642) and c in (210, 110, 115, 210, 480);"

    sql """DROP TABLE IF EXISTS ${tableName}"""
    
    tableName = "in_table_6"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    // Case 6: Default partitioning, primary keys are all bucket columns
    sql """
            create table ${tableName} (
                a int not null,
                b int not null,
                c string not null
            )
            unique key(a, b)
            distributed by hash(a, b) buckets 16
            PROPERTIES(
                "replication_num" = "1",
                "store_row_column" = "true"
            );
        """
    sql """
            insert into ${tableName} values(123, 132, "a");
            insert into ${tableName} values(123, 222, "b");
            insert into ${tableName} values(22, 2, "c");
            insert into ${tableName} values(1, 1, "d");
            insert into ${tableName} values(2, 2, "e");
            insert into ${tableName} values(3, 3, "f");
            insert into ${tableName} values(4, 4, "i");
        """
    qt_case_6_sql "select * from ${tableName} where a = 123 and b = 132;"
    order_qt_case_6_sql "select * from ${tableName} where a = 123 and b in (132, 1, 222, 333);"
    order_qt_case_6_sql "select * from ${tableName} where a in (123, 1, 222, 2, 3, 4) and b in (132, 1, 222, 333, 2, 3, 4);"

    sql """DROP TABLE IF EXISTS ${tableName}"""

    tableName = "in_table_7"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    // Case 7: Partition and bucket columns are the same, but only part of the primary key
    sql """
            create table ${tableName} (
                a int not null,
                b int not null,
                c int not null,
                d string not null
            )
            unique key(a, b, c)
            partition by RANGE(a)
            (
                partition p0 values [(0), (100)),
                partition p1 values [(100), (200)),
                partition p2 values [(200), (300)),
                partition p3 values [(300), (400)),
                partition p4 values [(400), (500)),
                partition p5 values [(500), (900)),
                partition p6 values [(900), (1200)),
                partition p7 values [(1200), (9000))
            )
            distributed by hash(a)
            buckets 16
            PROPERTIES(
                "replication_num" = "1",
                "store_row_column" = "true"
            );
        """

    sql """
            insert into ${tableName} values(123, 100, 110, "a");
            insert into ${tableName} values(222, 100, 115, "b");
            insert into ${tableName} values(12, 12, 120, "c");
            insert into ${tableName} values(1231, 1220, 210, "d");
            insert into ${tableName} values(323, 49, 240, "e");
            insert into ${tableName} values(843, 7342, 370, "f");
            insert into ${tableName} values(633, 2642, 480, "g");
            insert into ${tableName} values(6333, 2642, 480, "h");
        """

    qt_case_7_sql "select * from ${tableName} where a = 123 and b = 100 and c = 110;"
    qt_case_7_sql "select * from ${tableName} where a = 123 and b = 101 and c = 110;"
    qt_case_7_sql "select * from ${tableName} where a = 1231 and b = 1220 and c = 210;"
    order_qt_case_7_sql "select * from ${tableName} where a = 123 and b in (132, 100, 222, 333) and c in (110, 115, 120);"
    order_qt_case_7_sql "select * from ${tableName} where a in (123, 12, 222, 1231, 420, 500, 6333, 633, 843) and b in (132, 100, 222, 1220, 300, 2642) and c in (210, 110, 115, 210, 480);"

    sql """DROP TABLE IF EXISTS ${tableName}"""

    tableName = "in_table_8"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    // Case 8: The bucket column is the same as the primary key, and the partition column is part of the primary key.
    sql """    
            create table ${tableName} (
                a int not null,
                b int not null,
                c string not null
            )
            unique key(a, b)
            partition by RANGE(a)
            (
                partition p0 values [(100), (200)),
                partition p1 values [(200), (300)),
                partition p2 values [(300), (400)),
                partition p3 values [(400), (420)),
                partition p4 values [(420), (500))
            )
            distributed by hash(a, b)
            buckets 16
            PROPERTIES(
                "replication_num" = "1",
                "store_row_column" = "true"
            );
        """
    sql """
            insert into ${tableName} values(123, 100, "a");
            insert into ${tableName} values(150, 100, "b");
            insert into ${tableName} values(350, 200, "c");
            insert into ${tableName} values(400, 250, "d");
            insert into ${tableName} values(400, 280, "e");
            insert into ${tableName} values(450, 350, "f");
        """
    qt_case_8_sql "select * from ${tableName} where a = 123 and b = 100;"
    qt_case_8_sql "select * from ${tableName} where a = 222 and b = 100;"
    order_qt_case_8_sql "select * from ${tableName} where a = 123 and b in (132, 100, 222, 333);"
    order_qt_case_8_sql "select * from ${tableName} where a = 400 and b in (250, 280, 300);"
    order_qt_case_8_sql "select * from ${tableName} where a in (123, 1, 350, 400, 420, 500, 1000) and b in (132, 100, 222, 200, 350, 250);"

    sql """DROP TABLE IF EXISTS ${tableName}"""

    tableName = "in_table_9"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    // Case 9: Partition leftmost match
    sql """    
            create table ${tableName} (
                a int not null,
                b int not null,
                c int not null,
                d string not null
            )
            unique key(a, b, c)
            partition by RANGE(a, b, c)
            (
                partition p values [(1, 1, 1), (100, 100, 100)),
                partition p0 values [(100, 100, 100), (200, 210, 220)),
                partition p1 values [(200, 210, 220), (300, 250, 290)),
                partition p2 values [(300, 250, 290), (350, 290, 310)),
                partition p3 values [(350, 290, 310), (400, 350, 390)),
                partition p4 values [(400, 350, 390), (800, 400, 450)),
                partition p5 values [(800, 400, 450), (2000, 500, 500)),
                partition p6 values [(2000, 500, 500), (5000, 600, 600)),
                partition p7 values [(5000, 600, 600), (9999, 9999, 9999))
            )
            distributed by hash(a, c)
            buckets 16
            PROPERTIES(
                "replication_num" = "1",
                "store_row_column" = "true"
            );
        """
    sql """
            insert into ${tableName} values(123, 100, 110, "zxcd");
            insert into ${tableName} values(222, 100, 115, "zxc");
            insert into ${tableName} values(12, 12, 120, "zxc");
            insert into ${tableName} values(1231, 1220, 210, "zxc");
            insert into ${tableName} values(323, 49, 240, "zxc");
            insert into ${tableName} values(843, 7342, 370, "zxcde");
            insert into ${tableName} values(633, 2642, 480, "zxc");
            insert into ${tableName} values(6333, 2642, 480, "zxc");
        """
    
    order_qt_case_9_sql "select * from ${tableName} where a=123 and b=100 and c=110;"
    order_qt_case_9_sql "select * from ${tableName} where a=222 and b=100 and c=115;"
    order_qt_case_9_sql "select * from ${tableName} where a=323 and b=49 and c=240;"
    order_qt_case_9_sql "select * from ${tableName} where b=100 and a=123 and c=110;"
    order_qt_case_9_sql "select * from ${tableName} where a=1231 and b=1220 and c=210;"
    order_qt_case_9_sql "select * from ${tableName} where a=6333 and b=2642 and c=480;"
    order_qt_case_9_sql "select * from ${tableName} where a=633 and b=2642 and c=480;"
    order_qt_case_9_sql "select * from ${tableName} where a=123 and b in (132,100,222,333) and c in (110, 115, 120);"
    order_qt_case_9_sql "select * from ${tableName} where a in (222,1231) and b in (100,1220,2642) and c in (115,210,480);"
    order_qt_case_9_sql "select * from ${tableName} where a in (123,222,12) and b in (100,12) and c in (110,115,120,210);"
    order_qt_case_9_sql "select * from ${tableName} where a=1231 and b in (20490,1220,300) and c = 210;"
    order_qt_case_9_sql "select * from ${tableName} where a in (123,1,222, 400,420, 500) and b in (132,100,222, 200,300) and c in (110,115,210);"
    order_qt_case_9_sql "select * from ${tableName} where a in (123,1,222, 1231,420, 500) and b in (132,100,222, 1220,300) and c in (210,110,115,210);"
    sql """DROP TABLE IF EXISTS ${tableName}"""
}