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

suite("test_point_IN_query", "p0") {
    def tableName = "rs_in_table"
    sql """DROP TABLE IF EXISTS ${tableName}"""
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
    
    order_qt_1_EQ_sql "select * from ${tableName} where a = 123 and b in (100, 12, 1220, 7342, 999, 2642) and  c in (110, 115, 120, 480);"
    order_qt_1_EQ_sql "select * from ${tableName} where a in (12, 222, 1231, 6333) and b = 2642 and c in (210, 110, 115, 210, 480);"
    order_qt_1_EQ_sql "select * from ${tableName} where a in (123, 1, 222, 1231, 420, 500) and b in (132, 100, 222, 1220, 300) and c = 210;"
    explain {
        sql "select * from ${tableName} where a in (12, 222, 1231, 6333) and b = 2642 and c in (210, 110, 115, 210, 480);"
        contains "SHORT-CIRCUIT"
    }

    order_qt_2_EQ_sql "select * from ${tableName} where a = 222 and b = 100 and c in (110, 115, 120, 480);"
    order_qt_2_EQ_sql "select * from ${tableName} where a = 1231 and  b in (100, 12, 1220, 7342, 999, 2642) and c = 210;"
    order_qt_2_EQ_sql "select * from ${tableName} where a in (12, 222, 1231, 6333) and  b = 1220 and c = 210;"
    explain {
        sql "select * from ${tableName} where a in (12, 222, 1231, 6333) and  b = 1220 and c = 210;"
        contains "SHORT-CIRCUIT"
    }

    order_qt_3_EQ_sql "select * from ${tableName} where a = 323 and b = 49 and c = 240;"
    order_qt_0_EQ_sql "select * from ${tableName} where a in (123, 222, 12) and b in (100, 12) and c in (110, 115, 120, 210);"
    explain {
        sql "select * from ${tableName} where a in (123, 222, 12) and b in (100, 12) and c in (110, 115, 120, 210);"
        contains "SHORT-CIRCUIT"
    }

    sql """DROP TABLE IF EXISTS ${tableName}"""
}