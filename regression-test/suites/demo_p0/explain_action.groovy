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

suite("explain_action") {

    // this case check explain, so we disable nereids
    sql """set enable_nereids_planner=false"""

    explain {
        sql("select 100")

        // contains("OUTPUT EXPRS:\n    <slot 0> 100\n") && contains("PARTITION: UNPARTITIONED\n")
        contains "OUTPUT EXPRS:\n    100\n"
        contains "PARTITION: UNPARTITIONED\n"
    }

    explain {
        sql("select 100")

        // contains(" 100\n") && !contains("abcdefg") && !("1234567")
        contains " 100\n"
        notContains "abcdefg"
        notContains "1234567"
    }

    explain {
        sql("select 100")
        // simple callback
        check { explainStr -> explainStr.contains("abcdefg") || explainStr.contains(" 100\n") }
    }

    explain {
        sql("a b c d e")
        // callback with exception and time
        check { explainStr, exception, startTime, endTime ->
            // assertXxx() will invoke junit5's Assertions.assertXxx() dynamically
            assertTrue(exception != null)
        }
    }

    def dbName = "regression_test_demo_p0"
    def tbName = "tb_base"
    def tbMvName = "${tbName}_mv"
    sql """
            set experimental_enable_nereids_planner=true;
        """
    sql """
            drop database if exists ${dbName};
        """
    sql """
            create database  ${dbName};
        """
    sql """ CREATE TABLE ${dbName}.${tbName} (
                k0 BOOLEAN NOT NULL, 
                k1 TINYINT NOT NULL, 
                k2 SMALLINT NOT NULL, 
                k3 INT NOT NULL, 
                k4 BIGINT NOT NULL, 
                k5 LARGEINT NOT NULL, 
                k6 DECIMALV3(9, 3) NOT NULL, 
                k7 CHAR(5) NOT NULL, 
                k8 DATE NOT NULL, 
                k9 DATETIME NOT NULL, 
                k10 VARCHAR(20) NOT NULL, 
                k11 DOUBLE NOT NULL, 
                k12 FLOAT NOT NULL
                ) PARTITION BY RANGE(k1) (
                PARTITION p1 
                VALUES 
                    LESS THAN ("-10"), 
                    PARTITION p2 
                VALUES 
                    LESS THAN ("0"), 
                    PARTITION p3 
                VALUES 
                    LESS THAN ("10"), 
                    PARTITION p4 
                VALUES 
                    LESS THAN ("20")
                ) DISTRIBUTED BY HASH(k4, k5) BUCKETS 1;
        """
    sql """
            CREATE MATERIALIZED VIEW ${tbMvName} AS SELECT k0, max(k8), min(k11), sum(k12), count(k10), hll_union(hll_hash(k7)), bitmap_union(to_bitmap(k2)) from ${dbName}.${tbName} group by k0;
        """
    sql """
            insert into ${dbName}.${tbName} values(1,2,3,4,5,6,7,"8","2020-10-01","2020-10-10","10",12,13);
        """
    def count = 10;
    while (true) {
        def result = sql "desc ${dbName}.${tbName} all;"
        if (result.toString().containsIgnoreCase("${tbMvName}")) {
            break;
        } else {
            sleep 1000
            count++
        }
    }
    explain {
        sql("SELECT k0, max(k8), min(k11), sum(k12), count(k10), hll_union(hll_hash(k7)), bitmap_union(to_bitmap(k2)) from ${dbName}.${tbName} group by k0;")
        contains "${tbMvName}"
    }
}
