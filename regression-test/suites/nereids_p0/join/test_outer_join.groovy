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

suite("test_outer_join", "nereids_p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"

    def tbl1 = "test_outer_join1"
    def tbl2 = "test_outer_join2"
    def tbl3 = "test_outer_join3"

    sql "DROP TABLE IF EXISTS ${tbl1}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbl1} (
                c0 DECIMALV3(8,3)
            ) 
            DISTRIBUTED BY HASH (c0) BUCKETS 1 PROPERTIES ("replication_num" = "1");
        """

    sql "DROP TABLE IF EXISTS ${tbl2}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbl2} (
                c0 CHAR(249)
            ) AGGREGATE KEY(c0) 
            DISTRIBUTED BY RANDOM BUCKETS 30 
            PROPERTIES ("replication_num" = "1");
        """

    sql "DROP TABLE IF EXISTS ${tbl3}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbl3} (
                c0 DECIMALV3(8,3)
            ) 
            DISTRIBUTED BY HASH (c0) BUCKETS 1 PROPERTIES ("replication_num" = "1");
        """

    sql """INSERT INTO ${tbl2} (c0) VALUES ('dr'), ('x7Tq'), ('');"""
    sql """INSERT INTO ${tbl1} (c0) VALUES (0.47683432698249817), (0.8864791393280029);"""
    sql """INSERT INTO ${tbl1} (c0) VALUES (0.11287713050842285);"""
    sql """INSERT INTO ${tbl2} (c0) VALUES ('');"""
    sql """INSERT INTO ${tbl2} (c0) VALUES ('');"""
    sql """INSERT INTO ${tbl2} (c0) VALUES ('hb');"""

    qt_join """
            SELECT * FROM  ${tbl2} RIGHT  OUTER JOIN ${tbl1} ON (('') like ('15DScmSM')) WHERE ('abc' LIKE 'abc') ORDER BY 2;    
    """
    qt_join """
            SELECT * FROM  ${tbl2} RIGHT  OUTER JOIN ${tbl1} ON (('') like ('15DScmSM')) WHERE ('abc' NOT LIKE 'abc');
    """
    qt_join """
            SELECT * FROM  ${tbl2} JOIN ${tbl1} ON (('') like ('15DScmSM')) WHERE ('abc' NOT LIKE 'abc');
    """
    qt_join """
            SELECT * FROM  ${tbl2} LEFT  OUTER JOIN ${tbl1} ON (('') like ('15DScmSM')) WHERE ('abc' NOT LIKE 'abc');    
    """

    sql "set disable_join_reorder=true"
    explain {
        sql "SELECT * FROM ${tbl1} RIGHT OUTER JOIN ${tbl3} ON ${tbl1}.c0 = ${tbl3}.c0"
        contains "RIGHT OUTER JOIN(PARTITIONED)"
    }
    explain {
        sql "SELECT * FROM ${tbl1} RIGHT ANTI JOIN ${tbl3} ON ${tbl1}.c0 = ${tbl3}.c0"
        contains "RIGHT ANTI JOIN(PARTITIONED)"
    }
    explain {
        sql "SELECT * FROM ${tbl1} FULL OUTER JOIN ${tbl3} ON ${tbl1}.c0 = ${tbl3}.c0"
        contains "FULL OUTER JOIN(PARTITIONED)"
    }

    sql "DROP TABLE IF EXISTS ${tbl1}"
    sql "DROP TABLE IF EXISTS ${tbl2}"
    sql "DROP TABLE IF EXISTS ${tbl3}"
}
