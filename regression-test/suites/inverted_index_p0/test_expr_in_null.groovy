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

suite("test_expr_in_null", "p0"){
    // prepare test table}
    def indexTblName = "test_expr_in_null"
    sql "DROP TABLE IF EXISTS ${indexTblName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${indexTblName}(
            k bigint,
            v ipv4,
            INDEX idx_dc (v) USING INVERTED
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true");
    """

    sql """
        insert into ${indexTblName} values (1, '192.168.1.1');
        insert into ${indexTblName} values (2, '192.168.1.2');
        insert into ${indexTblName} values (3, '192.168.1.3');
    """

    
    sql """ set enable_common_expr_pushdown = true """
    sql """ set enable_inverted_index_query = true """

    qt_sql """
        SELECT count() FROM  ${indexTblName}
        WHERE CAST(v AS ipv4) NOT IN ('33.33.33.33', cast("abc" as ipv4))
        OR CAST(v AS ipv4)  IN ( '33.33.22.33', cast("ddd" as ipv4));
    """

     qt_sql """
        SELECT count() FROM  ${indexTblName}
        WHERE CAST(v AS ipv4) NOT IN ('33.33.33.33',NULL)
        OR CAST(v AS ipv4)  IN ( '33.33.22.33', NULL);
    """

    sql """ set enable_inverted_index_query = false """

    qt_sql """
        SELECT count() FROM  ${indexTblName}
        WHERE CAST(v AS ipv4) NOT IN ('33.33.33.33', cast("abc" as ipv4))
        OR CAST(v AS ipv4)  IN ( '33.33.22.33', cast("ddd" as ipv4));
    """

    qt_sql """
        SELECT count() FROM  ${indexTblName}
        WHERE CAST(v AS ipv4) NOT IN ('33.33.33.33',NULL)
        OR CAST(v AS ipv4)  IN ( '33.33.22.33', NULL);
    """
}