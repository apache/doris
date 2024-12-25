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

suite("test_select_except") {
    sql """ DROP TABLE IF EXISTS tbl_select_except """
    sql """
        CREATE TABLE tbl_select_except (
            siteid INT DEFAULT '10',
            citycode SMALLINT,
            username VARCHAR(32) DEFAULT '',
            pv BIGINT SUM DEFAULT '0'
        ) ENGINE=OLAP
        AGGREGATE KEY(siteid, citycode, username)
        DISTRIBUTED BY HASH(siteid) BUCKETS 10
        PROPERTIES (
         "replication_allocation" = "tag.location.default: 1",
         "in_memory" = "false",
         "storage_format" = "V2"
        );
    """
    sql """
        insert into tbl_select_except values(1,1,'jim',2)
    """
    sql """
        insert into tbl_select_except values(2,1,'grace',3)
    """
    sql """
        insert into tbl_select_except values(3,2,'tom',4)
    """

    List<List<Object>> results = sql "select * except (siteid, citycode) from tbl_select_except order by username"

    assertEquals(results.size(), 3)
    assertEquals(results[0].size(), 2)
    assertEquals(results[1].size(), 2)
    assertEquals(results[2].size(), 2)

    assertEquals(results[0][0], 'grace')
    assertEquals(results[1][0], 'jim')
    assertEquals(results[2][0], 'tom')
    assertEquals(results[0][1], 3)
    assertEquals(results[1][1], 2)
    assertEquals(results[2][1], 4)

    try {
        test {
            sql """
            select * except (concat(username, 's')) from tbl_select_except order by username
            """
            exception "errCode"
        }
        test {
            sql """
            select * except (siteid, citycode, username, pv) from tbl_select_except"""
            exception "errCode"
        }
        qt_except_agg """
        select * except (siteid, username, pv), sum(pv) 
        from tbl_select_except 
        group by citycode order by citycode"""
        qt_except_agg_ordinal """
        select * except (siteid, username, pv), sum(pv) 
        from tbl_select_except 
        group by 1 order by citycode"""
    } finally {
        sql "drop table if exists tbl_select_except"
    }
}
