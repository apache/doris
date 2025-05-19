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

suite("test_nereids_show_load") {
    sql """ drop database if exists  test_nereids_show_load_db"""
    sql """create database test_nereids_show_load_db"""
    sql """use test_nereids_show_load_db;"""
    sql """ DROP TABLE IF EXISTS test_nereids_show_load """

    sql """
        CREATE TABLE IF NOT EXISTS test_nereids_show_load (
            `k1` bigint(20) NULL,
            `k2` bigint(20) NULL
        ) ENGINE=OLAP
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 2
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    sql """ insert into test_nereids_show_load  with label `test_nereids_show_load_label_xyz` values (1,1); """
    sql """ insert into test_nereids_show_load  with label `new_test_nereids_show_load_label_xyz` values (2,2); """

    checkNereidsExecute("SHOW LOAD")
    checkNereidsExecute("SHOW LOAD FROM test_nereids_show_load_db")
    checkNereidsExecute("SHOW LOAD WHERE LABEL = 'test_nereids_show_load_label_xyz'")
    checkNereidsExecute("SHOW LOAD FROM test_nereids_show_load_db WHERE LABEL = 'test_nereids_show_load_label_xyz'")
    checkNereidsExecute("SHOW LOAD WHERE LABEL LIKE '%test_nereids_show_load_label_xyz'")
    checkNereidsExecute("SHOW LOAD FROM test_nereids_show_load_db WHERE LABEL LIKE '%test_nereids_show_load_label_xyz'")
    checkNereidsExecute("SHOW LOAD WHERE STATE = 'PENDING'")
    checkNereidsExecute("SHOW LOAD FROM test_nereids_show_load_db WHERE STATE = 'PENDING'")
    checkNereidsExecute("SHOW LOAD WHERE STATE = 'PENDING' AND LABEL = 'test_nereids_show_load_label_xyz'")
    checkNereidsExecute("SHOW LOAD FROM test_nereids_show_load_db WHERE STATE = 'PENDING' AND LABEL = 'test_nereids_show_load_label_xyz'")
    checkNereidsExecute("SHOW LOAD WHERE STATE = 'FINISHED' AND LABEL = 'test_nereids_show_load_label_xyz'")
    checkNereidsExecute("SHOW LOAD FROM test_nereids_show_load_db WHERE STATE = 'FINISHED' AND LABEL = 'test_nereids_show_load_label_xyz'")
    checkNereidsExecute("SHOW LOAD WHERE STATE = 'CANCELLED' AND LABEL = 'test_nereids_show_load_label_xyz'")
    checkNereidsExecute("SHOW LOAD FROM test_nereids_show_load_db WHERE STATE = 'CANCELLED' AND LABEL = 'test_nereids_show_load_label_xyz'")

    checkNereidsExecute("SHOW LOAD WHERE LABEL = 'test_nereids_show_load_label_xyz' ORDER BY LoadStartTime")
    checkNereidsExecute("SHOW LOAD FROM test_nereids_show_load_db WHERE LABEL = 'test_nereids_show_load_label_xyz' ORDER BY LoadStartTime")
    checkNereidsExecute("SHOW LOAD WHERE LABEL = 'test_nereids_show_load_label_xyz' ORDER BY LoadStartTime LIMIT 1")
    checkNereidsExecute("SHOW LOAD FROM test_nereids_show_load_db WHERE LABEL = 'test_nereids_show_load_label_xyz' ORDER BY LoadStartTime LIMIT 1")
    checkNereidsExecute("SHOW LOAD WHERE LABEL = 'test_nereids_show_load_label_xyz' ORDER BY LoadStartTime LIMIT 1,1")
    checkNereidsExecute("SHOW LOAD FROM test_nereids_show_load_db WHERE LABEL = 'test_nereids_show_load_label_xyz' ORDER BY LoadStartTime LIMIT 1,1")

    checkNereidsExecute("SHOW LOAD ORDER BY LoadStartTime")
    checkNereidsExecute("SHOW LOAD  FROM test_nereids_show_load_db ORDER BY LoadStartTime")
    checkNereidsExecute("SHOW LOAD ORDER BY LoadStartTime LIMIT 1")
    checkNereidsExecute("SHOW LOAD FROM test_nereids_show_load_db ORDER BY LoadStartTime LIMIT 1")

    def res1 = sql """SHOW LOAD WHERE LABEL = 'test_nereids_show_load_label_xyz'"""
    assertEquals("test_nereids_show_load_label_xyz", res1.get(0).get(1))

    def res2 = sql """SHOW LOAD WHERE LABEL = 'test_nereids_show_load_label_xyz' AND State = 'FINISHED'"""
    assertEquals("test_nereids_show_load_label_xyz", res2.get(0).get(1))

    def res4 = sql """SHOW LOAD WHERE LABEL like '%test_nereids_show_load_label_xyz%'"""
    assertEquals("test_nereids_show_load_label_xyz", res4.get(0).get(1))

    def res5 = sql """SHOW LOAD WHERE LABEL = 'test_nereids_show_load_label_xyz' ORDER BY LABEL"""
    assertEquals("test_nereids_show_load_label_xyz", res5.get(0).get(1))

    def res6 = sql """SHOW LOAD WHERE LABEL = 'test_nereids_show_load_label_xyz' ORDER BY LABEL LIMIT 2"""
    assertEquals("test_nereids_show_load_label_xyz", res6.get(0).get(1))

    def res7 = sql """SHOW LOAD WHERE LABEL = 'test_nereids_show_load_label_xyz' ORDER BY LABEL LIMIT 2,2"""
    def size7 = res7.size()
    assertEquals(0, size7)

    assertThrows(Exception.class, {
        sql """SHOW LOAD WHERE LABEL = 'test_nereids_show_load_label_xyz' AND State like '%xx%'"""
    })
    assertThrows(Exception.class, {
        sql """SHOW LOAD WHERE TaskInfo = 'xx'"""
    })
    assertThrows(Exception.class, {
        sql """SHOW LOAD WHERE LABEL = 'test_nereids_show_load_label_xyz' OR State = 'xx'"""
    })
    assertThrows(Exception.class, {
        sql """SHOW LOAD WHERE TaskInfo like '%xx%'"""
    })
    assertThrows(Exception.class, {
        sql """"SHOW LOAD WHERE LABEL = 'test_nereids_show_load_label_xyz' ORDER BY 1"""
    })
    sql """drop database if exists test_nereids_show_load_db"""
}
