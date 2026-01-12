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

suite("test_nereids_show_partitions") {
    sql "CREATE DATABASE IF NOT EXISTS test_show_partitions"
    sql "DROP TABLE IF EXISTS test_show_partitions.test_show_partitions_tbl"
    sql """
  CREATE TABLE IF NOT EXISTS test_show_partitions.test_show_partitions_tbl
  (
      `user_id` LARGEINT NOT NULL COMMENT "用户id",
      `date` DATE NOT NULL COMMENT "数据灌入日期时间",
      `city` VARCHAR(20) NOT NULL COMMENT "用户所在城市",
      `age` SMALLINT COMMENT "用户年龄",
      `sex` TINYINT COMMENT "用户性别",
      `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费"
  ) ENGINE = olap
  AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`)
  PARTITION BY LIST (`city`)
  (
    PARTITION `p_huabei` VALUES IN ("beijing", "tianjin"),
    PARTITION `p_dongbei` VALUES IN ("shenyang"),
    PARTITION `p_huazhong` VALUES IN ("chengdu")
  )
  DISTRIBUTED BY HASH(`user_id`) BUCKETS 16
  PROPERTIES ("replication_num" = "1");
 """
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl order by PartitionId")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl limit 1")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl limit 1,1")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl order by PartitionId limit 1,1")

    // PartitionId
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where PartitionId = 1748353297225")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where PartitionId = 1748353297225 order by PartitionId")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where PartitionId = 1748353297225 limit 1")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where PartitionId = 1748353297225 order by PartitionId limit 1")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where PartitionId != 1748353297225")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where PartitionId != 1748353297225 order by PartitionId")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where PartitionId != 1748353297225 limit 1")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where PartitionId != 1748353297225 order by PartitionId limit 1")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where PartitionId != 1748353297225 limit 1,1")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where PartitionId != 1748353297225 limit 100,1")

    // PartitionName
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where PartitionName = 'p_dongbei'")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where PartitionName = 'p_dongbei' order by PartitionName")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where PartitionName = 'p_dongbei' order by PartitionName limit 1")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where PartitionName = 'p_dongbei' limit 1")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where PartitionName = 'p_dongbei' limit 1,1")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where PartitionName = 'p_dongbei' limit 100,1")

    // like
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where PartitionName like 'p_dongbei'")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where PartitionName like 'p_dongbei' order by PartitionName")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where PartitionName like 'p_dongbei' order by PartitionName limit 1")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where PartitionName like 'p_dongbei' limit 1")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where PartitionName like 'p_dongbei' limit 1,1")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where PartitionName like 'p_dongbei' limit 100,1")

    // Buckets
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where Buckets = 16")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where Buckets = 16 order by PartitionName")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where Buckets = 16 order by PartitionName limit 1")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where Buckets = 16 limit 1")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where Buckets = 16 limit 1,1")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where Buckets >= 16")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where Buckets <= 16 order by PartitionName")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where Buckets >= 16 order by PartitionName limit 1")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where Buckets <= 16 limit 1")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where Buckets <= 16 limit 100,1")

    // complex where
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where Buckets = 16 and PartitionName = 'p_dongbei'")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where Buckets = 16 and PartitionName = 'p_dongbei' order by PartitionName")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where Buckets = 16 and PartitionName = 'p_dongbei' order by PartitionName limit 1")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where Buckets = 16 and PartitionName = 'p_dongbei' limit 1")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where Buckets = 16 and PartitionName = 'p_dongbei' limit 1,1")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where Buckets = 16 and PartitionName = 'p_dongbei' and PartitionId != 1748353297225")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where Buckets = 16 and PartitionName = 'p_dongbei' and PartitionId != 1748353297225 order by PartitionName")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where Buckets = 16 and PartitionName = 'p_dongbei' and PartitionId != 1748353297225 order by PartitionName limit 1")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where Buckets = 16 and PartitionName = 'p_dongbei' and PartitionId != 1748353297225 limit 1")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where Buckets = 16 and PartitionName = 'p_dongbei' and PartitionId != 1748353297225 limit 1,1")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl " +
            "where Buckets = 16 and PartitionName = 'p_dongbei' and PartitionId != 1748353297225 limit 100,1")

    // state
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl where state = 'NORMAL'")
    checkNereidsExecute("show partitions from test_show_partitions.test_show_partitions_tbl where state like '%NORMAL%'")

   def res1 = sql """show partitions from test_show_partitions.test_show_partitions_tbl"""
   assertEquals(3, res1.size())
   def res2 = sql """show partitions from test_show_partitions.test_show_partitions_tbl limit 1"""
   assertEquals(1, res2.size())
   def res3 = sql """show partitions from test_show_partitions.test_show_partitions_tbl limit 1,1"""
   assertEquals(1, res3.size())
   def res4 = sql """show partitions from test_show_partitions.test_show_partitions_tbl order by PartitionId limit 1,1"""
   assertEquals(1, res4.size())

   def res5 = sql """show partitions from test_show_partitions.test_show_partitions_tbl where PartitionId = 1748353297225"""
   assertEquals(0, res5.size())
   def res6 = sql """show partitions from test_show_partitions.test_show_partitions_tbl where PartitionId != 1748353297225"""
   assertEquals(3, res6.size())
   def res7 = sql """show partitions from test_show_partitions.test_show_partitions_tbl where PartitionId != 1748353297225 limit 1"""
   assertEquals(1, res7.size())
   def res8 = sql """show partitions from test_show_partitions.test_show_partitions_tbl where PartitionId != 1748353297225 order by PartitionName"""
   assertEquals(3, res8.size())
   def res9 = sql """show partitions from test_show_partitions.test_show_partitions_tbl where PartitionId != 1748353297225 order by PartitionName limit 1"""
   assertEquals(1, res9.size())
   def res10 = sql """show partitions from test_show_partitions.test_show_partitions_tbl where PartitionId != 1748353297225 order by PartitionName limit 1"""
   assertEquals("p_dongbei", res10.get(0).get(1))
   def res11 = sql """show partitions from test_show_partitions.test_show_partitions_tbl where PartitionName = 'p_huabei' order by PartitionName"""
   assertEquals("p_huabei", res11.get(0).get(1))
   def res12 = sql """show partitions from test_show_partitions.test_show_partitions_tbl where PartitionName like 'p_%' order by PartitionName"""
   assertEquals(3, res12.size())
   def res13 = sql """show partitions from test_show_partitions.test_show_partitions_tbl where PartitionName like 'p_%' order by PartitionName"""
   assertEquals("p_dongbei", res13.get(0).get(1))
   def res14 = sql """show partitions from test_show_partitions.test_show_partitions_tbl where PartitionName like 'p_%' order by PartitionName limit 1,1"""
   assertEquals("p_huabei", res14.get(0).get(1))
   def res15 = sql """show partitions from test_show_partitions.test_show_partitions_tbl where Buckets = 16"""
   assertEquals(3, res15.size())
   def res16 = sql """show partitions from test_show_partitions.test_show_partitions_tbl where Buckets = 16 order by PartitionName"""
   assertEquals("p_dongbei", res16.get(0).get(1))
   def res17 = sql """show partitions from test_show_partitions.test_show_partitions_tbl where Buckets >= 16 order by PartitionName"""
   assertEquals("p_dongbei", res17.get(0).get(1))
   def res18 = sql """show partitions from test_show_partitions.test_show_partitions_tbl where Buckets >= 16 order by PartitionName"""
   assertEquals(3, res18.size())
   def res19 = sql """show partitions from test_show_partitions.test_show_partitions_tbl where Buckets >= 16 and PartitionName = 'p_huabei'"""
   assertEquals(1, res19.size())
   def res20 = sql """show partitions from test_show_partitions.test_show_partitions_tbl where Buckets >= 16 and PartitionName = 'p_huabei'"""
   assertEquals("p_huabei", res20.get(0).get(1))
   def res21 = sql """show partitions from test_show_partitions.test_show_partitions_tbl where Buckets >= 16 and PartitionName = 'p_huabei' and PartitionId != 1"""
   assertEquals("p_huabei", res21.get(0).get(1))
   def res22 = sql """show partitions from test_show_partitions.test_show_partitions_tbl where Buckets >= 16 and PartitionName = 'p_huabei' and PartitionId = 1"""
   assertEquals(0, res22.size())

   // test for offset value is bigger than partitions' number
   def res23 = sql """show partitions from test_show_partitions.test_show_partitions_tbl where Buckets = 16 limit 100,1"""
   assertEquals(0, res23.size())

    // test for state
    def res24 = sql """show partitions from test_show_partitions.test_show_partitions_tbl where state = 'NORMAL'"""
    assertEquals(3, res24.size())
    def res25 = sql """show partitions from test_show_partitions.test_show_partitions_tbl where state like '%NORMAL%'"""
    assertEquals(3, res25.size())

   assertThrows(Exception.class, {
      sql """show partitions from test_show_partitions.test_show_partitions_tbl where VisibleVersion = 1"""
   })
   assertThrows(Exception.class, {
      sql """show partitions from test_show_partitions.test_show_partitions_tbl where Buckets >= 16 or PartitionName = 'p_huabei'"""
   })
   assertThrows(Exception.class, {
      sql """show partitions from test_show_partitions.test_show_partitions_tbl where Buckets like '16'"""
   })
   assertThrows(Exception.class, {
      sql """show partitions from test_show_partitions.test_show_partitions_tbl where State > 'NORMAL'"""
   })
   assertThrows(Exception.class, {
      sql """show partitions from test_show_partitions.test_show_partitions_tbl where LastConsistencyCheckTime = 1"""
   })
   assertThrows(Exception.class, {
      sql """show partitions from test_show_partitions.test_show_partitions_tbl where 1 = 2"""
   })
}
