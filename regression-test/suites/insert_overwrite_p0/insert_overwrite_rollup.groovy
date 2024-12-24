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

import org.junit.Assert;

suite("insert_overwrite_rollup","p0") {
    String suiteName = "insert_overwrite_not_fallback"
    String tableName = "${suiteName}_table"
    String rollupName = "${suiteName}_rollup"
    sql """drop table if exists `${tableName}`"""
    sql """
    CREATE TABLE `${tableName}`
      (
          k2 TINYINT,
          k3 INT not null
      )
      COMMENT "my first table"
      PARTITION BY LIST(`k3`)
      (
          PARTITION `p1` VALUES IN ('1'),
          PARTITION `p2` VALUES IN ('2'),
          PARTITION `p3` VALUES IN ('3')
      )
      DISTRIBUTED BY HASH(k2) BUCKETS 2
      ROLLUP (
          ${rollupName}(k3)
      )
      PROPERTIES (
          "replication_num" = "1"
      );
    """
    sql """
        insert overwrite table ${tableName} values(1,1);
        """
    def res = sql "desc ${tableName} all"
    logger.info("res: " + res.toString())
    assertTrue(res.toString().contains("${rollupName}"))
    sql """drop table if exists `${tableName}`"""
}

