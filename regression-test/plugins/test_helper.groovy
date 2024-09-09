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

import org.apache.doris.regression.suite.Suite

Suite.metaClass.createTestTable = { String tableName, boolean uniqueTable = false ->
    Suite suite = delegate as Suite
    def sql = { String sqlStr ->
        suite.sql sqlStr
    }

    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    sql "drop table if exists ${tableName}"

    sql """
        create table ${tableName}
        (
            id int,
            value int
        )
        ${uniqueTable ? "unique key(id)" : ""}
        partition by range(id)
        (
            partition p1 values[('1'), ('2')),
            partition p2 values[('2'), ('3')),
            partition p3 values[('3'), ('4')),
            partition p4 values[('4'), ('5')),
            partition p5 values[('5'), ('6'))
        )
        distributed by hash(id)
        properties(
            'replication_num'='1'
        )
        """

    sql """
        insert into ${tableName}
        values (1, 1), (1, 2),
               (2, 1), (2, 2), 
               (3, 1), (3, 2),
               (4, 1), (4, 2),
               (5, 1), (5, 2)
        """

    sql "sync"
}


logger.info("Added 'createTestTable' function to Suite")
