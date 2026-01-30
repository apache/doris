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

// This suit test the `frontends` tvf
suite("test_tablets_tvf", "p0,tvf") {
    String db = "test_tablets_tvf_db"
    String tb = "test_tablets_tvf_tbl"
    String nonAdminUser = "test_tablets_user"
    String nonAdminPwd = "123456"

    // 准备测试表（多分区）
    sql """ CREATE DATABASE IF NOT EXISTS ${db}"""
    sql """ USE ${db};"""
    sql """ DROP TABLE IF EXISTS ${tb} """
    sql """  
        CREATE TABLE ${tb} (  
            k1 INT,  
            k2 VARCHAR(10)  
        ) DUPLICATE KEY(k1)  
        PARTITION BY RANGE(k1) (  
            PARTITION p1 VALUES LESS THAN ("10"),  
            PARTITION p2 VALUES LESS THAN ("20")  
        )  
        DISTRIBUTED BY HASH(k1) BUCKETS 2  
        PROPERTIES("replication_num" = "1");  
    """
    sql """ INSERT INTO ${tb} VALUES (1,'a'), (11,'b'); """

    // 创建非 admin 用户
    try_sql("DROP USER IF EXISTS ${nonAdminUser}")
    sql """CREATE USER '${nonAdminUser}' IDENTIFIED BY '${nonAdminPwd}'"""

    // 1. describe function 校验列名与顺序
    def titles=["TabletId","ReplicaId","BackendId","SchemaHash","Version","LstSuccessVersion","LstFailedVersion","LstFailedTime","LocalDataSize","RemoteDataSize","RowCount","State","LstConsistencyCheckTime","CheckVersion","VisibleVersionCount","VersionCount","QueryHits","PathHash","Path","MetaUrl","CompactionStatus","CooldownReplicaId","CooldownMetaId"]
    def desc = sql """ desc function `tablets`("database"="${db}","table"="${tb}"); """
    assertTrue(titles.size() == desc.size())
    for (int i = 0; i < titles.size(); i++) {
        assertEquals(titles.get(i), desc[i][0])
    }

    // 2. explain verbose
    explain {
        sql """ select * from `tablets`("database"="${db}","table"="${tb}"); """
        contains "tablets"
    }
    explain {
        verbose true
        sql """ select TabletId, BackendId from `tablets`("database"="${db}","table"="${tb}","partitions"="p1"); """
        contains "TabletId"
        contains "BackendId"
    }

    // 3. 参数组合测试
    // 必填参数缺失
    test {
        sql """ select * from `tablets`(); """
        exception "tablets table-valued-function params must be not null or empty"
    }
    test {
        sql """ select * from `tablets`("database"="${db}"); """
        exception "dbName and tableName must not be null or empty"
    }
    // 非法参数
    test {
        sql """ select * from `tablets`("unknown"="x"); """
        exception "'unknown' is invalid property"
    }

    // 4. 与 show tablets 结果一致性
    def tvfRows = sql """ select * from `tablets`("database"="${db}","table"="${tb}") order by TabletId, ReplicaId; """
    logger.info("tvfRows: ${tvfRows}")
    def showRows = sql """ show tablets from ${tb} order by TabletId, ReplicaId; """
    assertEquals(showRows.size(), tvfRows.size())
    for (int i = 0; i < showRows.size(); i++) {
        for (int j = 0; j < titles.size(); j++) {
            assertEquals(showRows[i][j], tvfRows[i][j])
        }
    }

    // 分区过滤
    def tvfP1 = sql """ select * from `tablets`("database"="${db}","table"="${tb}","partitions"="p1") order by TabletId; """
    def showP1 = sql """ show tablets from ${tb} partition(p1) order by TabletId; """
    assertEquals(showP1.size(), tvfP1.size())

    // 5. 权限测试（非 admin 用户）
    connect(nonAdminUser, nonAdminPwd, context.config.jdbcUrl) {
        test {
            sql """ desc function `tablets`("database"="${db}","table"="${tb}"); """
            exception "ERR_SPECIFIC_ACCESS_DENIED_ERROR"
        }
        test {
            sql """ select * from `tablets`("database"="${db}","table"="${tb}"); """
            exception "ERR_SPECIFIC_ACCESS_DENIED_ERROR"
        }
    }

    // 6. 其他场景：空结果集（空表）
    sql """ DROP TABLE IF EXISTS empty_tbl """
    sql """ CREATE TABLE empty_tbl (k INT) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES("replication_num"="1"); """
    def empty = sql """ select * from `tablets`("database"="${db}","table"="empty_tbl"); """
    assertEquals(0, empty.size())

    // 清理
    sql """ DROP TABLE ${tb} """
    sql """ DROP TABLE IF EXISTS empty_tbl """
    try_sql("DROP USER ${nonAdminUser}")
}
