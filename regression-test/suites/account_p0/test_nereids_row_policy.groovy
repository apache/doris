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
suite("test_nereids_row_policy") {
    def dbName = context.config.getDbNameByFile(context.file)
    def tableName = "nereids_row_policy"
    def user='row_policy_user'

    sql "set enable_nereids_planner = true"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
            CREATE TABLE ${tableName} (
                `k` INT,
                `v` INT
            ) DUPLICATE KEY (`k`) DISTRIBUTED BY HASH (`k`) BUCKETS 1
            PROPERTIES ('replication_num' = '1')
    """
    sql """
        insert into ${tableName} values (1,1), (2,1), (3,3), (4,4);
    """

    sql "DROP USER IF EXISTS ${user}"
    sql "CREATE USER ${user} IDENTIFIED BY '123456'"
    sql "GRANT SELECT_PRIV ON internal.${dbName}.${tableName} TO ${user}"

    def tokens = context.config.jdbcUrl.split('/')
    def url=tokens[0] + "//" + tokens[2] + "/" + dbName + "?"

    // no row policy
    def result1 = connect(user=user, password='123456', url=url) {
        sql "SELECT * FROM ${tableName}"
    }
    assertEquals(result1.size(), 4)


    // policy1: v = 1
    sql """
        CREATE ROW POLICY test_row_policy_1 ON ${dbName}.${tableName}
        AS RESTRICTIVE TO ${user} USING (v = 1)
    """
    def result2 = connect(user=user, password='123456', url=url) {
        sql "SELECT * FROM ${tableName}"
    }
    assertEquals(result2.size(), 2)

    // policy2: k = 1
    sql """
	CREATE ROW POLICY test_row_policy_2 ON ${dbName}.${tableName}
	AS RESTRICTIVE TO ${user} USING (k = 1)
    """
    def result3 = connect(user=user, password='123456', url=url) {
        sql "SELECT * FROM ${tableName}"
    }
    assertEquals(result3.size(), 1)

    // drop policy1
    sql """
	DROP ROW POLICY test_row_policy_1 ON ${dbName}.${tableName} FOR ${user}
    """
    def result4 = connect(user=user, password='123456', url=url) {
        sql "SELECT * FROM ${tableName}"
    }
    assertEquals(result4.size(), 1)

   // drop policy2
    sql """
	DROP ROW POLICY test_row_policy_2 ON ${dbName}.${tableName} FOR ${user}
    """
    def result5 = connect(user=user, password='123456', url=url) {
        sql "SELECT * FROM ${tableName}"
    }
    assertEquals(result5.size(), 4)
}
