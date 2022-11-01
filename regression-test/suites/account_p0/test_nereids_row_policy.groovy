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
    def tokens = context.config.jdbcUrl.split('/')
    def url=tokens[0] + "//" + tokens[2] + "/" + dbName + "?"

    def assertQueryResult = { size, enableNereids=true ->
        def result = connect(user=user, password='123456', url=url) {
            sql "set enable_nereids_planner = ${enableNereids}"
            sql "SELECT * FROM ${tableName}"
        }
        assertEquals(result.size(), size)
    }

    def createPolicy = { name, predicate, enableNereids=true ->
        sql "set enable_nereids_planner = ${enableNereids}"
        sql """
            CREATE ROW POLICY ${name} ON ${dbName}.${tableName}
            AS RESTRICTIVE TO ${user} USING (${predicate})
        """
    }

    def dropPolciy = { name ->
        sql """
            DROP ROW POLICY ${name} ON ${dbName}.${tableName} FOR ${user}
        """
    }

    // create table
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
            CREATE TABLE ${tableName} (
                `k` INT,
                `v` INT
            ) DUPLICATE KEY (`k`) DISTRIBUTED BY HASH (`k`) BUCKETS 1
            PROPERTIES ('replication_num' = '1')
    """
    sql """
        insert into ${tableName} values (1,1), (2,1), (1,3);
    """
    // create user
    sql "DROP USER IF EXISTS ${user}"
    sql "CREATE USER ${user} IDENTIFIED BY '123456'"
    sql "GRANT SELECT_PRIV ON internal.${dbName}.${tableName} TO ${user}"


    // no policy
    assertQueryResult 3

    // create original policy
    createPolicy"original", "k = 1", false
    assertQueryResult 2

    // merge nereids policy1. result: original policy (k=1 && v=1)
    createPolicy"policy1", "v = 1"
    assertQueryResult 1

    // drop original policy. result: nereids policy (v=1)
    dropPolciy "original"
    assertQueryResult 2

    // merger neredis policy2. result: nereids policy (v=1 && k=1)
    createPolicy"policy2", "k = 1"
    assertQueryResult 1

    // drop neredis policy1. result: nereids policy (k=1)
    dropPolciy "policy1"
    assertQueryResult 2

   // drop neredis policy2
    dropPolciy"policy2"
    assertQueryResult 3

}
