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
    def viewName = "view_" + tableName
    def user='row_policy_user'
    def tokens = context.config.jdbcUrl.split('/')
    def url=tokens[0] + "//" + tokens[2] + "/" + dbName + "?"
    def isCloudMode = {
        def ret = sql_return_maparray  """show backends"""
        ret.Tag[0].contains("cloud_cluster_name")
    }
    def cloudMode = isCloudMode.call()
    //cloud-mode
    if (cloudMode) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
    }

    def assertQueryResult = { size ->
        def result1 = connect(user=user, password='123abc!@#', url=url) {
            sql "set enable_nereids_planner = false"
            sql "SELECT * FROM ${tableName}"
        }
        def result2 = connect(user=user, password='123abc!@#', url=url) {
            sql "set enable_nereids_planner = true"
            sql "set enable_fallback_to_original_planner = false"
            sql "SELECT * FROM ${tableName}"
        }
        connect(user=user, password='123abc!@#', url=url) {
            sql "set enable_nereids_planner = true"
            sql "set enable_fallback_to_original_planner = false"
            test {
                sql "SELECT * FROM ${viewName}"
                exception "does not have privilege for"
            }
        }
        assertEquals(size, result1.size())
        assertEquals(size, result2.size())
    }

    def createPolicy = { name, predicate, type ->
        sql """
            CREATE ROW POLICY IF NOT EXISTS ${name} ON ${dbName}.${tableName}
            AS ${type} TO ${user} USING (${predicate})
        """
    }

    def dropPolciy = { name ->
        sql """
            DROP ROW POLICY IF EXISTS ${name} ON ${dbName}.${tableName} FOR ${user}
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

    // create view
    sql "DROP VIEW IF EXISTS ${viewName}"
    sql """
        CREATE VIEW ${viewName} AS SELECT * FROM ${tableName};
    """
    
    // create user
    sql "DROP USER IF EXISTS ${user}"
    sql "CREATE USER ${user} IDENTIFIED BY '123abc!@#'"
    sql "GRANT SELECT_PRIV ON internal.${dbName}.${tableName} TO ${user}"

    sql 'sync'

    //cloud-mode
    if (cloudMode) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
    }

    dropPolciy "policy0"
    dropPolciy "policy1"
    dropPolciy "policy2"
    dropPolciy "policy3"

    // no policy
    assertQueryResult 3

    // (k = 1)
    createPolicy"policy0", "k = 1", "RESTRICTIVE"
    assertQueryResult 2

    // (k = 1 and v = 1)
    createPolicy"policy1", "v = 1", "RESTRICTIVE"
    assertQueryResult 1

    // (v = 1)
    dropPolciy "policy0"
    assertQueryResult 2

    // (v = 1) and (k = 1)
    createPolicy"policy2", "k = 1", "PERMISSIVE"
    assertQueryResult 1

   // (v = 1) and (k = 1 or k = 2)
    createPolicy"policy3", "k = 2", "PERMISSIVE"
    assertQueryResult 2

    dropPolciy "policy0"
    dropPolciy "policy1"
    dropPolciy "policy2"
    dropPolciy "policy3"
}
