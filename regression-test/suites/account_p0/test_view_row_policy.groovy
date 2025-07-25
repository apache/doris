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
suite("test_view_row_policy") {
    String suiteName = "test_view_row_policy"
    String dbName = context.config.getDbNameByFile(context.file)
    String tableName = "${suiteName}_table"
    String viewName = "${suiteName}_view"
    String user = "${suiteName}_user"
    String pwd = 'C123_567p'
    String tablePolcyName = "${suiteName}_table_policy"
    String viewPolcyName = "${suiteName}_view_policy"
    def tokens = context.config.jdbcUrl.split('/')
    def url=tokens[0] + "//" + tokens[2] + "/" + dbName + "?"

    sql "DROP ROW POLICY IF EXISTS ${tablePolcyName} ON ${dbName}.${tableName} FOR ${user}"
    sql "DROP ROW POLICY IF EXISTS ${viewPolcyName} ON ${dbName}.${viewName} FOR ${user}"

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
        insert into ${tableName} values (1,1), (1,2), (2,2);
    """

    // create view
    sql "DROP VIEW IF EXISTS ${viewName}"
    sql """
        CREATE VIEW ${viewName} AS SELECT * FROM ${tableName};
    """
    
    // create user
    sql "DROP USER IF EXISTS ${user}"
    sql "CREATE USER ${user} IDENTIFIED BY '${pwd}'"
    sql """grant select_priv on regression_test to ${user}"""
    sql "GRANT SELECT_PRIV ON internal.${dbName}.* TO ${user}"

    sql 'sync'

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user}""";
    }

    // no policy
    connect(user, "${pwd}", url) {
        order_qt_no_policy "SELECT * FROM ${viewName}"
    }

    // table policy
    sql """
        CREATE ROW POLICY IF NOT EXISTS ${tablePolcyName} ON ${dbName}.${tableName}
        AS RESTRICTIVE TO ${user} USING (k=1)
    """
    connect(user, "${pwd}", url) {
        order_qt_table_policy "SELECT * FROM ${viewName}"
    }

    // view policy
    sql """
        CREATE ROW POLICY IF NOT EXISTS ${viewPolcyName} ON ${dbName}.${viewName}
        AS RESTRICTIVE TO ${user} USING (v=2)
    """
    connect(user, "${pwd}", url) {
        order_qt_view_policy "SELECT * FROM ${viewName}"
    }

    sql "DROP ROW POLICY IF EXISTS ${tablePolcyName} ON ${dbName}.${tableName} FOR ${user}"
    sql "DROP ROW POLICY IF EXISTS ${viewPolcyName} ON ${dbName}.${tableName} FOR ${user}"
}
