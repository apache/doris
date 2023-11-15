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

package org.apache.doris.regression.action

import groovy.util.logging.Slf4j
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.doris.regression.suite.SuiteContext
import org.apache.doris.regression.util.JdbcUtils

import java.sql.ResultSetMetaData

@Slf4j
class RestoreAction implements SuiteAction {
    public final InetSocketAddress address
    public final String user
    public final String password

    String db
    List<String> tables
    String location
    String region
    String ak
    String sk
    String endpoint
    String repository
    String snapshot
    String timestamp
    int replicationNum
    long timeout

    SuiteContext context

    RestoreAction(SuiteContext context) {
        this.address = context.getFeHttpAddress()
        this.user = context.config.feHttpUser
        this.password = context.config.feHttpPassword
        this.db = context.config.defaultDb
        this.context = context
    }

    void db(String db) {
        this.db = db
    }

    void location(String location) {
        this.location = location
    }

    void region(String region) {
        this.region = region
    }

    void ak(String ak) {
        this.ak = ak
    }

    void sk(String sk) {
        this.sk = sk
    }

    void endpoint(String endpoint) {
        this.endpoint = endpoint
    }

    void repository(String repo) {
        this.repository = repo
    }

    void snapshot(String snapshot) {
        this.snapshot = snapshot
    }

    void timestamp(String timestamp) {
        this.timestamp = timestamp
    }

    void replicationNum(int replicaNum) {
        this.replicationNum = replicaNum
    }

    void timeout(long timeout) {
        this.timeout = timeout
    }

    void tables(String tables) {
        if (StringUtils.isNotEmpty(tables))
        this.tables = tables.split(",")
    }

    @Override
    void run() {
        // create repo
        createRepository()
        // restore
        restore()
        // check result
        checkRestore()
    }

    private void createRepository() {
        String showRepoSql = """
SHOW REPOSITORIES
"""
        String dropRepoSql = """
DROP REPOSITORY ${repository}
"""
        List<List<Object>> showRepoRes = null
        ResultSetMetaData meta = null
        (showRepoRes, meta) = JdbcUtils.executeToList(context.conn, showRepoSql)
        for (List<Object> row : showRepoRes) {
            if (row.size() > 1 && repository.equalsIgnoreCase(row[1].toString())) {
                JdbcUtils.executeToList(context.conn, dropRepoSql)
                break
            }
        }

        String createRepoSql = """
CREATE READ ONLY REPOSITORY `${repository}`
WITH S3 ON LOCATION "${location}"
PROPERTIES (
  "AWS_ENDPOINT" = "${endpoint}",
  "AWS_ACCESS_KEY" = "${ak}",
  "AWS_SECRET_KEY" = "${sk}",
  "AWS_REGION" = "${region}"
)
"""
        JdbcUtils.executeToList(context.conn, createRepoSql)
    }

    private void restore() {
        List<List<Object>> showTablesRes = null
        ResultSetMetaData meta = null

        if (CollectionUtils.isEmpty(tables)) {
            tables = new ArrayList<>()
            String showTables = """SHOW TABLES"""
            (showTablesRes, meta) = JdbcUtils.executeToList(context.conn, showTables)
            for (List<Object> tableRes : showTablesRes) {
                tables.add(tableRes[0].toString())
            }
        }
        if (CollectionUtils.isNotEmpty(tables)) {
            for (String t : tables) {
                String dropTableSql = """DROP TABLE IF EXISTS `${t}` FORCE"""
                JdbcUtils.executeToList(context.conn, dropTableSql)
                log.info("drop table '${t}'")
            }
        }

        String restoreSql = """
RESTORE SNAPSHOT `${getRealDbName()}`.`${snapshot}`
FROM ${repository}
"""
        if (CollectionUtils.isNotEmpty(tables)) {
            restoreSql += """
ON (
    `${tables.join("`,\n    `")}`
)
"""
        }
        restoreSql += """
PROPERTIES (
  "backup_timestamp" = "${timestamp}",
  "replication_num" = "${replicationNum}",
  "timeout" = "${timeout}"
)
"""
        JdbcUtils.executeToList(context.conn, restoreSql)
    }

    private void checkRestore() {
        String showRestoreSql = """
SHOW RESTORE FROM `${getRealDbName()}`
"""
        List<List<Object>> result = null
        ResultSetMetaData meta = null

        while (true) {
            (result, meta) = JdbcUtils.executeToList(context.conn, showRestoreSql)
            if (!result.empty && result[result.size() - 1].size() >= 5) {
                def status = result[result.size() - 1][4].toString()
                if ("CANCELLED".equalsIgnoreCase(status)) {
                    log.info("restore snapshot '${snapshot}' failed.")
                    throw new IllegalStateException("restore ${snapshot} failed.")
                } else if ("FINISHED".equalsIgnoreCase(status)) {
                    log.info("restore snapshot '${snapshot}' success.")
                    break
                }
            }
            sleep(10000)
        }
    }

    private String getRealDbName() {
        return context.config.getDbNameByFile(context.file)
    }
}
