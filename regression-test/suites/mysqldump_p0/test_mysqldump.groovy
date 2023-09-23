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

suite("test_mysqldump") { suite ->

    def executeMySQLCommand = { String command ->
        def cmds = ["/bin/bash", "-c", command]
        logger.info("Execute: ${cmds}".toString())
        Process p = cmds.execute()

        def errMsg = new StringBuilder()
        def msg = new StringBuilder()
        p.waitForProcessOutput(msg, errMsg)

        assert errMsg.length() == 0: "error occurred!" + errMsg
        assert p.exitValue() == 0
    }

    def dbName = context.config.getDbNameByFile(context.file)
    def tablePrefix = "test_mysqldump_table_"

    for (int i = 1; i <= 2; ++i) {
        def tableName = tablePrefix + i.toString();
        sql "DROP TABLE IF EXISTS ${tableName}"
    }

    for (int i = 1; i <= 2; ++i) {
        def tableName = tablePrefix + i.toString();
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` int(11) NULL,
            `k2` tinyint(4) NULL,
            `k3` smallint(6) NULL,
            `k4` bigint(20) NULL,
            `k5` largeint(40) NULL,
            `k6` float NULL,
            `k7` double NULL,
            `k8` decimal(9, 0) NULL,
            `k9` char(10) NULL,
            `k10` varchar(1024) NULL,
            `k11` text NULL,
            `k12` date NULL,
            `k13` datetime NULL
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
            """
        // load all columns
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','

            file 'data.csv'
            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(25, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }
    }

    qt_sql "select count(*) from information_schema.tables where table_schema = '${dbName}'"
    for (int i = 1; i <= 2; ++i) {
        def tableName = tablePrefix + i.toString();
        qt_sql "select count(*) from ${tableName}"
        qt_sql "select * from ${tableName} order by k1 limit 5"
    }

    // use mysqldump
    String jdbcUrlConfig = context.config.jdbcUrl;
    String tempString = jdbcUrlConfig.substring(jdbcUrlConfig.indexOf("jdbc:mysql://") + 13);
    String mysqlHost = tempString.substring(0, tempString.indexOf(":"));
    String mysqlPort = tempString.substring(tempString.indexOf(":") + 1, tempString.indexOf("/"));
    def filePath = "/tmp/mysql_${dbName}.db" 
    String cmdMySQLdump = "mysqldump -uroot -h" + mysqlHost + " -P" + mysqlPort + " --no-tablespaces --databases ${dbName} > ${filePath}";
    executeMySQLCommand(cmdMySQLdump);
                                      
    // restore
    for (int i = 1; i <= 2; ++i) {
        def tableName = tablePrefix + i.toString();
        sql "DROP TABLE IF EXISTS ${tableName}"
    }
    sql "DROP DATABASE IF EXISTS ${dbName}"

    String cmdRestore = "mysql -uroot -h" + mysqlHost + " -P" + mysqlPort + " < ${filePath}";
    executeMySQLCommand(cmdRestore);
    qt_sql "select count(*) from information_schema.tables where table_schema = '${dbName}'"
    for (int i = 1; i <= 2; ++i) {
        def tableName = tablePrefix + i.toString();
        qt_sql "select count(*) from ${tableName}"
        qt_sql "select * from ${tableName} order by k1 limit 5"
    }
}