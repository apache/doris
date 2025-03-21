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

import org.junit.jupiter.api.Assertions

suite("docs/data-operate/import/import-way/mysql-load-manual.md") {
    def is_linux = System.getProperty("os.name").toLowerCase().contains("linux")
    def run_cmd = { String cmdText ->
        try {
            println(cmd cmdText)
            return true
        } catch (Exception ignored) {
            return false
        }
    }
    if (!is_linux) {
        logger.warn("don't run this case if not in Linux")
        return
    }
    logger.info("check if has installed mysql cmd")
    if (run_cmd("mysql --help 2>&1 >/dev/null") || run_cmd("yum -y install mysql")) {
        logger.info("mysql cmd can work properly, go continue")
    } else {
        logger.warn("could not install mysql cmd client, skip this case")
        return
    }
    def writeToFile = {String path, String data ->
        OutputStreamWriter w = null
        try {
            w = new OutputStreamWriter(new FileOutputStream(path))
            w.write(data)
            w.flush()
            w.close()
        } finally {
            if (w != null) w.close()
        }
    }
    def load_local = {String sql ->
        if (is_linux) {
            var output = cmd """
                cd ${context.file.parent} && \\ 
                cat << EOF | mysql --local-infile -vvv -h ${getMasterIp()} -P ${getMasterPort("mysql")} -u ${context.config.jdbcUser} ${context.config.jdbcPassword.isEmpty() ? "" : "-p  ${context.config.jdbcPassword}"} -D testdb
${sql}
EOF
            """
            println(output)
        }
    }

    try {
        multi_sql """
            CREATE DATABASE IF NOT EXISTS testdb;
            USE testdb;
            DROP TABLE IF EXISTS t1;
        """
        sql """
            CREATE TABLE testdb.t1 (
                pk     INT, 
                v1     INT SUM
            ) AGGREGATE KEY (pk) 
            DISTRIBUTED BY hash (pk)
            PROPERTIES ("replication_num" = "1");
        """
        load_local """
            LOAD DATA LOCAL
            INFILE 'client_local.csv'
            INTO TABLE testdb.t1
            COLUMNS TERMINATED BY ','
            LINES TERMINATED BY '\\n';
        """

        try {
            sql """show load warnings where label='b612907c-ccf4-4ac2-82fe-107ece655f0f';"""
        } catch (Exception e) {
            if (!e.getMessage().contains("job is not exist")) {
                logger.error("occurring other error is not in expected")
                throw e
            }
        }

        sql "CREATE DATABASE IF NOT EXISTS testDb"
        sql "DROP TABLE IF EXISTS testDb.testTbl"
        sql """
            CREATE TABLE testDb.testTbl (
                k1     INT, 
                k2     INT, 
                v1     INT SUM
            ) AGGREGATE KEY (k1,k2)
            PARTITION BY RANGE(k1) (
                PARTITION p1 VALUES LESS THAN (1),
                PARTITION p2 VALUES LESS THAN (2),
                PARTITION p3 VALUES LESS THAN (3)
            )
            DISTRIBUTED BY hash (k1)
            PROPERTIES ("replication_num" = "1");
        """
        writeToFile("${context.file.parent}/testData", "1\t2\t3\n")
        load_local """
            LOAD DATA LOCAL
            INFILE 'testData'
            INTO TABLE testDb.testTbl
            PROPERTIES ("timeout"="100");
        """
        load_local """
            LOAD DATA LOCAL
            INFILE 'testData'
            INTO TABLE testDb.testTbl
            PROPERTIES ("max_filter_ratio"="0.2");
        """
        load_local """
            LOAD DATA LOCAL
            INFILE 'testData'
            INTO TABLE testDb.testTbl
            (k2, k1, v1);
        """
        writeToFile("${context.file.parent}/testData", "1,2,3\n")
        load_local """
            LOAD DATA LOCAL
            INFILE 'testData'
            INTO TABLE testDb.testTbl
            COLUMNS TERMINATED BY ','
            LINES TERMINATED BY '\\n';
        """
        writeToFile("${context.file.parent}/testData", "1\t2\t3\n")
        load_local """
            LOAD DATA LOCAL
            INFILE 'testData'
            INTO TABLE testDb.testTbl
            PARTITION (p1, p2);
        """
        load_local """
            LOAD DATA LOCAL
            INFILE 'testData'
            INTO TABLE testDb.testTbl
            PROPERTIES ("timezone"="Africa/Abidjan");
        """
        load_local """
            LOAD DATA LOCAL
            INFILE 'testData'
            INTO TABLE testDb.testTbl
            PROPERTIES ("exec_mem_limit"="10737418240");
        """
    } catch (Throwable t) {
        Assertions.fail("examples in docs/data-operate/import/import-way/mysql-load-manual.md failed to exec, please fix it", t)
    }
}
