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
import org.awaitility.Awaitility;
import static java.util.concurrent.TimeUnit.SECONDS;
import static groovy.test.GroovyAssert.shouldFail

suite("hdfs_load_default_file_format", "p0,external,kerberos,external_docker") {
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hdfsPort = context.config.otherConfigs.get("hive3HdfsPort")
    def defaultFS = "hdfs://${externalEnvIp}:${hdfsPort}"

    def table = "hdfs_load_default_file_format_tbl";
    def table2 = "hdfs_load_default_file_format_tbl2";

    def outfile_to_hdfs = { defaultFs, format, format_prop ->
        def outFilePath = "${defaultFs}/tmp/hdfs_load_default_file_format_"
        // select ... into outfile ...
        def res = sql """
            SELECT * FROM ${table}
            INTO OUTFILE "${outFilePath}"
            FORMAT AS ${format}
            PROPERTIES (
                ${format_prop}
                "fs.defaultFS" = "${defaultFs}"
            );
        """
        println res
        return res[0][3]
    }

    def hdfsLoad = { filePath, column_sep, defaultFs ->
        def dataCountResult = sql """
            SELECT count(*) FROM ${table}
        """
        sql """truncate table ${table2}"""
        def dataCount = dataCountResult[0][0]
        def label = "hdfs_load_label_" + System.currentTimeMillis()
        // test: do not specify the format, infer from file suffix.
        def load = sql """
            LOAD LABEL `${label}` (
            data infile ("${filePath}")
            into table ${table2}
            ${column_sep}
            (k1, k2))
            with hdfs
            (
               "fs.defaultFS" = "${defaultFs}"
            );
        """
        Awaitility.await().atMost(60, SECONDS).pollInterval(1, SECONDS).until({
            def loadResult = sql """
                show load where label = '${label}';
            """
            println loadResult

            if (null == loadResult || loadResult.isEmpty() || null == loadResult.get(0) || loadResult.get(0).size() < 3) {
                return false;
            }
            if (loadResult.get(0).get(2) == 'CANCELLED' || loadResult.get(0).get(2) == 'FAILED') {
                throw new RuntimeException("load failed")
            }

            return loadResult.get(0).get(2) == 'FINISHED'
        })


        def expectedCount = dataCount;
        Awaitility.await().atMost(5, SECONDS).pollInterval(1, SECONDS).until({
            def loadResult = sql """
                select count(*) from ${table2}
            """
            println "loadResult: ${loadResult}, expected: ${expectedCount}"
            return loadResult.get(0).get(0) == expectedCount
        })
    }

    sql """drop table if exists ${table}"""
    sql """drop table if exists ${table2}"""
    sql """create table ${table}
        (k1 int, k2 string) distributed by hash(k1) buckets 1
        properties("replication_num" = "1");
    """
    sql """create table ${table2} like ${table}"""

    sql """insert into ${table} values(1, "name");"""
    for (int i = 0; i < 10; i++) {
        sql """insert into ${table} select k1 + ${i}, concat(k2, k1 + ${i}) from ${table}"""
    }

    // outfile parquet and load
    def outfile = outfile_to_hdfs("hdfs://${externalEnvIp}:${hdfsPort}", "parquet", "");
    println outfile
    hdfsLoad(outfile, "", "hdfs://${externalEnvIp}:${hdfsPort}")

    // outfile csv and load
    outfile = outfile_to_hdfs("hdfs://${externalEnvIp}:${hdfsPort}", "csv", "\"column_separator\" = \"xx\",");
    println outfile
    hdfsLoad(outfile, "COLUMNS TERMINATED BY \"xx\"", "hdfs://${externalEnvIp}:${hdfsPort}")
}

