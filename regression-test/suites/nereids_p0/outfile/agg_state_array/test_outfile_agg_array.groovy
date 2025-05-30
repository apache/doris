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

import org.codehaus.groovy.runtime.IOGroovyMethods

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

suite("test_outfile_agg_state_array") {
    def outFilePath = """./tmp/test_outfile_agg_state_array"""
    File path = new File(outFilePath)
    path.deleteDir()
    path.mkdirs()

    sql "set enable_agg_state=true"
    sql "DROP TABLE IF EXISTS a_table"
    sql """
    create table a_table(
        k1 int null,
        k2 agg_state<array_agg(int)> generic
    )
    aggregate key (k1)
    distributed BY hash(k1) buckets 3
    properties("replication_num" = "1");
    """
    sql "insert into a_table values(1,array_agg_state(1));"
    sql "insert into a_table values(1,array_agg_state(2));"
    sql "insert into a_table values(2,array_agg_state(3));"

    qt_test "select k1,array_agg_merge(k2) from a_table group by k1 order by k1;"

    sql """select * from a_table into outfile "file://${path.getAbsolutePath()}/tmp" FORMAT AS PARQUET;"""

    sql "DROP TABLE IF EXISTS a_table2"
    sql """
    create table a_table2(
        k1 int null,
        k2 agg_state<max_by(int not null,int)> generic,
        k3 agg_state<group_concat(string)> generic
    )
    aggregate key (k1)
    distributed BY hash(k1) buckets 3
    properties("replication_num" = "1");
    """

    def filePath=path.getAbsolutePath()+"/tmp*"
    cmd """
    curl --location-trusted -u ${context.config.jdbcUser}:${context.config.jdbcPassword} -H "format:PARQUET" -H "Expect:100-continue" -T ${filePath} http://${context.config.feHttpAddress}/api/regression_test_nereids_p0_outfile_agg_state_array/a_table2/_stream_load
    """
    Thread.sleep(10000)
    qt_test "select k1,max_by_merge(k2),group_concat_merge(k3) from a_table2 group by k1 order by k1;"
}
