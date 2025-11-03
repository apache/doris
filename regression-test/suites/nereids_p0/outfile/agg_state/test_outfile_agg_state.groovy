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
import org.apache.doris.regression.util.ExportTestHelper
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

suite("test_outfile_agg_state") {
    def hosts = []
    List<List<Object>> backends = sql("show backends");
    for (def b : backends) {
        hosts.add(b[1])
    }
    ExportTestHelper testHelper = new ExportTestHelper(hosts)

    sql "set enable_agg_state=true"
    sql "DROP TABLE IF EXISTS a_table"
    sql """
    create table a_table(
        k1 int null,
        k2 agg_state<max_by(int not null,int)> generic,
        k3 agg_state<group_concat(string)> generic
    )
    aggregate key (k1)
    distributed BY hash(k1) buckets 3
    properties("replication_num" = "1");
    """
    sql "insert into a_table values(1,max_by_state(3,1),group_concat_state('a'));"
    sql "insert into a_table values(1,max_by_state(2,2),group_concat_state('bb'));"
    sql "insert into a_table values(2,max_by_state(1,3),group_concat_state('ccc'));"

    qt_test "select k1,max_by_merge(k2),group_concat_merge(k3) from a_table group by k1 order by k1;"

    sql """select * from a_table into outfile "file://${testHelper.remoteDir}/e_" FORMAT AS PARQUET;"""
    testHelper.collect()

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

    def filePath=testHelper.localDir+"/*"
    cmd """
    curl --location-trusted -u ${context.config.jdbcUser}:${context.config.jdbcPassword} -H "format:PARQUET" -H "Expect:100-continue" -T ${filePath} -XPUT http://${context.config.feHttpAddress}/api/regression_test_nereids_p0_outfile_agg_state/a_table2/_stream_load
    """
    Thread.sleep(10000)
    qt_test "select k1,max_by_merge(k2),group_concat_merge(k3) from a_table2 group by k1 order by k1;"

    testHelper.close()
}
