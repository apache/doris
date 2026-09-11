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

suite("test_outfile_agg_state_bitmap") {
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
        k2 agg_state<bitmap_union(bitmap not null)> generic
    )
    aggregate key (k1)
    distributed BY hash(k1) buckets 3
    properties("replication_num" = "1");
    """
    sql "insert into a_table values(1,bitmap_union_state(to_bitmap(1)));"
    sql "insert into a_table values(1,bitmap_union_state(to_bitmap(2)));"
    sql "insert into a_table values(2,bitmap_union_state(to_bitmap(3)));"

    qt_test "select k1,bitmap_to_string(bitmap_union_merge(k2)) from a_table group by k1 order by k1;"

    sql """select * from a_table into outfile "file://${testHelper.remoteDir}/tmp_" FORMAT AS PARQUET;"""
    testHelper.collect()

    sql "DROP TABLE IF EXISTS a_table2"
    sql """
    create table a_table2(
        k1 int null,
        k2 agg_state<bitmap_union(bitmap not null)> generic
    )
    aggregate key (k1)
    distributed BY hash(k1) buckets 3
    properties("replication_num" = "1");
    """

    // Raw serialized states cannot be cast from file columns to AGG_STATE.
    def files = new File(testHelper.localDir).listFiles().findAll { it.isFile() }
    assertTrue(!files.isEmpty())
    files.each { exportedFile ->
        streamLoad {
            table "a_table2"
            set "format", "parquet"
            file exportedFile.absolutePath
            check { result, exception, startTime, endTime ->
                assertTrue(exception == null)
                def response = parseJson(result)
                assertEquals("Fail", response.Status)
                assertTrue(response.Message.contains("cast"), response.Message)
            }
        }
    }
    // Copying typed states between tables remains supported.
    sql "insert into a_table2 select * from a_table"
    qt_test "select k1,bitmap_to_string(bitmap_union_merge(k2)) from a_table2 group by k1 order by k1;"
    testHelper.close()
}
