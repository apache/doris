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

suite("test_group_commit_wal_limit") {
    def db= "regression_test_load_p0_stream_load"
    def tableName = "test_group_commit_wal_limit"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                k bigint,  
                v string
                )  
                UNIQUE KEY(k)  
                DISTRIBUTED BY HASH (k) BUCKETS 32  
                PROPERTIES(  
                "replication_num" = "1"
            );
    """
    // streamload
    // normal case
    StringBuilder strBuilder = new StringBuilder()
    strBuilder.append("curl --location-trusted -u " + context.config.jdbcUser + ":" + context.config.jdbcPassword)
    strBuilder.append(" -H \"group_commit:async_mode\" -H \"column_separator:,\" " )
    strBuilder.append(" -H \"compress_type:gz\" -H \"format:csv\" " )
    strBuilder.append(" -T " + context.config.dataPath + "/load_p0/stream_load/test_group_commit_wal_limit.csv.gz")
    strBuilder.append(" http://" + context.config.feHttpAddress + "/api/${db}/${tableName}/_stream_load")

    String command = strBuilder.toString()
    logger.info("command is " + command)
    def process = ['bash','-c',command].execute() 
    def code = process.waitFor()
    assertEquals(code, 0)
    def out = process.text
    logger.info("out is " + out )
    assertTrue(out.contains('group_commit'))

    // httpload 
    // normal case
    strBuilder = new StringBuilder()
    strBuilder.append("curl -v --location-trusted -u " + context.config.jdbcUser + ":" + context.config.jdbcPassword)
    String sql = " -H \"sql:insert into " + db + "." + tableName + " (k,v) select c1, c2 from http_stream(\\\"format\\\" = \\\"csv\\\", \\\"column_separator\\\" = \\\",\\\", \\\"compress_type\\\" = \\\"gz\\\" ) \" "
    strBuilder.append(sql)
    strBuilder.append(" -H \"group_commit:async_mode\"") 
    strBuilder.append(" -T " + context.config.dataPath + "/load_p0/stream_load/test_group_commit_wal_limit.csv.gz")
    strBuilder.append(" http://" + context.config.feHttpAddress + "/api/_http_stream")

    command = strBuilder.toString()
    logger.info("command is " + command)
    process = ['bash','-c',command].execute() 
    code = process.waitFor()
    assertEquals(code, 0)
    out = process.text
    logger.info("out is " + out )
    assertTrue(out.contains('group_commit'))
}
