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

suite("test_insert_with_index", "p0") {

     def set_be_config = { key, value ->
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
    }
    def test = { format -> 
        def srcName = "src_table"
        def dstName = "dst_table"
        sql """ DROP TABLE IF EXISTS ${srcName}; """
        sql """
            CREATE TABLE ${srcName} (
                k bigint,
                v variant,
                INDEX idx_v (`v`) USING INVERTED PROPERTIES("parser" = "english") COMMENT ''
            ) ENGINE=OLAP
            DUPLICATE KEY(`k`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k`) BUCKETS 2
            PROPERTIES ( "replication_num" = "1", "inverted_index_storage_format" = ${format});
        """

        sql """insert into ${srcName} values(1, '{"a" : 123, "b" : "xxxyyy", "c" : 111999111}')"""
        sql """insert into ${srcName} values(2, '{"a" : 18811, "b" : "hello world", "c" : 1181111}')"""
        sql """insert into ${srcName} values(3, '{"a" : 18811, "b" : "hello wworld", "c" : 11111}')"""
        sql """insert into ${srcName} values(4, '{"a" : 1234, "b" : "hello xxx world", "c" : 8181111}')"""
        qt_sql_2 """select * from ${srcName} where cast(v["a"] as smallint) > 123 and cast(v["b"] as string) match 'hello' and cast(v["c"] as int) > 1024 order by k"""
        sql """insert into ${srcName} values(5, '{"a" : 123456789, "b" : 123456, "c" : 8181111}')"""
        qt_sql_3 """select * from ${srcName} where cast(v["a"] as int) > 123 and cast(v["b"] as string) match 'hello' and cast(v["c"] as int) > 11111 order by k"""

        sql """ DROP TABLE IF EXISTS ${dstName}; """
        sql """
            CREATE TABLE ${dstName} (
                k bigint,
                v variant,
                INDEX idx_v (`v`) USING INVERTED PROPERTIES("parser" = "english") COMMENT ''
            ) ENGINE=OLAP
            DUPLICATE KEY(`k`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k`) BUCKETS 2
            PROPERTIES ( "replication_num" = "1", "inverted_index_storage_format" = ${format});
        """
        sql """ insert into ${dstName} select * from ${srcName}"""
        qt_sql_2 """select * from ${srcName} where cast(v["a"] as smallint) > 123 and cast(v["b"] as string) match 'hello' and cast(v["c"] as int) > 1024 order by k"""
        qt_sql_3 """select * from ${srcName} where cast(v["a"] as int) > 123 and cast(v["b"] as string) match 'hello' and cast(v["c"] as int) > 11111 order by k"""
        sql """ DROP TABLE IF EXISTS ${dstName}; """
        sql """ DROP TABLE IF EXISTS ${srcName}; """
    }

    set_be_config("inverted_index_ram_dir_enable", "true")
    test.call("V1")
    test.call("V2")
    set_be_config("inverted_index_ram_dir_enable", "false")
    test.call("V1")
    test.call("V2")
    set_be_config("inverted_index_ram_dir_enable", "true")
}