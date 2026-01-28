import java.util.concurrent.atomic.AtomicReference

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

suite("query_cache_with_context") {
    multi_sql """
        set enable_sql_cache=false;
        set enable_query_cache=false;
        drop table if exists query_cache_with_context;
        create table query_cache_with_context(id int, value decimal(26, 4)) properties('replication_num'='1');
        insert into query_cache_with_context values(1, 1), (2, 2), (3, 3);
        set enable_query_cache=true;
        """

    def getDigest = { def sqlStr ->
        AtomicReference<String> result = new AtomicReference<>()
        explain {
            sql sqlStr

            check {exp ->
                def digests = exp.split("\n").findAll { line -> line.contains("DIGEST") }
                if (!digests.isEmpty()) {
                    result.set(digests.get(0).split(":")[1].trim())
                }
            }
        }
        return result.get()
    }

    def test_session_variable_change = {
        sql "set enable_decimal256=true"
        def digest1 = getDigest("select id from query_cache_with_context group by id")
        sql "set enable_decimal256=false"
        def digest2 = getDigest("select id from query_cache_with_context group by id")
        assertNotEquals(digest1, digest2)
    }()

    def test_udf_function = {
        def jarPath = """${context.config.suitePath}/javaudf_p0/jars/java-udf-case-jar-with-dependencies.jar"""
        scp_udf_file_to_all_be(jarPath)
        sql("DROP FUNCTION IF EXISTS test_udf_with_query_cache(string, int, int);")
        sql """ CREATE FUNCTION test_udf_with_query_cache(string, int, int) RETURNS string PROPERTIES (
                                "file"="file://${jarPath}",
                                "symbol"="org.apache.doris.udf.StringTest",
                                "type"="JAVA_UDF"
                            ); """
        String digest = getDigest("select test_udf_with_query_cache(id, 2, 3) from query_cache_with_context group by 1")
        assertEquals(null, digest)
    }()
}