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

suite("regression_test_variant_var_index", "p0, nonConcurrent"){
    def table_name = "var_index"
    sql """ set default_variant_enable_typed_paths_to_sparse = false """
    sql "DROP TABLE IF EXISTS var_index"
    sql """
        CREATE TABLE IF NOT EXISTS var_index (
            k bigint,
            v variant<'timestamp' : double, 'a' : int, 'b' : string, 'c' : int>,
            INDEX idx_var(v) USING INVERTED  PROPERTIES("parser" = "english") COMMENT ''
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1 
        properties("replication_num" = "1", "disable_auto_compaction" = "true");
    """

    sql """insert into var_index values(1, '{"a" : 123, "b" : "xxxyyy", "c" : 111999111}')"""
    sql """insert into var_index values(2, '{"a" : 18811, "b" : "hello world", "c" : 1181111}')"""
    sql """insert into var_index values(3, '{"a" : 18811, "b" : "hello wworld", "c" : 11111}')"""
    sql """insert into var_index values(4, '{"a" : 1234, "b" : "hello xxx world", "c" : 8181111}')"""
    qt_sql """select * from var_index where cast(v["a"] as smallint) > 123 and cast(v["b"] as string) match 'hello' and cast(v["c"] as int) > 1024 order by k"""
    trigger_and_wait_compaction(table_name, "full")
    sql """ set enable_common_expr_pushdown = true """
    sql """set enable_match_without_inverted_index = false""" 
    qt_sql """select * from var_index where cast(v["a"] as smallint) > 123 and cast(v["b"] as string) match 'hello' and cast(v["c"] as int) > 1024 order by k"""
    sql """set enable_match_without_inverted_index = true""" 
    sql """insert into var_index values(5, '{"a" : 123456789, "b" : 123456, "c" : 8181111}')"""
    qt_sql """select * from var_index where cast(v["a"] as int) > 123 and cast(v["b"] as string) match 'hello' and cast(v["c"] as int) > 11111 order by k"""
    // insert double/float/array/json
    sql """insert into var_index values(6, '{"timestamp": 1713283200.060359}')"""
    sql """insert into var_index values(7, '{"timestamp": 17.0}')"""
    sql """insert into var_index values(8, '{"arr": [123]}')"""
    sql """insert into var_index values(9, '{"timestamp": 17.0}'),(10, '{"timestamp": "17.0"}')"""
    sql """insert into var_index values(11, '{"nested": [{"a" : 1}]}'),(11, '{"nested": [{"b" : "1024"}]}')"""
    trigger_and_wait_compaction(table_name, "full")
    qt_sql "select * from var_index order by k limit 15"

    sql "DROP TABLE IF EXISTS var_index"
    boolean findException = false
    try {
        sql """
            CREATE TABLE IF NOT EXISTS var_index (
                k bigint,
                v variant,
                INDEX idx_var(v) USING INVERTED  PROPERTIES("parser" = "english") COMMENT ''
            )
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS 1 
            properties("replication_num" = "1", "disable_auto_compaction" = "true", "inverted_index_storage_format" = "V1");
        """
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("not supported in inverted index format V1"))
        findException = true
    }
    assertTrue(findException)

    findException = false
    sql """
        CREATE TABLE IF NOT EXISTS var_index (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1 
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "inverted_index_storage_format" = "V1");
    """

    try {
        sql """ALTER TABLE var_index ADD INDEX idx_var(v) USING INVERTED"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("not supported in inverted index format V1"))
        findException = true
    }
    assertTrue(findException)

    setFeConfigTemporary([enable_inverted_index_v1_for_variant: true]) {
        if (isCloudMode()) {
            sql "DROP TABLE IF EXISTS var_index"
            try {
                sql """
                    CREATE TABLE IF NOT EXISTS var_index (
                        k bigint,
                        v variant,
                        INDEX idx_var(v) USING INVERTED  PROPERTIES("parser" = "english") COMMENT ''
                    )
                    DUPLICATE KEY(`k`)
                    DISTRIBUTED BY HASH(k) BUCKETS 1 
                    properties("replication_num" = "1", "disable_auto_compaction" = "true", "inverted_index_storage_format" = "V1");
                """
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("not supported in inverted index format V1"))
            }

            sql """
                CREATE TABLE IF NOT EXISTS var_index (
                    k bigint,
                    v variant
                )
                DUPLICATE KEY(`k`)
                DISTRIBUTED BY HASH(k) BUCKETS 1 
                properties("replication_num" = "1", "disable_auto_compaction" = "true", "inverted_index_storage_format" = "V1");
            """

            try {
                sql """ALTER TABLE var_index ADD INDEX idx_var(v) USING INVERTED"""
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("not supported in inverted index format V1"))
            }
        } else {
            sql """
            CREATE TABLE IF NOT EXISTS var_index (
                    k bigint,
                    v variant,
                    INDEX idx_var(v) USING INVERTED  PROPERTIES("parser" = "english") COMMENT ''
                )
                DUPLICATE KEY(`k`)
                DISTRIBUTED BY HASH(k) BUCKETS 1 
                properties("replication_num" = "1", "disable_auto_compaction" = "true", "inverted_index_storage_format" = "V1");
            """

            sql "DROP TABLE IF EXISTS var_index"
            sql """
                CREATE TABLE IF NOT EXISTS var_index (
                    k bigint,
                    v variant
                )
                DUPLICATE KEY(`k`)
                DISTRIBUTED BY HASH(k) BUCKETS 1 
                properties("replication_num" = "1", "disable_auto_compaction" = "true", "inverted_index_storage_format" = "V1");
            """
            sql """ALTER TABLE var_index ADD INDEX idx_var(v) USING INVERTED"""
            test {
                sql """ build index idx_var on var_index"""
                exception "The idx_var index can not be built on the v column, because it is a variant type column"
            }
        }
        
    }

    sql """ DROP TABLE IF EXISTS ${table_name} """
    sql """
    CREATE TABLE IF NOT EXISTS ${table_name} (
        k bigint,
        v variant<properties("variant_max_subcolumns_count" = "0")>,
        INDEX idx_var(v) USING INVERTED  PROPERTIES("parser" = "english") COMMENT '',
        INDEX idx_var2(v) USING INVERTED
    )
    DUPLICATE KEY(`k`)
    DISTRIBUTED BY HASH(k) BUCKETS 1 
    properties("replication_num" = "1", "disable_auto_compaction" = "true");
    """

    sql """insert into var_index values(1, '{"name": "张三", "age": 18}')"""
    sql """ select * from var_index """

    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def wait_for_latest_op_on_table_finish = { table, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(3000) // wait change table state to normal
                logger.info(table + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    def get_indeces_count = {
        def tablets = sql_return_maparray """ show tablets from ${table_name}; """

        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        String tablet_id = tablets[0].TabletId
        String backend_id = tablets[0].BackendId
        String ip = backendId_to_backendIP.get(backend_id)
        String port = backendId_to_backendHttpPort.get(backend_id)
        def (code, out, err) = http_client("GET", String.format("http://%s:%s/api/show_nested_index_file?tablet_id=%s", ip, port, tablet_id))
        logger.info("Run show_nested_index_file_on_tablet: code=" + code + ", out=" + out + ", err=" + err)
        if (parseJson(out.trim()).status == "E-6003") {
            return 0
        }
        def rowset_count = parseJson(out.trim()).rowsets.size();
        def indices_count = 0
        for (def rowset in parseJson(out.trim()).rowsets) {
            for (int i = 0; i < rowset.segments.size(); i++) {
                def segment = rowset.segments[i]
                assertEquals(i, segment.segment_id)
                if (segment.indices != null) {
                    indices_count += segment.indices.size()
                }
            }
        }
        return indices_count
    }

    assertEquals(3, get_indeces_count())

    sql """ drop index idx_var2 on ${table_name} """
    wait_for_latest_op_on_table_finish(table_name, timeout)
    sql """ insert into ${table_name} values(2, '{"name": "李四", "age": 20}') """
    sql """ select * from ${table_name} """
    if (isCloudMode()) {
        assertEquals(4, get_indeces_count())
    } else {
        assertEquals(5, get_indeces_count())
    }
    

    sql """ insert into ${table_name} values(2, '{"name": "李四", "age": 20}') """
    sql """ insert into ${table_name} values(2, '{"name": "李四", "age": 20}') """
    sql """ insert into ${table_name} values(2, '{"name": "李四", "age": 20}') """
    sql """ select * from ${table_name} """
    if (isCloudMode()) {
        assertEquals(10, get_indeces_count())
    } else {
        assertEquals(11, get_indeces_count())
    }

    sql """ drop index idx_var on ${table_name} """
    wait_for_latest_op_on_table_finish(table_name, timeout)
    sql """ insert into ${table_name} values(2, '{"name": "李四", "age": 20}') """
    sql """ select * from ${table_name} """
    trigger_and_wait_compaction(table_name, "full")
    assertEquals(0, get_indeces_count())
}