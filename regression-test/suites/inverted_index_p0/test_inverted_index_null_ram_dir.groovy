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

// This test out is exactly the same as test_inverted_index_null.groovy
suite("test_inverted_index_null_ram_dir") {

    def tableName = "test_inverted_index_null_ram_dir"
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def set_be_config = { key, value ->
        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
    }

    def check_config = { String key, String value ->
        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))
            logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def configList = parseJson(out.trim())
            assert configList instanceof List
            for (Object ele in (List) configList) {
                assert ele instanceof List<String>
                if (((List<String>) ele)[0] == key) {
                    assertEquals(value, ((List<String>) ele)[2])
                }
            }
        }
    }

    boolean invertedIndexRamDirEnable = false
    boolean has_update_be_config = false
    try {
        String backend_id;
        backend_id = backendId_to_backendIP.keySet()[0]
        def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))
        
        logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def configList = parseJson(out.trim())
        assert configList instanceof List

        for (Object ele in (List) configList) {
            assert ele instanceof List<String>
            if (((List<String>) ele)[0] == "inverted_index_ram_dir_enable") {
                invertedIndexRamDirEnable = Boolean.parseBoolean(((List<String>) ele)[2])
                logger.info("inverted_index_ram_dir_enable: ${((List<String>) ele)[2]}")
            }
        }
        set_be_config.call("inverted_index_ram_dir_enable", "true")
        has_update_be_config = true
        // check updated config
        check_config.call("inverted_index_ram_dir_enable", "true");


        sql "drop table if exists ${tableName}"

        sql """
          CREATE TABLE IF NOT EXISTS `${tableName}` (
          `id` int NULL COMMENT "",
          `city` varchar(20) NULL COMMENT "",
          `addr` varchar(20) NULL COMMENT "",
          `name` varchar(20) NULL COMMENT "",
          `compy` varchar(20) NULL COMMENT "",
          `n` int NULL COMMENT "",
          INDEX idx_city(city) USING INVERTED,
          INDEX idx_addr(addr) USING INVERTED PROPERTIES("parser"="english"),
          INDEX idx_n(n) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "in_memory" = "false",
          "storage_format" = "V2"
        )
        """

        sql """insert into ${tableName} values
                (1,null,'addr qie3','yy','lj',100),
                (2,null,'hehe',null,'lala',200),
                (3,'beijing','addr xuanwu','wugui',null,300),
                (4,'beijing','addr fengtai','fengtai1','fengtai2',null),
                (5,'beijing','addr chaoyang','wangjing','donghuqu',500),
                (6,'shanghai','hehe',null,'haha',null),
                (7,'tengxun','qie','addr gg','lj',null),
                (8,'tengxun2','qie',null,'lj',800)
        """
        sql """ set enable_common_expr_pushdown = true """

        // select all data
        qt_select_0 "SELECT * FROM ${tableName} ORDER BY id"

        // test IS NULL , IS NOT NULL
        qt_select_is_null_1 "SELECT * FROM ${tableName} WHERE city IS NULL ORDER BY id"
        qt_select_is_null_2 "SELECT * FROM ${tableName} WHERE city IS NOT NULL ORDER BY id"
        qt_select_is_null_3 "SELECT * FROM ${tableName} WHERE addr IS NULL ORDER BY id"
        qt_select_is_null_4 "SELECT * FROM ${tableName} WHERE addr IS NOT NULL ORDER BY id"
        qt_select_is_null_5 "SELECT * FROM ${tableName} WHERE n IS NULL ORDER BY id"
        qt_select_is_null_6 "SELECT * FROM ${tableName} WHERE n IS NOT NULL ORDER BY id"

        // test compare predicate
        qt_select_compare_11 "SELECT * FROM ${tableName} WHERE city  = 'shanghai' ORDER BY id"
        qt_select_compare_12 "SELECT * FROM ${tableName} WHERE city != 'shanghai' ORDER BY id"
        qt_select_compare_13 "SELECT * FROM ${tableName} WHERE city <= 'shanghai' ORDER BY id"
        qt_select_compare_14 "SELECT * FROM ${tableName} WHERE city >= 'shanghai' ORDER BY id"

        qt_select_compare_21 "SELECT * FROM ${tableName} WHERE n  = 500 ORDER BY id"
        qt_select_compare_22 "SELECT * FROM ${tableName} WHERE n != 500 ORDER BY id"
        qt_select_compare_23 "SELECT * FROM ${tableName} WHERE n <= 500 ORDER BY id"
        qt_select_compare_24 "SELECT * FROM ${tableName} WHERE n >= 500 ORDER BY id"

        // test in predicates
        qt_select_in_1 "SELECT * FROM ${tableName} WHERE city IN ('shanghai', 'beijing') ORDER BY id"
        qt_select_in_2 "SELECT * FROM ${tableName} WHERE city NOT IN ('shanghai', 'beijing') ORDER BY id"
        qt_select_in_3 "SELECT * FROM ${tableName} WHERE n IN (100, 300) ORDER BY id"
        qt_select_in_4 "SELECT * FROM ${tableName} WHERE n NOT IN (100, 300) ORDER BY id"

        // test match predicates
        qt_select_match_1 "SELECT * FROM ${tableName} WHERE addr MATCH_ANY 'addr fengtai' ORDER BY id"
        qt_select_match_2 "SELECT * FROM ${tableName} WHERE addr MATCH_ALL 'addr fengtai' ORDER BY id"

    } finally {
        if (has_update_be_config) {
            set_be_config.call("inverted_index_ram_dir_enable", invertedIndexRamDirEnable.toString())
        }
    }
}

