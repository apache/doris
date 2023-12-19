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

suite("test_insert_with_aggregation_memtable", "nonConcurrent") {
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    def backendId_to_params = [string:[:]]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def set_be_param = { paramName, paramValue ->
        // for eache be node, set paramName=paramValue
        for (String id in backendId_to_backendIP.keySet()) {
		    def beIp = backendId_to_backendIP.get(id)
		    def bePort = backendId_to_backendHttpPort.get(id)
		    def (code, out, err) = curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", beIp, bePort, paramName, paramValue))
		    assertTrue(out.contains("OK"))
	    }
    }

    def reset_be_param = { paramName ->
        // for eache be node, reset paramName to default
        for (String id in backendId_to_backendIP.keySet()) {
            def beIp = backendId_to_backendIP.get(id)
            def bePort = backendId_to_backendHttpPort.get(id)
            def original_value = backendId_to_params.get(id).get(paramName)
            def (code, out, err) = curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", beIp, bePort, paramName, original_value))
            assertTrue(out.contains("OK"))
        }
    }

    def get_be_param = { paramName ->
        // for eache be node, get param value by default
        def paramValue = ""
        for (String id in backendId_to_backendIP.keySet()) {
            def beIp = backendId_to_backendIP.get(id)
            def bePort = backendId_to_backendHttpPort.get(id)
            // get the config value from be
            def (code, out, err) = curl("GET", String.format("http://%s:%s/api/show_config?conf_item=%s", beIp, bePort, paramName))
            assertTrue(code == 0)
            assertTrue(out.contains(paramName))
            // parsing
            def resultList = parseJson(out)[0]
            assertTrue(resultList.size() == 4)
            // get original value
            paramValue = resultList[2]
            backendId_to_params.get(id, [:]).put(paramName, paramValue)
        }
    }
    
    def testTable = "test_memtable_enable_with_aggregate"
    sql """ DROP TABLE IF EXISTS ${testTable}"""
    def testTableDDL = """
        create table ${testTable} 
        (
            `id` LARGEINT NOT NULL,
            `k1` DATE NOT NULL,
            `k2` VARCHAR(20),
            `k3` SMALLINT,
            `k4` TINYINT,
            `k5` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00",
            `k6` BIGINT SUM DEFAULT "0",
            `k7` INT MAX DEFAULT "0",
            `k8` INT MIN DEFAULT "99999"
        )
        AGGREGATE KEY(`id`, `k1`, `k2`, `k3`, `k4`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    def insert_sql = """
        insert into ${testTable} values
        (10000,"2017-10-01","北京",20,0,"2017-10-01 06:00:00",20,10,10),
        (10000,"2017-10-01","北京",20,0,"2017-10-01 07:00:00",15,2,2),
        (10001,"2017-10-01","北京",30,1,"2017-10-01 17:05:45",2,22,22),
        (10002,"2017-10-02","上海",20,1,"2017-10-02 12:59:12",200,5,5),
        (10003,"2017-10-02","广州",32,0,"2017-10-02 11:20:00",30,11,11),
        (10004,"2017-10-01","深圳",35,0,"2017-10-01 10:00:15",100,3,3),
        (10004,"2017-10-03","深圳",35,0,"2017-10-03 10:20:22",11,6,6);
    """
  
    sql testTableDDL
    sql "sync"
    sql insert_sql
    sql "sync"
    qt_sql "select * from ${testTable} order by id asc"
    
    // store the original value
    get_be_param("enable_shrink_memory")
    get_be_param("write_buffer_size_for_agg")

    // the original value is false
    set_be_param("enable_shrink_memory", "true")
    // the original value is 400MB
    set_be_param("write_buffer_size_for_agg", "512") // change it to 0.5KB
    sql """ DROP TABLE IF EXISTS ${testTable}"""
    sql testTableDDL
    sql "sync"
    sql insert_sql
    sql "sync"
    qt_sql "select * from ${testTable} order by id asc"

    // test with mv
    def table_name = "agg_shrink"
    sql "DROP TABLE IF EXISTS ${table_name}"
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            k bigint,
            v text 
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 4
        properties("replication_num" = "1");
    """
    set_be_param("write_buffer_size_for_agg", "10240") // change it to 10KB
    sql """INSERT INTO ${table_name} SELECT *, '{"k1":1, "k2": "hello world", "k3" : [1234], "k4" : 1.10000, "k5" : [[123]]}' FROM numbers("number" = "4096")"""
    sql """INSERT INTO ${table_name} SELECT k, v from ${table_name}"""
    sql """INSERT INTO ${table_name} SELECT k, v from ${table_name}"""
    createMV("""create materialized view var_cnt as select k, count(k) from ${table_name} group by k""")    
    sql """INSERT INTO ${table_name} SELECT k, v from ${table_name} limit 8101"""
    // insert with no duplicate
    sql """INSERT INTO ${table_name} SELECT *, '{"k1":1, "k2": "hello world", "k3" : [1234], "k4" : 1.10000, "k5" : [[123]]}' FROM numbers("number" = "4096"); """

    reset_be_param("enable_shrink_memory")
    reset_be_param("write_buffer_size_for_agg")

}