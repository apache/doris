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

suite("test_load_with_functions") {
    sql "DROP TABLE IF EXISTS `test_table`"
    sql """
        create table IF NOT EXISTS `test_table` (
                `id` int NULL,
                `arr` array<text> NULL
        ) ENGINE=OLAP
          DUPLICATE KEY(`id`)  distributed by hash(`id`) buckets 1 properties("replication_num" = "1");
    """

    //  curl -v --location-trusted -u root: -H "format:json" -H "strip_outer_array:true" -H "read_json_by_line: true" -H "group_mode: sync_mode" -H "columns:arr=ARRAY_MAP(x -> IFNULL(x, '$'), arr)" -T test.json
    streamLoad {
        table "test_table"
        set 'strip_outer_array', 'true'
        set 'read_json_by_line', 'true'
        set 'group_mode', 'sync_mode'
        set 'columns', 'arr=ARRAY_MAP(x -> IFNULL(x, \'$\'), arr)'
        set 'format', 'json'
        file "test.json"
        time 60

        // if declared a check callback, the default check condition will ignore.
        // So you must check all condition
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
        }
    }
	
    // test array_map with non-array arg for nereids which should throw exception
    // literal

    test {
        sql """
            select array_map(x -> x is null, "sss");
        """
        exception "lambda argument must be array"
    }
    // column
    sql """ insert into test_table values(1, ["a", "b", "c"]) """
    sql """ insert into test_table values(2, ["a", "b", "c"]) """

    
    test {
        sql """
            select array_map(x -> x is null, id) from test_table;
        """
        exception "lambda argument must be array"
    }     


    test {
        sql """
            select array_map(x -> x is null, arr[0]) from test_table;
        """
        exception "lambda argument must be array"
    } 

}

