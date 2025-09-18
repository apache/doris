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

import org.apache.doris.regression.suite.ClusterOptions

suite("test_load", "docker") {
    def options = new ClusterOptions()
    options.beConfigs += [
        'delete_bitmap_store_version=2',
        'delete_bitmap_max_bytes_store_in_fdb=-1',
        'enable_delete_bitmap_store_v2_check_correctness=true',
        'enable_java_support=false'
    ]
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true
    docker(options) {
        sql """
            CREATE TABLE test_load (
                `k` int(11) NOT NULL,
                `v` int(11) NOT NULL
            ) ENGINE=OLAP
            unique KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES (
                "disable_auto_compaction"="true",
                "replication_num" = "1"
            ); 
        """

        sql """ insert into test_load values(1, 1), (2, 2); """
        sql """ insert into test_load values(3, 3), (4, 4); """
        sql """ insert into test_load values(1, 10), (3, 30); """
        order_qt_select_1 "SELECT * FROM test_load;"
        // change be config: delete_bitmap_max_bytes_store_in_fdb=0
        update_all_be_config("delete_bitmap_max_bytes_store_in_fdb", "0")
        sql """ insert into test_load values(2, 20), (4, 40); """
        order_qt_select_2 "SELECT * FROM test_load;"

        // change be config: delete_bitmap_store_version=3
    }

}
