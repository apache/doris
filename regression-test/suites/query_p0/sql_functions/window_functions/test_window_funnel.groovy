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

suite("test_window_funnel") {
    // todo:test Window Funnel function causes be to crash
    def tableName = "dev_windowfunnel_test2"

    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """ CREATE TABLE $tableName ( 
                `xwho` varchar(50) NULL COMMENT 'xwho', 
                `xwhen` datetime COMMENT 'xwhen', 
                `xwhat` int NULL COMMENT 'xwhat' 
                ) 
                DUPLICATE KEY(xwho) 
                DISTRIBUTED BY HASH(xwho) BUCKETS 3 
                PROPERTIES ( 
                  "replication_num" = "1"
                ); 
         """

    sql """ INSERT into $tableName (xwho, xwhen, xwhat) values ('1', '2022-03-12 10:41:00', 1),
                                                               ('1', '2022-03-12 13:28:02', 2),
                                                               ('1', '2022-03-12 16:15:01', 3),
                                                               ('1', '2022-03-12 19:05:04', 4); 
        """

    sql """truncate table $tableName;"""

    qt_select_all """ select * from $tableName; """

    qt_select_by_window_funnel """ select window_funnel( 1, 'default', t.xwhen, t.xwhat = 1, t.xwhat = 2 ) AS level from $tableName t ;"""

}
