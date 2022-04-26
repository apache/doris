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

suite("test_lag_lead_window") {
    def tableName = "wftest"


    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} ( `aa` varchar(10) NULL COMMENT "", `bb` text NULL COMMENT "", `cc` text NULL COMMENT "" ) 
        ENGINE=OLAP UNIQUE KEY(`aa`) DISTRIBUTED BY HASH(`aa`) BUCKETS 3 
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "in_memory" = "false", "storage_format" = "V2" );
    """

    sql """ INSERT INTO ${tableName} VALUES 
        ('a','aa','/wyyt-image/2021/11/13/595345040188712460.jpg'),
        ('b','aa','/wyyt-image/2022/04/13/1434607674511761493.jpg'),
        ('c','cc','/wyyt-image/2022/04/13/1434607674511761493.jpg') """

    // not_vectorized
    sql """ set enable_vectorized_engine = false """

    qt_select_default """ select min(t.cc) over(PARTITION by t.cc  order by t.aa) ,
                            lag(t.cc,1,'') over (PARTITION by t.cc  order by t.aa) as l1 from ${tableName} t; """

    qt_select_default2 """ select min(t.cc) over(PARTITION by t.cc  order by t.aa) ,
                            lead(t.cc,1,'') over (PARTITION by t.cc  order by t.aa) as l1 from ${tableName} t; """

}