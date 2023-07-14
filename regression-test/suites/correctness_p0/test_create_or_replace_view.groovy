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

 suite("test_create_or_replace_view") {
    String testViewName = "test_view"

    sql """ drop view if exists ${testViewName} """

    sql """ CREATE VIEW ${testViewName}
                (
                    k1 COMMENT "number"
                )
            COMMENT "my first view"
            AS
            SELECT number as k1 FROM numbers("number" = "10");
        """

    order_qt_view1 """ select * from ${testViewName} """

    sql """ CREATE OR REPLACE VIEW ${testViewName}
                (
                    k1 COMMENT "number"
                )
            COMMENT "my first view"
            AS
            SELECT number as k1 FROM numbers("number" = "20");
        """

    order_qt_view2 """ select * from ${testViewName} """

    sql """ drop table if exists vwtest1 """

    sql """
        CREATE TABLE IF NOT EXISTS vwtest1 (
            `cc` varchar(200) NULL COMMENT "",
            `dd` int NULL COMMENT ""
        ) ENGINE=OLAP
        UNIQUE KEY (`cc`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`cc`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
        )
    """

    sql """
        INSERT INTO  vwtest1 VALUES('a', 1), ('b', 1), ('c', 1);
    """	

    sql """ drop table if exists vwtest2 """

    sql """
        CREATE TABLE IF NOT EXISTS vwtest2 (
            `cc` varchar(200) NULL COMMENT "",
            `dd` int NULL COMMENT ""
        ) ENGINE=OLAP
        UNIQUE KEY (`cc`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`cc`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
        )
    """	 

    sql """
        INSERT INTO  vwtest2 VALUES('a', 1), ('b', 1), ('d', 1);
    """

    sql """ CREATE OR REPLACE VIEW ${testViewName}
                (
                    cc COMMENT "", dd1 COMMENT "", dd2 COMMENT ""
                )
            COMMENT "my first view"
            AS
            SELECT t1.cc as cc, t1.dd as dd1, t2.dd as dd2 FROM vwtest1 t1, vwtest2 t2 where t1.cc = t2.cc;
        """

    order_qt_view3 """ select * from ${testViewName} """

    order_qt_showcreate  """ show create view ${testViewName} """

}
