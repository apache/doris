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

suite("test_infer_predicate") {
    sql """ DROP TABLE IF EXISTS infer_predicate_t1 """
    sql """ DROP TABLE IF EXISTS infer_predicate_t2 """
    sql """ DROP VIEW IF EXISTS infer_predicate_v1 """
    sql """ DROP TABLE IF EXISTS infer_predicate_t4 """
    sql """ DROP TABLE IF EXISTS infer_predicate_t5 """
    sql"""
        CREATE TABLE infer_predicate_t2 (
            a varchar(1) NULL COMMENT "",
            x varchar(1) NULL COMMENT "",
            c varchar(1) NULL COMMENT ""
        ) ENGINE=OLAP
        UNIQUE KEY(a)
        DISTRIBUTED BY HASH(a) BUCKETS 8
        PROPERTIES (
            "replication_allocation" = "tag.location.default:1",
            "in_memory" = "false",
            "storage_format" = "V2"
        );
    """


    sql"""
        CREATE TABLE infer_predicate_t5 (
            x varchar(5) NULL COMMENT "",
            d varchar(3) NULL COMMENT ""
        ) ENGINE=OLAP
        UNIQUE KEY(x)
        DISTRIBUTED BY HASH(x) BUCKETS 8
        PROPERTIES (
            "replication_allocation" = "tag.location.default:1",
            "in_memory" = "false",
            "storage_format" = "V2"
        );
    """
        
    sql"""
        CREATE TABLE infer_predicate_t4 (
            a varchar(1) NULL COMMENT "",
            e varchar(1) NULL COMMENT ""
        ) ENGINE=OLAP
        UNIQUE KEY(a)
        DISTRIBUTED BY HASH(a) BUCKETS 8
        PROPERTIES (
            "replication_allocation" = "tag.location.default:1",
            "in_memory" = "false",
            "storage_format" = "V2"
        );
    """
        
        
    sql"""
        CREATE TABLE infer_predicate_t1 (
            k1 varchar(20) NULL COMMENT "",
            dt date NULL COMMENT ""
        ) ENGINE=OLAP
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 6
        PROPERTIES (
            "replication_allocation" = "tag.location.default:1",
            "in_memory" = "false",
            "storage_format" = "V2"
        );
    """
        
    sql"""
        CREATE VIEW infer_predicate_v1
        AS
        SELECT k1
        FROM infer_predicate_t1
        WHERE dt = (
           SELECT max(dt)
        FROM infer_predicate_t1
        ); 
    """

    try {
        sql """
            select
            f1.k1
            from (
                select
                infer_predicate_t2.c as k1
                from infer_predicate_t5
                inner join infer_predicate_t2 on infer_predicate_t2.x=infer_predicate_t5.x
                inner join infer_predicate_t4 on infer_predicate_t2.a=infer_predicate_t4.a and infer_predicate_t2.x in ('ZCR', 'ZDR')
            ) f1
            left join infer_predicate_v1 on f1.k1=infer_predicate_v1.k1; 
        """
    } finally {
        sql """ DROP TABLE IF EXISTS infer_predicate_t1 """
        sql """ DROP TABLE IF EXISTS infer_predicate_t2 """
        sql """ DROP VIEW IF EXISTS infer_predicate_v1 """
        sql """ DROP TABLE IF EXISTS infer_predicate_t4 """
        sql """ DROP TABLE IF EXISTS infer_predicate_t5 """
    }
}
