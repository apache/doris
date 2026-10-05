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

suite("test_crossjoin_inlineview_slot") {
    sql "DROP TABLE IF EXISTS t0;" 
    sql "DROP TABLE IF EXISTS t1;"
    sql "DROP TABLE IF EXISTS t2;"
    sql "DROP TABLE IF EXISTS t3;"
    sql "DROP TABLE IF EXISTS t4;"
    
    sql """create table if not exists t0 (id0 int) engine=olap distributed by hash(id0) buckets 1 properties("replication_num"="1");"""
    sql """create table if not exists t1 (id1 int) engine=olap distributed by hash(id1) buckets 1 properties("replication_num"="1");"""
    sql """create table if not exists t2 (id2 tinyint(4), status tinyint(4)) engine=olap distributed by hash(id2) buckets 1 properties("replication_num"="1");"""
    sql """create table if not exists t3 (id3 int) engine=olap distributed by hash(id3) buckets 1 properties("replication_num"="1");"""
    sql """create table if not exists t4 (id4 int) engine=olap distributed by hash(id4) buckets 1 properties("replication_num"="1");"""
    
    sql "insert into t0 values (1);"
    sql "insert into t1 values (1);"
    sql "insert into t2 values (1, 1);"
    sql "insert into t3 values (1);"
    sql "insert into t4 values (1);"
    
    qt_sql """
            select id0 
            from t0
            where 
                EXISTS (   
                    select         
                        id4     
                    from t4 right join t1         
                        on id4 = id1 
                    where 
                        EXISTS (       
                            select             
                                status
                            from t2 join t3
                                on (id2 = id3 )         
                            )
                ) 
    """
}
