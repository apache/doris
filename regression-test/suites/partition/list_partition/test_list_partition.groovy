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

suite("test_list_partition", "partition") {
    // todo: test list partitions, such as: create, alter table partition ...
    sql "drop table if exists list_par"
    sql """
        CREATE TABLE list_par ( 
            k1 tinyint NOT NULL, 
            k2 smallint NOT NULL, 
            k3 int NOT NULL, 
            k4 bigint NOT NULL, 
            k5 decimal(9, 3) NOT NULL, 
            k6 char(5) NOT NULL, 
            k10 date NOT NULL, 
            k11 datetime NOT NULL, 
            k7 varchar(20) NOT NULL, 
            k8 double max NOT NULL, 
            k9 float sum NOT NULL ) 
        AGGREGATE KEY(k1,k2,k3,k4,k5,k6,k10,k11,k7) 
        PARTITION BY LIST(k1) ( 
            PARTITION p1 VALUES IN ("1","2","3","4"), 
            PARTITION p2 VALUES IN ("5","6","7","8","9","10","11","12","13","14"), 
            PARTITION p3 VALUES IN ("15") ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")
        """
    List<List<Object>> result1  = sql "show tables like 'list_par'"
    logger.info("${result1}")
    assertEquals(result1.size(), 1)
    List<List<Object>> result2  = sql "show partitions from list_par"
    logger.info("${result2}")
    assertEquals(result2.size(), 3)
    sql "drop table list_par"
}
