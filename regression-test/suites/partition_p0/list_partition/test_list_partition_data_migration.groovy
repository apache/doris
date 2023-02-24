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

suite("test_list_partition_data_migration") {
    sql "drop table if exists list_par_data_migration"
    sql """
        CREATE TABLE IF NOT EXISTS list_par_data_migration ( 
            k1 tinyint NOT NULL, 
            k2 smallint NOT NULL, 
            k3 int NOT NULL, 
            k4 bigint NOT NULL, 
            k5 decimal(9, 3) NOT NULL,
            k8 double max NOT NULL, 
            k9 float sum NOT NULL ) 
        AGGREGATE KEY(k1,k2,k3,k4,k5)
        PARTITION BY LIST(k1) ( 
            PARTITION p1 VALUES IN ("1","2","3","4"), 
            PARTITION p2 VALUES IN ("5","6","7","8"), 
            PARTITION p3 ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")
        """

    sql """insert into list_par_data_migration values (1,1,1,1,24453.325,1,1)"""
    sql """insert into list_par_data_migration values (10,1,1,1,24453.325,1,1)"""
    sql """insert into list_par_data_migration values (11,1,1,1,24453.325,1,1)"""
    qt_sql """select * from list_par_data_migration order by k1"""

    List<List<Object>> result1  = sql "show partitions from list_par_data_migration"
    logger.info("${result1}")
    assertEquals(result1.size(), 3)

    // alter table create one more default partition
    try {
        test {
        sql """alter table list_par_data_migration add partition p5"""
        exception "errCode = 2, detailMessage = Invalid list value format: errCode = 2, detailMessage = The partition key"
    }
    } finally {
    }

    sql """alter table list_par_data_migration add partition p4 values in ("10")"""
    sql """insert into list_par_data_migration values (10,1,1,1,24453.325,1,1)"""
    qt_sql """select * from list_par_data_migration order by k1"""
    qt_sql """select count(*) from list_par_data_migration partition p4"""

    sql """insert into list_par_data_migration select * from list_par_data_migration partition p3 where k1=10 order by k1"""

    // it seems the return orders of partitions might be random
    // and we have no way sort the result by the order of partition
    // qt_sql """select * from list_par_data_migration order by k1"""
    qt_sql """select * from list_par_data_migration partition p1 order by k1"""
    qt_sql """select * from list_par_data_migration partition p3 order by k1"""

    sql """delete from list_par_data_migration partition p3 where k1=10"""

    qt_sql """select * from list_par_data_migration partition p3 order by k1"""

    qt_sql """select * from list_par_data_migration order by k1"""
}
