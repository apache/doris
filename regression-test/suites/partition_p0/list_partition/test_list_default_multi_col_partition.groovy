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

suite("test_list_default_multi_col_partition") {
    sql "drop table if exists list_default_multi_col_par"
    sql """
        CREATE TABLE IF NOT EXISTS list_default_multi_col_par ( 
            k1 tinyint NOT NULL, 
            k2 smallint NOT NULL, 
            k3 int NOT NULL, 
            k4 bigint NOT NULL, 
            k5 decimal(9, 3) NOT NULL,
            k8 double max NOT NULL, 
            k9 float sum NOT NULL ) 
        AGGREGATE KEY(k1,k2,k3,k4,k5)
        PARTITION BY LIST(k1,k2) ( 
            PARTITION p1 VALUES IN (("1","2"),("3","4")), 
            PARTITION p2 VALUES IN (("5","6"),("7","8")), 
            PARTITION p3 ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")
        """

    sql """insert into list_default_multi_col_par values (1,2,1,1,24453.325,1,1)"""
    // the following two rows should be inserted into default partition successfully
    sql """insert into list_default_multi_col_par values (10,1,1,1,24453.325,1,1)"""
    sql """insert into list_default_multi_col_par values (11,1,1,1,24453.325,1,1)"""
    qt_sql """select * from list_default_multi_col_par order by k1"""

    List<List<Object>> result1  = sql "show partitions from list_default_multi_col_par"
    logger.info("${result1}")
    assertEquals(result1.size(), 3)

    // alter table create one more default partition
    try {
        test {
        sql """alter table list_default_multi_col_par add partition p5"""
        exception "errCode = 2, detailMessage = Invalid list value format: errCode = 2, detailMessage = The partition key"
    }
    } finally {
    }

    sql """alter table list_default_multi_col_par drop partition p3"""

    sql "drop table list_default_multi_col_par"


    // create one table without default partition
    sql """
        CREATE TABLE IF NOT EXISTS list_default_multi_col_par ( 
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
            PARTITION p2 VALUES IN ("5","6","7","8") 
        ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")
        """
    // insert value which is not allowed in existing partitions
    def exception_str = isGroupCommitMode() ? "too many filtered rows" : "Insert has filtered data in strict mode"
    try {
        test {
        sql """insert into list_default_multi_col_par values (10,1,1,1,24453.325,1,1)"""
        exception exception_str
    }
    } finally{
    }

    // alter table add default partition
    sql """alter table list_default_multi_col_par add partition p3"""

    // insert the formerly disallowed value
    sql """insert into list_default_multi_col_par values (10,1,1,1,24453.325,1,1)"""

    qt_sql """select * from list_default_multi_col_par order by k1"""
    qt_sql """select * from list_default_multi_col_par partition p1 order by k1"""
    qt_sql """select * from list_default_multi_col_par partition p3 order by k1"""

    // drop the default partition
    sql """alter table list_default_multi_col_par drop partition p3"""
    qt_sql """select * from list_default_multi_col_par order by k1"""

    // insert value which is not allowed in existing partitions
    try {
        test {
        sql """insert into list_default_multi_col_par values (10,1,1,1,24453.325,1,1)"""
        exception exception_str
    }
    } finally{
    }
    qt_sql """select * from list_default_multi_col_par order by k1"""
}
