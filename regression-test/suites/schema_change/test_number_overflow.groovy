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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite ("test_number_overflow") {
    def getJobState = { tableName ->
        def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
        return jobStateResult[0][9]
    }

    try {

        sql """ DROP TABLE IF EXISTS test_number_overflow """
        sql """
                CREATE TABLE IF NOT EXISTS test_number_overflow ( k1 INT NOT NULL, k2 VARCHAR(4096) NOT NULL, k3 VARCHAR(4096) NOT NULL, k4 VARCHAR(4096) NOT NULL, k5 VARCHAR(4096) NOT NULL, k6 VARCHAR(4096) NOT NULL, k7 VARCHAR(4096) NOT NULL, k8 VARCHAR(4096) NOT NULL, k9 VARCHAR(4096) NOT NULL, v1 FLOAT SUM NOT NULL, v2 DECIMAL(20,7) SUM NOT NULL ) AGGREGATE KEY(k1,k2,k3,k4,k5,k6,k7,k8,k9) PARTITION BY RANGE(k1) ( PARTITION partition_a VALUES LESS THAN ("5"), PARTITION partition_b VALUES LESS THAN ("30"), PARTITION partition_c VALUES LESS THAN ("100"), PARTITION partition_d VALUES LESS THAN ("500"), PARTITION partition_e VALUES LESS THAN ("1000"), PARTITION partition_f VALUES LESS THAN ("2000"), PARTITION partition_g VALUES LESS THAN MAXVALUE ) DISTRIBUTED BY HASH(k1, k2) BUCKETS 3
                PROPERTIES ( "replication_num" = "1");
            """

        sql """ insert into test_number_overflow values(1, "1", "1", "1", "2147483648", "1", "1", "1", "1", 1, 1)
            """
    
        sql """
            ALTER TABLE test_number_overflow MODIFY COLUMN k5 INT
            """

        int max_try_time = 100
        while(max_try_time--){
            String result = getJobState('test_number_overflow')
            if (result == "FINISHED") {
                assertEquals(1,2)
            } else if (result == "CANCELLED") {
                break;
            } else {
                sleep(2000)
                if (max_try_time < 1){
                        assertEquals(3,4)
                }
            }
        }

    } finally {
        try_sql("DROP TABLE IF EXISTS test_number_overflow")
    }
}
