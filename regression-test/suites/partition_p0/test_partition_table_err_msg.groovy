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

suite("test_partition_table_err_msg", "p0") {
    // create partition table errors
    test {
        sql """
            CREATE TABLE err_tb1 ( 
              k1 TINYINT NOT NULL, 
              k2 SMALLINT NOT NULL, 
              k3 INT NOT NULL, 
              k4 BIGINT NOT NULL, 
              k5 DATETIME NOT NULL, 
              v1 DATE REPLACE NOT NULL) 
              AGGREGATE KEY(k1,k2,k3,k4,k5) 
            PARTITION BY RANGE(k5) ( 
              PARTITION partition_a VALUES LESS THAN ("2017-01-01 00:00:00"), 
              PARTITION partition_b VALUES LESS THAN ("2017-01-01 00:00:00"), 
              PARTITION partition_c VALUES LESS THAN ("2039-01-01 00:00:00"), 
              PARTITION partition_d VALUES LESS THAN MAXVALUE ) 
            DISTRIBUTED BY HASH(k1) BUCKETS 13"""
        exception "is intersected with range"
    }
    test {
        sql """
            CREATE TABLE err_tb2 ( 
              k1 TINYINT NOT NULL, 
              k2 SMALLINT NOT NULL, 
              k3 INT NOT NULL, 
              k4 BIGINT NOT NULL, 
              k5 DATETIME NOT NULL, 
              v1 DATE REPLACE NOT NULL ) 
            AGGREGATE KEY(k1,k2,k3,k4,k5)
            PARTITION BY RANGE(k5) ( 
              PARTITION partition_a VALUES LESS THAN ("0000-01-01 00:00:00"), 
              PARTITION partition_b VALUES LESS THAN ("2027-01-01 23:59:59"), 
              PARTITION partition_c VALUES LESS THAN ("2039-01-01 00:00:00"), 
              PARTITION partition_d VALUES LESS THAN MAXVALUE ) 
            DISTRIBUTED BY HASH(k1) BUCKETS 13;
        """
        exception "Partition's upper value should not be MIN VALUE: ('0000-01-01 00:00:00')"
    }
    // datetime overflow
    test {
        sql """
            CREATE TABLE err_tb3 ( 
              k1 TINYINT NOT NULL, 
              k2 SMALLINT NOT NULL, 
              k3 INT NOT NULL, 
              k4 BIGINT NOT NULL, 
              k5 DATETIME NOT NULL, 
              v1 DATE REPLACE NOT NULL)
            AGGREGATE KEY(k1,k2,k3,k4,k5) 
            PARTITION BY RANGE(k5) ( 
              PARTITION partition_a VALUES LESS THAN ("1899-12-31 23:59:59"), 
              PARTITION partition_b VALUES LESS THAN ("2027-01-01 00:00:00"), 
              PARTITION partition_c VALUES LESS THAN ("2039-01-01 00:00:00"), 
              PARTITION partition_d VALUES LESS THAN ("10000-01-01 00:00:00") ) 
            DISTRIBUTED BY HASH(k1) BUCKETS 13
        """
        exception "date/datetime literal [10000-01-01 00:00:00] is invalid"
    }
    test {
        sql """
            CREATE TABLE err_tb4 ( 
              k1 TINYINT NOT NULL, 
              k2 SMALLINT NOT NULL, 
              k3 INT NOT NULL, 
              k4 BIGINT NOT NULL, 
              k5 DATETIME NOT NULL, 
              v1 DATE REPLACE NOT NULL)
            AGGREGATE KEY(k1,k2,k3,k4,k5) 
            PARTITION BY RANGE(k5) ( 
              PARTITION partition_a VALUES LESS THAN ("1899-12-31 23:59:59"), 
              PARTITION partition_b VALUES LESS THAN ("2027-01-01 00:00:00"), 
              PARTITION partition_c VALUES LESS THAN ("2039-01-01 00:00:00"), 
              PARTITION partition_d VALUES LESS THAN ("9999-12-31 24:00:00") ) 
            DISTRIBUTED BY HASH(k1) BUCKETS 13
        """
        exception "date/datetime literal [9999-12-31 24:00:00] is invalid"
    }
    test {
        sql """
            CREATE TABLE err_tb5 ( 
              k1 TINYINT NOT NULL, 
              k2 SMALLINT NOT NULL, 
              k3 INT NOT NULL, 
              k4 BIGINT NOT NULL, 
              k5 DATETIME NOT NULL, 
              v1 DATE REPLACE NOT NULL ) 
            AGGREGATE KEY(k1,k2,k3,k4,k5) 
            PARTITION BY RANGE(k4) ( 
              PARTITION partition_a VALUES LESS THAN ("2000"), 
              PARTITION partition_b VALUES LESS THAN ("1000"), 
              PARTITION partition_c VALUES LESS THAN ("3000"), 
              PARTITION partition_d VALUES LESS THAN MAXVALUE ) 
            DISTRIBUTED BY HASH(k1) BUCKETS 13;
        """
        exception " is intersected with range"
    }
    test {
        sql """
            CREATE TABLE err_tb6 ( 
              k1 TINYINT NOT NULL, 
              k2 SMALLINT NOT NULL, 
              k3 INT NOT NULL, 
              k4 BIGINT NOT NULL, 
              k5 DATETIME NOT NULL, 
              v1 DATE REPLACE NOT NULL ) 
            AGGREGATE KEY(k1,k2,k3,k4,k5) 
            PARTITION BY RANGE(k1) ( 
              PARTITION partition_a VALUES LESS THAN ("20"), 
              PARTITION partition_b VALUES LESS THAN ("100"), 
              PARTITION partition_c VALUES LESS THAN ("101"), 
              PARTITION partition_d VALUES LESS THAN ("3000") ) 
            DISTRIBUTED BY HASH(k1) BUCKETS 13
        """
        exception "Invalid range value format： errCode = 2, detailMessage = Number out of range[3000]. type: tinyint"
    }
    test {
        sql """
            CREATE TABLE err_tb7 ( 
              k1 TINYINT NOT NULL, 
              k2 SMALLINT NOT NULL, 
              k3 INT NOT NULL, 
              k4 BIGINT NOT NULL, 
              k5 DATETIME NOT NULL, 
              v1 DATE REPLACE NOT NULL) 
            AGGREGATE KEY(k1,k2,k3,k4,k5) 
            PARTITION BY RANGE(k1) ( 
              PARTITION partition_a VALUES LESS THAN ("-50.1"), 
              PARTITION partition_b VALUES LESS THAN ("20.5"), 
              PARTITION partition_c VALUES LESS THAN ("100.0"), 
              PARTITION partition_d VALUES LESS THAN ("101") ) 
            DISTRIBUTED BY HASH(k1) BUCKETS 13
        """
        exception "Invalid range value format： errCode = 2, detailMessage = Invalid number format: -50.1"
    }
    // aggregate table value column can't be partition key
    test {
        sql """
            CREATE TABLE err_tb8 ( 
              k1 TINYINT NOT NULL, 
              k2 SMALLINT NOT NULL, 
              k3 INT NOT NULL, 
              k4 BIGINT NOT NULL, 
              k5 DATETIME NOT NULL, 
              v1 INT REPLACE NOT NULL ) 
            AGGREGATE KEY(k1,k2,k3,k4,k5) 
            PARTITION BY RANGE(v1) ( 
              PARTITION partition_a VALUES LESS THAN ("5"), 
              PARTITION partition_b VALUES LESS THAN ("20"), 
              PARTITION partition_c VALUES LESS THAN ("30"), 
              PARTITION partition_d VALUES LESS THAN MAXVALUE ) 
            DISTRIBUTED BY HASH(k1) BUCKETS 5
        """
        exception "The partition column could not be aggregated column"
    }
}
