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

suite("test_broker_load_with_merge", "load_p0") {
    // define a sql table
    def testTable = "tbl_test_broker_load_with_merge"

    def create_test_table = {testTablex ->
        def result1 = sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
                `k1` BIGINT NOT NULL,
                `k2` DATE NOT NULL,
                `k3` INT(11) NOT NULL,
                `k4` INT(11) NOT NULL,
                `v5` BIGINT SUM NULL DEFAULT "0"
            ) ENGINE=OLAP
            UNIQUE KEY(`k1`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k1`) BUCKETS 16
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "storage_format" = "V2"
            );
            """

        // DDL/DML return 1 row and 3 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

        // insert 1 row to check whether the table is ok
        def result2 = sql """ INSERT INTO ${testTable} VALUES
                        (1,'2023-09-01',1,1,1)
                        """
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 1, "Insert should update 1 rows")
    }

    def create_agg_test_table = {testTablex ->
        def result1 = sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
                `k1` BIGINT NOT NULL,
                `k2` DATE NULL,
                `k3` INT(11) NOT NULL,
                `k4` INT(11) NOT NULL,
                `v5` BIGINT SUM NULL DEFAULT "0"
            ) ENGINE=OLAP
            AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k1`) BUCKETS 16
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "storage_format" = "V2"
            );
            """

        // DDL/DML return 1 row and 3 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

        // insert 1 row to check whether the table is ok
        def result2 = sql """ INSERT INTO ${testTable} VALUES
                        (1,'2023-09-01',1,1,1)
                        """
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 1, "Insert should update 1 rows")
    }

    def load_from_hdfs_check_merge_type_1 = {testTablex, label, hdfsFilePath, format, brokerName, hdfsUser, hdfsPasswd ->
        test {
            sql """
                        LOAD LABEL ${label} (
                            DATA INFILE("${hdfsFilePath}")
                            INTO TABLE ${testTablex}
                            COLUMNS TERMINATED BY ","
                            FORMAT as "${format}"
                            DELETE ON "k1=1"
                        )
                        with BROKER "${brokerName}" (
                        "username"="${hdfsUser}",
                        "password"="${hdfsPasswd}")
                        PROPERTIES  (
                        "timeout"="1200",
                        "max_filter_ratio"="0.1");
                        """
            exception "not support DELETE ON clause when merge type is not MERGE."
        }
    }

    def load_from_hdfs_check_merge_type_2 = {testTablex, label, hdfsFilePath, format, brokerName, hdfsUser, hdfsPasswd ->

        test {
            sql """
                        LOAD LABEL ${label} (
                            MERGE
                            DATA INFILE("${hdfsFilePath}")
                            INTO TABLE ${testTablex}
                            COLUMNS TERMINATED BY ","
                            FORMAT as "${format}"
                        )
                        with BROKER "${brokerName}" (
                        "username"="${hdfsUser}",
                        "password"="${hdfsPasswd}")
                        PROPERTIES  (
                        "timeout"="1200",
                        "max_filter_ratio"="0.1");
                        """
            exception "Excepted DELETE ON clause when merge type is MERGE."
        }
    }

    def load_from_hdfs_check_merge_type_3 = {testTablex, label, hdfsFilePath, format, brokerName, hdfsUser, hdfsPasswd ->
        test {
            sql """
                        LOAD LABEL ${label} (
                            DELETE
                            DATA INFILE("${hdfsFilePath}")
                            NEGATIVE
                            INTO TABLE ${testTablex}
                            COLUMNS TERMINATED BY ","
                            FORMAT as "${format}"
                        )
                        with BROKER "${brokerName}" (
                        "username"="${hdfsUser}",
                        "password"="${hdfsPasswd}")
                        PROPERTIES  (
                        "timeout"="1200",
                        "max_filter_ratio"="0.1");
                        """
            exception "not support MERGE or DELETE with NEGATIVE."
        }

    }

    def load_from_hdfs_check_merge_type_4 = {testTablex, label, hdfsFilePath, format, brokerName, hdfsUser, hdfsPasswd ->
        test {
            sql """
                        LOAD LABEL ${label} (
                            DELETE
                            DATA INFILE("${hdfsFilePath}")
                            INTO TABLE ${testTablex}
                            COLUMNS TERMINATED BY ","
                            FORMAT as "${format}"
                        )
                        with BROKER "${brokerName}" (
                        "username"="${hdfsUser}",
                        "password"="${hdfsPasswd}")
                        PROPERTIES  (
                        "timeout"="1200",
                        "max_filter_ratio"="0.1");
                        """
            exception "load by MERGE or DELETE is only supported in unique tables."
        }
    }


    // if 'enableHdfs' in regression-conf.groovy has been set to true,
    // the test will run these case as below.
    if (enableHdfs()) {
        brokerName = getBrokerName()
        hdfsUser = getHdfsUser()
        hdfsPasswd = getHdfsPasswd()
        def hdfs_csv_file_path = uploadToHdfs "load_p0/broker_load/broker_load_with_merge.csv"

        // case1: has delete on condition and without merge
        try {
            sql "DROP TABLE IF EXISTS ${testTable}"
            create_test_table.call(testTable)

            def test_load_label = UUID.randomUUID().toString().replaceAll("-", "")

            load_from_hdfs_check_merge_type_1.call(testTable, test_load_label, hdfs_csv_file_path, "csv",
                    brokerName, hdfsUser, hdfsPasswd)

        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }

        // case2: has merge with delete on condition
        try {
            sql "DROP TABLE IF EXISTS ${testTable}"
            create_test_table.call(testTable)

            def test_load_label = UUID.randomUUID().toString().replaceAll("-", "")

            load_from_hdfs_check_merge_type_2.call(testTable, test_load_label, hdfs_csv_file_path, "csv",
                    brokerName, hdfsUser, hdfsPasswd)

        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }

        // case3: has merge with NEGATIVEs on condition
        try {
            sql "DROP TABLE IF EXISTS ${testTable}"
            create_test_table.call(testTable)
            def test_load_label = UUID.randomUUID().toString().replaceAll("-", "")
            load_from_hdfs_check_merge_type_3.call(testTable, test_load_label, hdfs_csv_file_path, "csv",
                    brokerName, hdfsUser, hdfsPasswd)
        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }


        // case4: agg table and merge type
        try {
            sql "DROP TABLE IF EXISTS ${testTable}"
            create_agg_test_table.call(testTable)

            def test_load_label = UUID.randomUUID().toString().replaceAll("-", "")

            load_from_hdfs_check_merge_type_4.call(testTable, test_load_label, hdfs_csv_file_path, "csv",
                    brokerName, hdfsUser, hdfsPasswd)

        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }

    }
}
