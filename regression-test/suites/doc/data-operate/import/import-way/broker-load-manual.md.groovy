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

import org.junit.jupiter.api.Assertions

suite("docs/data-operate/import/import-way/broker-load-manual.md", "p0,nonConcurrent") {
    try {
        sql "CREATE DATABASE IF NOT EXISTS demo;"
        sql "USE demo;"
        sql "show load order by createtime desc limit 1"
        try {
            sql """CANCEL LOAD FROM demo WHERE LABEL = "broker_load_2022_03_23";"""
        } catch (Exception e) {
            if (!e.getMessage().contains("Load job does not exist")) {
                logger.error("this error should not throw")
                throw e
            }
        }
        sql "CLEAN LABEL FROM demo;"

        multi_sql """
            DROP TABLE IF EXISTS load_hdfs_file_test;
            CREATE TABLE IF NOT EXISTS load_hdfs_file_test (
                id INT,
                age INT,
                name STRING
            ) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
        """

        if (enableHdfs()) {
            logger.info("test hdfs load")
            var base_path = uploadToHdfs("doc/data-operate/import/import-way/broker_load")
            sql """
                LOAD LABEL demo.label_20220402
                (
                    DATA INFILE("${base_path}/test_hdfs.txt")
                    INTO TABLE `load_hdfs_file_test`
                    COLUMNS TERMINATED BY "\\t"
                    (id,age,name)
                )
                with HDFS
                (
                  "fs.defaultFS"="${getHdfsFs()}",
                  "hadoop.username" = "${getHdfsUser()}",
                  "hadoop.password" = "${getHdfsPasswd()}"
                )
                PROPERTIES
                (
                    "timeout"="1200",
                    "max_filter_ratio"="0.1"
                );
            """
            waitForBrokerLoadDone("label_20220402")
            order_qt_sql "SELECT * FROM load_hdfs_file_test"
            sql """
                LOAD LABEL demo.label_20220402_2
                (
                    DATA INFILE("${base_path}/test_hdfs.txt")
                    INTO TABLE `load_hdfs_file_test`
                    COLUMNS TERMINATED BY "\\t"            
                    (id,age,name)
                ) 
                with HDFS
                (
                    "hadoop.username" = "${getHdfsUser()}",
                    "fs.defaultFS"="${getHdfsFs()}",
                    "dfs.nameservices" = "hafs",
                    "dfs.ha.namenodes.hafs" = "my_namenode1, my_namenode2",
                    "dfs.namenode.rpc-address.hafs.my_namenode1" = "${getHdfsFs()}",
                    "dfs.namenode.rpc-address.hafs.my_namenode2" = "${getHdfsFs()}",
                    "dfs.client.failover.proxy.provider.hafs" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
                )
                PROPERTIES
                (
                    "timeout"="1200",
                    "max_filter_ratio"="0.1"
                );
            """
            waitForBrokerLoadDone("label_20220402_2")
            order_qt_sql "SELECT * FROM load_hdfs_file_test"

            multi_sql """
                CREATE DATABASE IF NOT EXISTS example_db;
                USE example_db;
                CLEAN LABEL FROM example_db;
                DROP TABLE IF EXISTS my_table1;
                CREATE TABLE IF NOT EXISTS my_table1 (
                    k1 INT,
                    k2 INT,
                    k3 INT
                ) PARTITION BY RANGE(k1) (
                    PARTITION p1 VALUES LESS THAN (10),
                    PARTITION p2 VALUES LESS THAN (20),
                    PARTITION p3 VALUES LESS THAN (30)
                ) DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1"
                );
                DROP TABLE IF EXISTS my_table2;
                CREATE TABLE IF NOT EXISTS my_table2 LIKE my_table1;
            """

            sql """
                LOAD LABEL example_db.label2
                (
                    DATA INFILE("${base_path}/input/file-10*")
                    INTO TABLE `my_table1`
                    PARTITION (p1)
                    COLUMNS TERMINATED BY ","
                    (k1, tmp_k2, tmp_k3)
                    SET (
                        k2 = tmp_k2 + 1,
                        k3 = tmp_k3 + 1
                    ),
                    DATA INFILE("${base_path}/input/file-20*")
                    INTO TABLE `my_table2`
                    COLUMNS TERMINATED BY ","
                    (k1, k2, k3)
                )
                with HDFS
                (
                  "fs.defaultFS"="${getHdfsFs()}",
                  "hadoop.username" = "${getHdfsUser()}",
                  "hadoop.password" = "${getHdfsPasswd()}"
                );
            """
            waitForBrokerLoadDone("label2")
            order_qt_sql "SELECT * FROM my_table1"

            multi_sql """
                DROP TABLE IF EXISTS my_table;
                CREATE TABLE IF NOT EXISTS my_table (
                    k1 INT,
                    k2 INT,
                    k3 INT
                ) PARTITION BY RANGE(k1) (
                    PARTITION p1 VALUES LESS THAN (10),
                    PARTITION p2 VALUES LESS THAN (20),
                    PARTITION p3 VALUES LESS THAN (30)
                ) DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1"
                );
            """
            sql """
                LOAD LABEL example_db.label3
                (
                    DATA INFILE("${base_path}/data/*/*")
                    INTO TABLE `my_table`
                    COLUMNS TERMINATED BY "\\\\x01"
                )
                with HDFS
                (
                  "fs.defaultFS"="${getHdfsFs()}",
                  "hadoop.username" = "${getHdfsUser()}",
                  "hadoop.password" = "${getHdfsPasswd()}"
                );
            """
            waitForBrokerLoadDone("label3")
            order_qt_sql "SELECT * FROM my_table"

            multi_sql """
                DROP TABLE IF EXISTS my_table;
                CREATE TABLE IF NOT EXISTS my_table (
                    k1 INT,
                    k2 INT,
                    k3 INT,
                    city STRING,
                    utc_date date
                ) PARTITION BY RANGE(k1) (
                    PARTITION p1 VALUES LESS THAN (10),
                    PARTITION p2 VALUES LESS THAN (20),
                    PARTITION p3 VALUES LESS THAN (30)
                ) DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1"
                );
            """
            sql """
                LOAD LABEL example_db.label4
                (
                    DATA INFILE("${base_path}/input/file.parquet")
                    INTO TABLE `my_table`
                    FORMAT AS "parquet"
                    (k1, k2, k3)
                )
                with HDFS
                (
                  "fs.defaultFS"="${getHdfsFs()}",
                  "hadoop.username" = "${getHdfsUser()}",
                  "hadoop.password" = "${getHdfsPasswd()}"
                );
            """
            waitForBrokerLoadDone("label4")
            order_qt_sql "SELECT * FROM my_table"

            sql """
                LOAD LABEL example_db.label5
                (
                    DATA INFILE("${base_path}/input/city=beijing/*/*")
                    INTO TABLE `my_table`
                    FORMAT AS "csv"
                    (k1, k2, k3)
                    COLUMNS FROM PATH AS (city, utc_date)
                )
                with HDFS
                (
                  "fs.defaultFS"="${getHdfsFs()}",
                  "hadoop.username" = "${getHdfsUser()}",
                  "hadoop.password" = "${getHdfsPasswd()}"
                );
            """
            waitForBrokerLoadDone("label5")
            order_qt_sql "SELECT * FROM my_table"

            sql """
                LOAD LABEL example_db.label6
                (
                    DATA INFILE("${base_path}/input/file.csv")
                    INTO TABLE `my_table`
                    (k1, k2, k3)
                    SET (
                        k2 = k2 + 1
                    )
                    PRECEDING FILTER k1 = 1
                    WHERE k1 > k2
                )
                with HDFS
                (
                  "fs.defaultFS"="${getHdfsFs()}",
                  "hadoop.username" = "${getHdfsUser()}",
                  "hadoop.password" = "${getHdfsPasswd()}"
                );
            """
            waitForBrokerLoadDone("label6")
            order_qt_sql "SELECT * FROM my_table"

            multi_sql """
                DROP TABLE IF EXISTS tbl12;
                CREATE TABLE IF NOT EXISTS tbl12 (
                    data_time DATETIME,
                    k2        INT,
                    k3        INT
                ) DISTRIBUTED BY HASH(data_time) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1"
                );
            """
            sql """
                LOAD LABEL example_db.label7
                (
                    DATA INFILE("${base_path}/data2/*/test.txt") 
                    INTO TABLE `tbl12`
                    COLUMNS TERMINATED BY ","
                    (k2,k3)
                    COLUMNS FROM PATH AS (data_time)
                    SET (
                        data_time=str_to_date(data_time, '%Y-%m-%d %H%%3A%i%%3A%s')
                    )
                )
                with HDFS
                (
                  "fs.defaultFS"="${getHdfsFs()}",
                  "hadoop.username" = "${getHdfsUser()}",
                  "hadoop.password" = "${getHdfsPasswd()}"
                );
            """
            waitForBrokerLoadDone("label7")
            order_qt_sql "SELECT * FROM tbl12"

            multi_sql """
                DROP TABLE IF EXISTS my_table;
                CREATE TABLE IF NOT EXISTS my_table (
                    k1 INT,
                    k2 INT,
                    k3 INT,
                    v1 INT,
                    v2 INT
                ) UNIQUE KEY (k1, k2, k3)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1"
                );
                INSERT INTO my_table VALUES
                    (0, 0, 0, 0, 0),
                    (1, 1, 1, 100, 100),
                    (2, 2, 2, 200, 200),
                    (3, 3, 3, 300, 300);
            """
            sql """
                LOAD LABEL example_db.label8
                (
                    MERGE DATA INFILE("${base_path}/data3/data.csv")
                    INTO TABLE `my_table`
                    (k1, k2, k3, v2, v1)
                    DELETE ON v2 > 100
                )
                with HDFS
                (
                  "fs.defaultFS"="${getHdfsFs()}",
                  "hadoop.username" = "${getHdfsUser()}",
                  "hadoop.password" = "${getHdfsPasswd()}"
                )
                PROPERTIES
                (
                    "timeout" = "3600",
                    "max_filter_ratio" = "0.1"
                );
            """
            waitForBrokerLoadDone("label8")
            order_qt_sql "SELECT * FROM my_table"
            multi_sql """
                DROP TABLE IF EXISTS my_table;
                CREATE TABLE IF NOT EXISTS my_table (
                    k1 INT,
                    k2 INT,
                    source_sequence INT,
                    v1 INT,
                    v2 INT
                ) UNIQUE KEY (k1, k2)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "function_column.sequence_col" = "source_sequence",
                    "replication_num" = "1"
                );
                INSERT INTO my_table VALUES
                    (0, 0, 0, 0, 0),
                    (1, 1, 1, 100, 100),
                    (2, 2, 2, 200, 200),
                    (3, 3, 3, 300, 300);
            """
            sql """
                LOAD LABEL example_db.label9
                (
                    DATA INFILE("${base_path}/test_sequence.csv")
                    INTO TABLE `my_table`
                    COLUMNS TERMINATED BY ","
                    (k1,k2,source_sequence,v1,v2)
                    ORDER BY source_sequence
                ) 
                with HDFS
                (
                  "fs.defaultFS"="${getHdfsFs()}",
                  "hadoop.username" = "${getHdfsUser()}",
                  "hadoop.password" = "${getHdfsPasswd()}"
                );
            """
            waitForBrokerLoadDone("label9")
            order_qt_sql "SELECT * FROM my_table"

            multi_sql """
                DROP TABLE IF EXISTS my_table;
                CREATE TABLE IF NOT EXISTS my_table (
                    id INT,
                    city STRING,
                    code INT
                )
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1"
                );
            """
            sql """
                LOAD LABEL example_db.label10
                (
                    DATA INFILE("${base_path}/file.json")
                    INTO TABLE `my_table`
                    FORMAT AS "json"
                    PROPERTIES(
                      "json_root" = "\$.item",
                      "jsonpaths" = "[\\"\$.id\\", \\"\$.city\\", \\"\$.code\\"]"
                    )       
                )
                with HDFS
                (
                  "fs.defaultFS"="${getHdfsFs()}",
                  "hadoop.username" = "${getHdfsUser()}",
                  "hadoop.password" = "${getHdfsPasswd()}"
                );
            """
            waitForBrokerLoadDone("label10")
            order_qt_sql "SELECT * FROM my_table"
            sql """
                LOAD LABEL example_db.label10_2
                (
                  DATA INFILE("${base_path}/file.json")
                  INTO TABLE `my_table`
                  FORMAT AS "json"
                  (id, code, city)
                  SET (id = id * 10)
                  PROPERTIES(
                    "json_root" = "\$.item",
                    "jsonpaths" = "[\\"\$.id\\", \\"\$.city\\", \\"\$.code\\"]"
                  )       
                )
                with HDFS
                (
                    "fs.defaultFS"="${getHdfsFs()}",
                    "hadoop.username" = "${getHdfsUser()}",
                     "hadoop.password" = "${getHdfsPasswd()}"
                );
            """
            waitForBrokerLoadDone("label10_2")
            order_qt_sql "SELECT * FROM my_table"
        }


        multi_sql """
            DROP TABLE IF EXISTS load_test;
            CREATE TABLE IF NOT EXISTS load_test (
                k1 INT,
                k2 INT,
                k3 INT
            ) DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        sql """
            LOAD LABEL example_db.example_label_1
            (
                DATA INFILE("s3://${getS3BucketName()}/regression/doc/file.csv")
                INTO TABLE load_test
                COLUMNS TERMINATED BY ","
            )
            WITH S3
            (
                "AWS_ENDPOINT" = "${getS3Endpoint()}",
                "AWS_ACCESS_KEY" = "${getS3AK()}",
                "AWS_SECRET_KEY"="${getS3SK()}",
                "AWS_REGION" = "${getS3Region()}"
            )
            PROPERTIES
            (
                "timeout" = "3600"
            );
        """
        waitForBrokerLoadDone("example_label_1")
        qt_sql "SELECT COUNT() FROM load_test"
    } catch (Throwable t) {
        Assertions.fail("examples in docs/data-operate/import/import-way/broker-load-manual.md failed to exec, please fix it", t)
    }
}
