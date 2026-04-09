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

suite("test_file_type_with_s3", "p0,external") {
    logger.info("start test_file_type_with_s3 test")
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disabled paimon test")
        return
    }
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String endpoint = "http://${externalEnvIp}:${minio_port}"
    qt_select_file_type_with_s3 """ select to_file("s3://warehouse/wh/test_partition_legacy.db/test_partition_legacy_true/dt=20514/bucket-0/data-e4bec578-6455-46f8-9b29-4ca3ebf3187e-0.parquet","us-east-1",
                                        "${endpoint}","admin","password"); """

    test {
        sql """select to_file("s3","us-east-1","${endpoint}","admin","password"); """;
        exception "INVALID_ARGUMENT"
    }

    test {
        sql """
                select to_file("s3://warehouse/wh/test_partition_legacy.db/test_partition_legacy_true/dt=20514/bucket-0/data-e4bec578-6455-46f8-9b29-4ca3ebf3187e-0.parquet","us-east-1",
                                        "${endpoint}","admin","sksksk");
            """
        exception "INVALID_ARGUMENT"
    }

    sql """ drop table if exists test_file; """
    sql """ 
        CREATE EXTERNAL TABLE test_file (
                `file` FILE NULL
            )
            ENGINE = fileset
            PROPERTIES (
                "s3.region" = "us-east-1",
                "s3.endpoint" = "${endpoint}",
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "location" = "s3://warehouse/wh/test_partition_legacy.db/test_partition_legacy_true/dt=20514/bucket-0/*"
        );
    """

    qt_select_file_type_with_s3_2 """ select * from test_file; """

    test {
        sql """
                insert into test_file values(
                to_file("s3://warehouse/wh/test_partition_legacy.db/test_partition_legacy_true/dt=20514/bucket-0/data-e4bec578-6455-46f8-9b29-4ca3ebf3187e-0.parquet","us-east-1",
                                                        "${endpoint}","admin","password")
                );
        """
        exception "not an OLAP table"
    }

    test {
        sql """
            select file from test_file order by file;
        """
        exception "and don't support filter, group by or order by"
    }

    test {
        sql """
            select file from test_file group by file;
        """
        exception "and don't support filter, group by or order by"
    }

    test {
        sql """
            select file from test_file where file = to_file("s3://warehouse/wh/test_partition_legacy.db/test_partition_legacy_true/dt=20514/bucket-0/data-e4bec578-6455-46f8-9b29-4ca3ebf3187e-0.parquet","us-east-1",
                                                        "${endpoint}","admin","password");
        """
        exception "FILE type does not support filter condition"
    }

    test {
        sql """
            create table test_file_2 (
                `file` FILE NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`file`)
            DISTRIBUTED BY HASH(`file`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        exception "only allowed in fileset engine"
    }

}
