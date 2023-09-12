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

suite("test_routine_load","p0,external,external_docker,external_docker_routine_load") {
    // todo: test routine load, need kafka
    // sql "show routine load"
    // define a sql table
    def testTable = "tbl_test_routine_load_basic"

    def result1 = sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
        `k1` int(20) NULL COMMENT "",
        `k2` int(20) NULL COMMENT "",
        `k3` int(20) NULL COMMENT "",
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "storage_format" = "V2"
        )
    """

    String enabled = context.config.otherConfigs.get("enableKafkaTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String kafka_port = context.config.otherConfigs.get("kafka_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        def topic1 = "test"
        sql """
            CREATE ROUTINE LOAD ${topic1} ON ${testTable}
            COLUMNS TERMINATED BY ","
            PROPERTIES
            (
            "max_batch_interval" = "5",
            "max_batch_rows" = "200000",
            "max_batch_size" = "209715200"
            )
            FROM KAFKA
            (
            "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
            "kafka_topic" = "${topic1}",
            "property.kafka_default_offsets" = "OFFSET_BEGINNING"
            );
        """
    }
}
