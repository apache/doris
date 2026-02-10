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

suite("test_kafka_streaming_basic", "p0,nonConcurrent") {
    
    // Test configuration
    String enabled = context.config.otherConfigs.get("enableKafkaTest")
    String kafka_port = context.config.otherConfigs.get("kafka_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    def kafka_broker = "${externalEnvIp}:${kafka_port}"
    
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        
        def catalogName = "test_kafka_catalog"
        def tableName = "kafka_streaming_target"
        def topicName = "test_kafka_streaming_topic"
        def jobName = "test_kafka_streaming_job"
        def dbName = context.config.getDbNameByFile(context.file)
        
        try {
            // 1. Drop existing objects if any
            sql "DROP JOB IF EXISTS ${jobName}"
            sql "DROP TABLE IF EXISTS ${tableName}"
            sql "DROP CATALOG IF EXISTS ${catalogName}"
            
            // 2. Create Trino Kafka Catalog
            sql """
                CREATE CATALOG IF NOT EXISTS ${catalogName} PROPERTIES (
                    "type" = "trino-connector",
                    "trino.connector.name" = "kafka",
                    "trino.kafka.nodes" = "${kafka_broker}",
                    "trino.kafka.table-names" = "${topicName}",
                    "trino.kafka.hide-internal-columns" = "false"
                )
            """
            
            // 3. Create target table
            sql """
                CREATE TABLE IF NOT EXISTS ${tableName} (
                    id INT,
                    name VARCHAR(100),
                    value INT
                )
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """
            
            // 4. Create Kafka Streaming Job
            sql """
                CREATE JOB ${jobName}
                ON STREAMING
                DO 
                INSERT INTO ${tableName}
                SELECT * FROM kafka(
                    "catalog" = "${catalogName}",
                    "database" = "default",
                    "table" = "${topicName}",
                    "kafka_default_offsets" = "OFFSET_BEGINNING",
                    "max_batch_rows" = "50"
                )
            """
            
            // 5. Verify job was created
            def result = sql "SHOW JOB ${jobName}"
            logger.info("Job created: ${result}")
            assertTrue(result.size() > 0)
            
            // 6. Check job status
            def jobStatus = sql "SHOW JOB STATUS ${jobName}"
            logger.info("Job status: ${jobStatus}")
            
        } finally {
            // Cleanup
            try {
                sql "STOP JOB ${jobName}"
            } catch (Exception e) {
                // Ignore
            }
            sql "DROP JOB IF EXISTS ${jobName}"
            sql "DROP TABLE IF EXISTS ${tableName}"
            sql "DROP CATALOG IF EXISTS ${catalogName}"
        }
    } else {
        logger.info("Kafka test is disabled, skipping test_kafka_streaming_basic")
    }
}
