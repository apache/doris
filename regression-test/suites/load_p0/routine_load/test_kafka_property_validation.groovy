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

suite("test_kafka_property_validation", "p0") {
    sql "DROP TABLE IF EXISTS test_kafka_property_validation"
    sql """
        CREATE TABLE test_kafka_property_validation (id INT)
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    // All failures must happen during analysis, without contacting a Kafka broker.
    def rejected = ["sasl.kerberos.kinit.cmd", "sasl.kerberos.keytab",
                    "sasl.kerberos.min.time.before.relogin", "plugin.library.paths",
                    "ssl.engine.location", "ssl.engine.id", "ssl.providers", "unknown.future.property"]
    rejected.each { key ->
        test {
            sql """
                CREATE ROUTINE LOAD test_kafka_property_validation_job ON test_kafka_property_validation
                FROM KAFKA (
                    "kafka_broker_list" = "127.0.0.1:1",
                    "kafka_topic" = "unused",
                    "property.${key}" = "unused"
                )
            """
            exception "Kafka property '${key}' is not allowed in Routine Load"
        }
    }
    ["ssl.ca.location", "ssl.certificate.location", "ssl.key.location"].each { key ->
        test {
            sql """
                CREATE ROUTINE LOAD test_kafka_property_validation_job ON test_kafka_property_validation
                FROM KAFKA (
                    "kafka_broker_list" = "127.0.0.1:1",
                    "kafka_topic" = "unused",
                    "property.${key}" = "/etc/ssl/private/unused.pem"
                )
            """
            exception "must reference a file registered with CREATE FILE"
        }
    }
}
