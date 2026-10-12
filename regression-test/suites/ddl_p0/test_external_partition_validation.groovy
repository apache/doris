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

suite("test_external_partition_validation", "p0") {
    // Invalid external partition models must fail in analysis without contacting a remote catalog.
    def rejectPartition = { String engine, String partition, String message ->
        String tableName = "test_external_partition_validation_${engine}"
        sql """DROP TABLE IF EXISTS `${tableName}`"""
        try {
            test {
                sql """
                    CREATE TABLE `${tableName}` (id INT, ts DATETIME, dt DATE)
                    ENGINE=${engine} ${partition}
                """
                exception message
            }
            assertEquals([], sql("""SHOW TABLES LIKE '${tableName}'"""))
        } finally {
            sql """DROP TABLE IF EXISTS `${tableName}`"""
        }
    }

    ["hive", "paimon", "maxcompute"].each { engine ->
        rejectPartition(engine, "PARTITION BY (date_trunc(ts, 'day')) ()",
                "only supports partitioning by columns")
    }
    ["hive", "paimon", "maxcompute", "iceberg"].each { engine ->
        String message = engine == "hive" ? "Partition values expressions is not supported in hive catalog"
                : "does not support explicit partition definitions"
        rejectPartition(engine, "PARTITION BY LIST(dt) (PARTITION p1 VALUES IN ('2026-01-01'))", message)
    }
    ["paimon", "maxcompute", "iceberg"].each { engine ->
        rejectPartition(engine, "PARTITION BY RANGE(dt) (PARTITION p1 VALUES LESS THAN ('2026-01-02'))",
                "does not support explicit partition definitions")
    }
    rejectPartition("hive", "PARTITION BY RANGE(dt) ()", "Only support 'LIST' partition type in hive catalog")
    rejectPartition("elasticsearch", "PARTITION BY LIST(dt) ()", "Elasticsearch table only permit range partition")
    rejectPartition("elasticsearch", "PARTITION BY RANGE(id, dt) ()",
            "Elasticsearch table's partition column could only be a single column")
    rejectPartition("jdbc", "PARTITION BY LIST(dt) ()", "Create jdbc table should not contain partition desc")
    ["mysql", "odbc", "broker"].each { engine ->
        rejectPartition(engine, "PARTITION BY LIST(dt) ()", "odbc, mysql and broker table is no longer supported")
    }
}
