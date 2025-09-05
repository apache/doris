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

suite("test_iceberg_optimize_actions_ddl", "p0,external,doris,external_docker,external_docker_doris") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name = "test_iceberg_optimize_actions_ddl"
    String db_name = "test_db"
    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

    sql """switch ${catalog_name}"""
    sql """use ${db_name}"""
    def table_name = "test_iceberg_systable_partitioned"

    // Test 1: Test rollback_to_snapshot action
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "rollback_to_snapshot", "snapshot_id" = "123456789")
        """
        exception "Iceberg rollback_to_snapshot procedure is not implemented yet"
    }

    // Test 2: Test rollback_to_timestamp action  
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "rollback_to_timestamp", "timestamp" = "2024-01-01T00:00:00")
        """
        exception "Iceberg rollback_to_timestamp procedure is not implemented yet"
    }

    // Test 3: Test set_current_snapshot action
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "set_current_snapshot", "snapshot_id" = "987654321")
        """
        exception "Iceberg set_current_snapshot procedure is not implemented yet"
    }

    // Test 4: Test cherrypick_snapshot action
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "cherrypick_snapshot", "snapshot_id" = "555666777")
        """
        exception "Iceberg cherrypick_snapshot procedure is not implemented yet"
    }

    // Test 5: Test fast_forward action with target_branch
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "fast_forward", "target_branch" = "main")
        """
        exception "Iceberg fast_forward procedure is not implemented yet"
    }

    // Test 6: Test fast_forward action with target_snapshot
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "fast_forward", "target_snapshot" = "111222333")
        """
        exception "Iceberg fast_forward procedure is not implemented yet"
    }

    // Test 7: Test expire_snapshots action
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "expire_snapshots", "older_than" = "2024-01-01T00:00:00")
        """
        exception "Iceberg expire_snapshots procedure is not implemented yet"
    }

    // Test 8: Test rewrite_data_files action
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "rewrite_data_files", "target_file_size" = "134217728")
        """
        exception "Iceberg rewrite_data_files procedure is not implemented yet"
    }


    // Test 9: Test validation - missing required property
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "rollback_to_snapshot")
        """
        exception "Missing required property: snapshot_id"
    }

    // Test 10: Test validation - invalid snapshot_id format
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "rollback_to_snapshot", "snapshot_id" = "invalid_id")
        """
        exception "Invalid snapshot_id format: invalid_id"
    }

    // Test 12: Test validation - invalid timestamp format
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "rollback_to_timestamp", "timestamp" = "invalid_timestamp")
        """
        exception "Invalid timestamp format"
    }
}