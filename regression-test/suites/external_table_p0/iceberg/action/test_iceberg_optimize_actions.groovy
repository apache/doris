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

    // Test rollback_to_snapshot action
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "rollback_to_snapshot", "snapshot_id" = "123456789")
        """
        exception "Iceberg rollback_to_snapshot procedure is not implemented yet"
    }

    // Test rollback_to_timestamp action  
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "rollback_to_timestamp", "timestamp" = "2024-01-01T00:00:00")
        """
        exception "Iceberg rollback_to_timestamp procedure is not implemented yet"
    }

    // Test set_current_snapshot action
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "set_current_snapshot", "snapshot_id" = "987654321")
        """
        exception "Iceberg set_current_snapshot procedure is not implemented yet"
    }

    // Test cherrypick_snapshot action
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "cherrypick_snapshot", "snapshot_id" = "555666777")
        """
        exception "Iceberg cherrypick_snapshot procedure is not implemented yet"
    }

    // Test fast_forward action with branch
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "fast_forward", "branch" = "main", "to" = "111222333")
        """
        exception "Iceberg fast_forward procedure is not implemented yet"
    }

    // Test expire_snapshots action
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "expire_snapshots", "older_than" = "2024-01-01T00:00:00")
        """
        exception "Iceberg expire_snapshots procedure is not implemented yet"
    }

    // Test rewrite_data_files action
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "rewrite_data_files", "target-file-size-bytes" = "134217728")
        """
        exception "Iceberg rewrite_data_files procedure is not implemented yet"
    }


    // Test validation - missing required property
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "rollback_to_snapshot")
        """
        exception "Missing required argument: snapshot_id"
    }

    // Test validation - negative snapshot_id
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "rollback_to_snapshot", "snapshot_id" = "-123")
        """
        exception "snapshot_id must be positive, got: -123"
    }

    // Test validation - zero snapshot_id
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "cherrypick_snapshot", "snapshot_id" = "0")
        """
        exception "snapshot_id must be positive, got: 0"
    }

    // Test validation - empty snapshot_id
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "set_current_snapshot", "snapshot_id" = "")
        """
        exception "Invalid snapshot_id format:"
    }

    // Test validation - missing timestamp for rollback_to_timestamp
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "rollback_to_timestamp")
        """
        exception "Missing required argument: timestamp"
    }

    // Test expire_snapshots with invalid older_than timestamp
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "expire_snapshots", "older_than" = "not-a-timestamp")
        """
        exception "Invalid older_than format"
    }

    // Test expire_snapshots with negative timestamp
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "expire_snapshots", "older_than" = "-1000")
        """
        exception "older_than timestamp must be non-negative"
    }

    // Test validation - retain_last must be at least 1
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "expire_snapshots", "retain_last" = "0")
        """
        exception "retain_last must be positive, got: 0"
    }

    // Test expire_snapshots with invalid retain_last format
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "expire_snapshots", "retain_last" = "not-a-number")
        """
        exception "Invalid retain_last format: not-a-number"
    }

    // Test expire_snapshots with negative retain_last
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "expire_snapshots", "retain_last" = "-5")
        """
        exception "retain_last must be positive, got: -5"
    }

    // Test expire_snapshots with neither older_than nor retain_last
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "expire_snapshots")
        """
        exception "At least one of 'older_than' or 'retain_last' must be specified"
    }

    // Test expire_snapshots with valid timestamp format (milliseconds)
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "expire_snapshots", "older_than" = "1640995200000")
        """
        exception "Iceberg expire_snapshots procedure is not implemented yet"
    }

    // Test expire_snapshots with valid ISO datetime
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "expire_snapshots", "older_than" = "2024-01-01T12:30:45")
        """
        exception "Iceberg expire_snapshots procedure is not implemented yet"
    }

    // Test expire_snapshots with valid retain_last and older_than
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "expire_snapshots", "older_than" = "2024-01-01T00:00:00", "retain_last" = "5")
        """
        exception "Iceberg expire_snapshots procedure is not implemented yet"
    }

    // Test unknown action
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "unknown_action")
        """
        exception "Unsupported Iceberg procedure: unknown_action."
    }

    // Test missing action property
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("some_param" = "value")
        """
        exception "OPTIMIZE TABLE requires 'action' property to be specified"
    }

    // Test unknown property for specific action
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "rollback_to_snapshot", "snapshot_id" = "123", "unknown_param" = "value")
        """
        exception "Unknown argument: unknown_param"
    }

    // Test rewrite_data_files with invalid target-file-size-bytes
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "rewrite_data_files", "target-file-size-bytes" = "0")
        """
        exception "target-file-size-bytes must be positive, got: 0"
    }

    // Test rewrite_data_files with invalid file size format
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "rewrite_data_files", "target-file-size-bytes" = "not-a-number")
        """
        exception "Invalid target-file-size-bytes format: not-a-number"
    }

    // Test set_current_snapshot with ref parameter
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "set_current_snapshot", "ref" = "main")
        """
        exception "Iceberg set_current_snapshot procedure is not implemented yet"
    }

    // Test set_current_snapshot with both snapshot_id and ref
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "set_current_snapshot", "snapshot_id" = "123", "ref" = "main")
        """
        exception "snapshot_id and ref are mutually exclusive, only one can be provided"
    }

    // Test set_current_snapshot with neither snapshot_id nor ref
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "set_current_snapshot")
        """
        exception "Either snapshot_id or ref must be provided"
    }

    // Test very large snapshot_id (within Long range)
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "rollback_to_snapshot", "snapshot_id" = "9223372036854775807")
        """
        exception "Iceberg rollback_to_snapshot procedure is not implemented yet"
    }

    // Test snapshot_id exceeding Long.MAX_VALUE
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "rollback_to_snapshot", "snapshot_id" = "99999999999999999999")
        """
        exception "Invalid snapshot_id format: 99999999999999999999"
    }

    // Test whitespace handling in parameters
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "rollback_to_snapshot", "snapshot_id" = "  123456789  ")
        """
        exception "Iceberg rollback_to_snapshot procedure is not implemented yet"
    }

    // Test case sensitivity in action names
    test {
        sql """
            OPTIMIZE TABLE ${catalog_name}.${db_name}.${table_name}
            PROPERTIES("action" = "ROLLBACK_TO_SNAPSHOT", "snapshot_id" = "123456789")
        """
        exception "Iceberg rollback_to_snapshot procedure is not implemented yet"
    }
}