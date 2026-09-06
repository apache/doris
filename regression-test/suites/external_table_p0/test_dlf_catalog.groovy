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

suite("test_dlf_catalog", "p0,external") {
    sql "DROP CATALOG IF EXISTS test_iceberg_dlf_catalog_validation"
    sql "DROP CATALOG IF EXISTS test_paimon_dlf_catalog_validation"
    sql "DROP CATALOG IF EXISTS test_iceberg_dlf_catalog_missing_secret"
    sql "DROP CATALOG IF EXISTS test_paimon_dlf_catalog_missing_secret"
    sql "DROP CATALOG IF EXISTS test_iceberg_dlf_catalog_unreachable"

    // test_connection=false keeps provider and alias validation independent of an external DLF service.
    sql """
        CREATE CATALOG test_iceberg_dlf_catalog_validation PROPERTIES (
            "type" = "iceberg",
            "iceberg.catalog.type" = "dlf",
            "warehouse" = "oss://dlf-regression-test/iceberg",
            "dlf.access_key" = "test-legacy-access-key",
            "dlf.secret_key" = "test-legacy-secret-key",
            "dlf.session_token" = "test-legacy-session-token",
            "dlf.endpoint" = "dlf-vpc.test-region.aliyuncs.com",
            "test_connection" = "false"
        )
    """

    qt_show_iceberg_dlf_catalog "SHOW CREATE CATALOG test_iceberg_dlf_catalog_validation"

    sql """
        CREATE CATALOG test_paimon_dlf_catalog_validation PROPERTIES (
            "type" = "paimon",
            "paimon.catalog.type" = "dlf",
            "warehouse" = "oss://dlf-regression-test/paimon",
            "dlf.catalog.accessKeyId" = "test-canonical-access-key",
            "dlf.catalog.accessKeySecret" = "test-canonical-secret-key",
            "dlf.catalog.securityToken" = "test-canonical-session-token",
            "dlf.catalog.endpoint" = "dlf.test-region.aliyuncs.com",
            "test_connection" = "false"
        )
    """

    qt_show_paimon_dlf_catalog "SHOW CREATE CATALOG test_paimon_dlf_catalog_validation"

    test {
        sql """
            CREATE CATALOG test_iceberg_dlf_catalog_missing_secret PROPERTIES (
                "type" = "iceberg",
                "iceberg.catalog.type" = "dlf",
                "warehouse" = "oss://dlf-regression-test/iceberg-invalid",
                "dlf.access_key" = "test-access-key",
                "dlf.region" = "test-region",
                "test_connection" = "false"
            )
        """
        exception "dlf.secret_key is required"
    }

    test {
        sql """
            CREATE CATALOG test_paimon_dlf_catalog_missing_secret PROPERTIES (
                "type" = "paimon",
                "paimon.catalog.type" = "dlf",
                "warehouse" = "oss://dlf-regression-test/paimon-invalid",
                "dlf.catalog.accessKeyId" = "test-access-key",
                "dlf.catalog.endpoint" = "dlf.test-region.aliyuncs.com",
                "test_connection" = "false"
            )
        """
        exception "dlf.secret_key is required"
    }

    test {
        sql """
            CREATE TABLE test_iceberg_dlf_catalog_validation.test_database.test_table (
                id BIGINT
            ) ENGINE=ICEBERG
        """
        exception "not supported"
    }

    test {
        sql """
            CREATE CATALOG test_iceberg_dlf_catalog_unreachable PROPERTIES (
                "type" = "iceberg",
                "iceberg.catalog.type" = "dlf",
                "warehouse" = "oss://dlf-regression-test/unreachable",
                "dlf.access_key" = "test-access-key",
                "dlf.secret_key" = "test-secret-key",
                "dlf.region" = "test-region",
                "dlf.endpoint" = "http://127.0.0.1:1",
                "oss.endpoint" = "oss-test-region.aliyuncs.com",
                "oss.region" = "test-region",
                "test_connection" = "true"
            )
        """
        exception "Iceberg DLF connectivity test failed"
    }
}
