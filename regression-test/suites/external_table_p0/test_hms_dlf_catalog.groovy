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

suite("test_hms_dlf_catalog", "p0,external") {
    sql "DROP CATALOG IF EXISTS test_hms_dlf_legacy"
    sql "DROP CATALOG IF EXISTS test_hms_dlf_canonical"
    sql "DROP CATALOG IF EXISTS test_hms_dlf_invalid"

    // These synthetic credentials exercise CREATE/ALTER validation without contacting a cloud service.
    sql """
        CREATE CATALOG test_hms_dlf_legacy PROPERTIES (
            "type" = "hms",
            "hive.metastore.type" = "DLF",
            "dlf.access_key" = "test-access-key",
            "dlf.secret_key" = "test-secret-key",
            "dlf.session_token" = "test-session-token",
            "dlf.region" = "test-region",
            "dlf.uid" = "test-account",
            "dlf.catalog_id" = "test-catalog",
            "test_connection" = "false"
        )
    """
    qt_show_legacy "SHOW CREATE CATALOG test_hms_dlf_legacy"

    // The custom metadata endpoint does not imply an OSS region or endpoint.
    sql """
        CREATE CATALOG test_hms_dlf_canonical PROPERTIES (
            "type" = "hms",
            "hive.metastore.type" = "dlf",
            "dlf.catalog.accessKeyId" = "test-canonical-key",
            "dlf.catalog.accessKeySecret" = "test-canonical-secret",
            "dlf.catalog.securityToken" = "test-canonical-token",
            "dlf.catalog.endpoint" = "dlf.example.test",
            "dlf.catalog.region" = "test-region",
            "dlf.catalog.id" = "test-catalog",
            "oss.endpoint" = "oss-storage-region.aliyuncs.com",
            "oss.region" = "storage-region",
            "test_connection" = "false"
        )
    """
    qt_show_canonical "SHOW CREATE CATALOG test_hms_dlf_canonical"

    sql """
        ALTER CATALOG test_hms_dlf_canonical SET PROPERTIES (
            "dlf.catalog.accessKeySecret" = "test-rotated-secret",
            "dlf.catalog.securityToken" = "test-rotated-token",
            "dlf.catalog.endpoint" = "dlf-new.example.test",
            "dlf.catalog.id" = "test-next-catalog"
        )
    """
    qt_show_altered "SHOW CREATE CATALOG test_hms_dlf_canonical"

    test {
        sql """
            CREATE CATALOG test_hms_dlf_invalid PROPERTIES (
                "type" = "hms", "hive.metastore.type" = "dlf", "test_connection" = "false"
            )
        """
        exception "dlf.access_key is required"
    }
    test {
        sql """
            CREATE CATALOG test_hms_dlf_invalid PROPERTIES (
                "type" = "hms", "hive.metastore.type" = "dlf",
                "dlf.access_key" = "test-key", "dlf.endpoint" = "dlf.example.test",
                "test_connection" = "false"
            )
        """
        exception "dlf.secret_key is required"
    }
    test {
        sql """
            CREATE CATALOG test_hms_dlf_invalid PROPERTIES (
                "type" = "hms", "hive.metastore.type" = "dlf",
                "dlf.access_key" = "test-key", "dlf.secret_key" = "test-secret",
                "test_connection" = "false"
            )
        """
        exception "dlf.endpoint is required"
    }
    test {
        sql """
            ALTER CATALOG test_hms_dlf_legacy SET PROPERTIES ("dlf.secret_key" = "")
        """
        exception "dlf.secret_key is required"
    }
    // A rejected ALTER must preserve the previous catalog configuration.
    qt_show_after_rejected_alter "SHOW CREATE CATALOG test_hms_dlf_legacy"

    test {
        sql """
            CREATE CATALOG test_hms_dlf_invalid PROPERTIES (
                "type" = "hms", "test_connection" = "false"
            )
        """
        exception "HMS URI ('hive.metastore.uris') is required"
    }
    test {
        sql """
            CREATE CATALOG test_hms_dlf_invalid PROPERTIES (
                "type" = "hms", "hive.metastore.type" = "GLUE", "test_connection" = "false"
            )
        """
        exception "AWS Glue as an HMS thrift metastore has been removed"
    }
}
