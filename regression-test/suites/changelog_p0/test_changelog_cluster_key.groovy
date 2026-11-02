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

suite("test_changelog_cluster_key", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    def inheritedChangelogDb = "test_changelog_cluster_key_db"

    sql "DROP TABLE IF EXISTS test_changelog_cluster_key FORCE"
    sql "DROP DATABASE IF EXISTS ${inheritedChangelogDb} FORCE"
    sql "CREATE DATABASE ${inheritedChangelogDb}"
    sql """
        ALTER DATABASE ${inheritedChangelogDb} SET PROPERTIES (
            "changelog.enable" = "true"
        )
    """

    setFeConfigTemporary([random_add_order_by_keys_for_mow: true]) {
        test {
            sql """
                CREATE TABLE test_changelog_cluster_key (
                    id BIGINT NOT NULL,
                    cluster_value INT NOT NULL,
                    payload VARCHAR(32) NOT NULL
                )
                UNIQUE KEY(id)
                ORDER BY(cluster_value)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true",
                    "light_schema_change" = "true",
                    "changelog.enable" = "true"
                )
            """
            exception "Unique merge-on-write tables with cluster keys do not support changelog"
        }

        // The changelog of this table comes from the database default only.
        sql """
            CREATE TABLE ${inheritedChangelogDb}.test_changelog_without_cluster_key (
                id BIGINT NOT NULL,
                cluster_value INT NOT NULL,
                payload VARCHAR(32) NOT NULL
            )
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "storage_format" = "V2"
            )
        """

        order_qt_effective_changelog_properties """
            SELECT PROPERTY_NAME, PROPERTY_VALUE
            FROM information_schema.table_properties
            WHERE TABLE_CATALOG = 'internal'
                AND TABLE_SCHEMA = '${inheritedChangelogDb}'
                AND TABLE_NAME = 'test_changelog_without_cluster_key'
                AND PROPERTY_NAME IN ('binlog.enable', 'changelog.enable')
            ORDER BY PROPERTY_NAME
        """

        qt_show_create_without_random_cluster_key """
            SHOW CREATE TABLE ${inheritedChangelogDb}.test_changelog_without_cluster_key
        """
    }
}
