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

suite("test_cloud_command_restrictions", "cloud_auth") {
    if (!isCloudMode()) {
        return
    }

    def user = "test_cloud_command_restrictions_admin"
    def password = "Cloud12345"
    sql "DROP USER IF EXISTS ${user}"
    sql "CREATE USER ${user} IDENTIFIED BY '${password}' DEFAULT ROLE 'admin'"

    def cloudUnsupportedStatements = [
            "ADMIN CANCEL REBALANCE DISK",
            "ADMIN CANCEL REPAIR TABLE cloud_restriction_db.missing_cloud_table",
            "ADMIN CHECK TABLET (10000) PROPERTIES (\"type\" = \"consistency\")",
            "ADMIN CLEAN TRASH",
            "ADMIN REBALANCE DISK",
            "ADMIN REPAIR TABLE cloud_restriction_db.missing_cloud_table",
            "ADMIN SET TABLE cloud_restriction_db.missing_cloud_table PARTITION VERSION "
                    + "PROPERTIES (\"partition_id\" = \"1\", \"visible_version\" = \"2\")",
            "ADMIN SET REPLICA STATUS PROPERTIES (\"tablet_id\" = \"1\", \"backend_id\" = \"2\", "
                    + "\"status\" = \"ok\")",
            "ADMIN SET REPLICA VERSION PROPERTIES (\"tablet_id\" = \"1\", \"backend_id\" = \"2\", "
                    + "\"version\" = \"3\")",
            "ALTER RESOURCE missing_cloud_resource PROPERTIES (\"s3.connection.maximum\" = \"100\")",
            "ALTER STORAGE POLICY missing_cloud_policy PROPERTIES (\"cooldown_ttl\" = \"86400\")",
            "BACKUP SNAPSHOT cloud_restriction_db.cloud_restriction_snapshot "
                    + "TO cloud_restriction_repository",
            "CANCEL DECOMMISSION BACKEND '127.0.0.1:9050'",
            "SHOW TABLET STORAGE FORMAT"
    ]

    def cloudRootOnlyStatements = [
            "ADMIN SET FRONTEND CONFIG (\"enable_udf_in_load\" = \"true\")",
            "ADMIN SHOW REPLICA DISTRIBUTION FROM cloud_restriction_db.missing_cloud_table"
    ]

    connect(user, password, context.config.jdbcUrl) {
        cloudUnsupportedStatements.each { statement ->
            test {
                sql statement
                exception "Unsupported operation"
            }
        }
        cloudRootOnlyStatements.each { statement ->
            test {
                sql statement
                exception "Unsupported operation"
            }
        }
    }

    cloudUnsupportedStatements.each { statement ->
        test {
            sql statement
            exception "Unsupported operation"
        }
    }
}
