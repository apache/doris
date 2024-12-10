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

import org.junit.Assert;
import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_database_management_auth","p0,auth_call") {

    String user = 'test_database_management_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_database_management_auth_db'
    def String error_in_cloud = "denied"
    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
        error_in_cloud = "Unsupported"
    }

    try_sql("DROP USER ${user}")
    try_sql """drop database if exists ${dbName}"""

    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user}"""
    sql """create database ${dbName}"""

    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """SHOW FRONTEND CONFIG"""
            exception "denied"
        }
        test {
            sql """ADMIN SET FRONTEND CONFIG ("disable_balance" = "true");"""
            exception "denied"
        }
        test {
            sql """SET global time_zone = "Asia/Shanghai";"""
            exception "denied"
        }
        test {
            sql """INSTALL PLUGIN FROM "";"""
            exception "denied"
        }
        test {
            sql """UNINSTALL PLUGIN demo;"""
            exception "denied"
        }
        test {
            sql """ADMIN SET REPLICA STATUS PROPERTIES("tablet_id" = "000", "backend_id" = "000", "status" = "ok");"""
            exception "denied"
        }
        test {
            sql """ADMIN SET REPLICA VERSION PROPERTIES("tablet_id" = "0", "backend_id" = "0", "version" = "0");"""
            exception "denied"
        }
        test {
            sql """ADMIN SET TABLE tb PARTITION VERSION PROPERTIES("partition_id" = "0", "visible_version" = "0");"""
            exception "denied"
        }
        test {
            sql """admin set table tbl status properties("state" = "NORMAL");"""
            exception "denied"
        }
        test {
            sql """SHOW REPLICA DISTRIBUTION FROM tbl;"""
            exception "${error_in_cloud}"
        }
        test {
            sql """SHOW REPLICA STATUS FROM db1.tbl1;"""
            exception "denied"
        }
        test {
            sql """ADMIN REPAIR TABLE tbl;"""
            exception "denied"
        }
        test {
            sql """ADMIN CANCEL REPAIR TABLE tbl PARTITION(p1);"""
            exception "denied"
        }
        test {
            sql """ADMIN CHECK TABLET (10000, 10001) PROPERTIES("type" = "consistency");"""
            exception "${error_in_cloud}"
        }
        test {
            sql """SHOW TABLET DIAGNOSIS 0;"""
            exception "denied"
        }
        test {
            sql """ADMIN COPY TABLET 10010 PROPERTIES("backend_id" = "10001");"""
            exception "denied"
        }
        test {
            sql """show tablet storage format verbose;"""
            exception "${error_in_cloud}"
        }
        test {
            sql """ADMIN CLEAN TRASH;"""
            exception "denied"
        }
        test {
            sql """RECOVER DATABASE db_name;"""
            exception "denied"
        }
        test {
            sql """ADMIN REBALANCE DISK;"""
            exception "denied"
        }
        test {
            sql """ADMIN CANCEL REBALANCE DISK;"""
            exception "denied"
        }
        test {
            sql """UNSET GLOBAL VARIABLE ALL;"""
            exception "denied"
        }
        test {
            sql """clean all query stats;"""
            exception "denied"
        }
        test {
            sql """REFRESH LDAP ALL;"""
            exception "denied"
        }
    }

    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
