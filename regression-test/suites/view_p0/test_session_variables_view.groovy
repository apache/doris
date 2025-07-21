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

suite("test_session_variables_view","view") {
    String suiteName = "test_session_variables_view"
    String tableName = "${suiteName}_table"
    String viewName = "${suiteName}_view"
    sql """drop table if exists `${tableName}`"""
    sql """drop view if exists ${viewName};"""

    sql """
        CREATE TABLE ${tableName}
        (
            k2 TINYINT,
            k3 INT not null
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

     sql """
        insert into ${tableName} values(1,1),(2,2),(3,3);
        """

    test {
          sql """
              CREATE VIEW ${viewName}
              AS
              SELECT k2,k3 from ${tableName} group by k2;
              """
          exception "not in aggregate's output"
      }

    sql""" set sql_mode=""; """

    sql """
         CREATE VIEW ${viewName}
         AS
         SELECT k2,k3 from ${tableName} group by k2;
        """

    order_qt_view_1 "SELECT * FROM ${viewName}"

    sql""" set sql_mode="ONLY_FULL_GROUP_BY"; """

    order_qt_view_2 "SELECT * FROM ${viewName}"


//     sql """drop table if exists `${tableName}`"""
//     sql """drop view if exists ${viewName};"""
}
