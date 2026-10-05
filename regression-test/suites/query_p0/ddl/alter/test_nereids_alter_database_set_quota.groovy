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

suite("test_nereids_alter_database_set_quota") {
    String quotaDb1= "quotaDb2"

    sql """DROP database IF EXISTS ${quotaDb1}"""
    sql """create database ${quotaDb1}"""
    sql """use ${quotaDb1}"""
    qt_show_data_sql """show data;"""
    checkNereidsExecute("ALTER DATABASE ${quotaDb1} SET DATA QUOTA 100M;");
    qt_show_data_sql_100m """show data"""
    checkNereidsExecute("ALTER DATABASE ${quotaDb1} SET DATA QUOTA 1024G;");
    qt_show_data_sql_1024g """show data"""
    checkNereidsExecute("ALTER DATABASE ${quotaDb1} SET DATA QUOTA 100T;");
    qt_show_data_sql_100t """show data"""
    checkNereidsExecute("ALTER DATABASE ${quotaDb1} SET DATA QUOTA 10995116277760;");
    qt_show_data_sql_10t """show data"""
    checkNereidsExecute("ALTER DATABASE ${quotaDb1} SET REPLICA QUOTA 102400;");
    qt_show_data_sql_replica_num """show data"""
    checkNereidsExecute("ALTER DATABASE ${quotaDb1} SET TRANSACTION QUOTA 100000;");
    sql """drop database ${quotaDb1}"""
}
