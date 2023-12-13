
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

suite("test_date_exprs") {
    def tbName = "test_date_exprs"
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
            create table ${tbName}(k1 datetimev1, k2 int) distributed by hash(k1) buckets 1 properties("replication_num" = "1");
        """
    sql """ insert into ${tbName} values("2016-11-04 00:00:01", 1); """

    qt_sql1 """ select dt
             from
             (
             select cast(k1 as datev1) as dt
             from ${tbName}
             ) r; """
    qt_sql2 """ select dt
             from
             (
             select cast(k1 as datev1) as dt
             from ${tbName}
             ) r; """
    sql "DROP TABLE ${tbName}"
}
