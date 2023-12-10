
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

suite("test_date_in_predicate") {
    def tbName = "test_date_in_predicate"
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName} (
                c0 int,
                c1 char(10),
                c2 datev1,
                c3 datev2
            )
            UNIQUE KEY(c0)
            DISTRIBUTED BY HASH(c0) BUCKETS 5 properties("replication_num" = "1");
        """
    sql "insert into ${tbName} values(1, 'test1', '2000-01-01', '2000-01-01')"
    sql "insert into ${tbName} values(2, 'test2', '2000-02-02', '2000-02-02')"
    sql "insert into ${tbName} values(3, 'test3', '2000-03-02', '2000-03-02')"

    qt_sql1 "select * from ${tbName} where c2 in ('2000-02-02')"
    qt_sql2 "select * from ${tbName} where c2 not in ('2000-02-02')"
    qt_sql3 "select * from ${tbName} where c3 in ('2000-02-02')"
    qt_sql4 "select * from ${tbName} where c3 not in ('2000-02-02')"
    sql "DROP TABLE ${tbName}"
}
