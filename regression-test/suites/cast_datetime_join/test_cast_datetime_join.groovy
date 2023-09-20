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

suite("test_cast_datetime_join") {
    sql "DROP TABLE IF EXISTS test_datetime1;"

    sql """  CREATE TABLE test_datetime1 properties("replication_allocation" = "tag.location.default: 1") AS
           SELECT cast(("2023-09-16 00:00:00") as datetime) AS work_date
                  ,'a' as data
           union all
           SELECT cast(("2023-09-16 00:00:00") as datetime) AS work_date
                  ,'b' as data
           union all
           SELECT cast(("2023-09-16 00:00:00") as datetime) AS work_date
                  ,'c' as data;
          """

    sql "DROP TABLE IF EXISTS test_datetime2;"

    sql """
         CREATE TABLE test_datetime2 properties("replication_allocation" = "tag.location.default: 1") AS
         SELECT cast(("2023-09-16 00:00:00") as datetime) AS work_date
                ,'1' as data
         union all
         SELECT cast(("2023-09-16 00:00:00") as datetime) AS work_date
                ,'2' as data
         union all
         SELECT cast(("2023-09-16 00:00:00") as datetime) AS work_date
                ,'3' as data;
        """

    qt_select1 " select * from test_datetime1 order by data;"

    qt_select2 " select * from test_datetime2 order by data;"

    qt_select3 """
        select * from test_datetime1 t1
        inner join test_datetime2 t2
        on t1.work_date = t2.work_date order by t2.data;
    """


}
