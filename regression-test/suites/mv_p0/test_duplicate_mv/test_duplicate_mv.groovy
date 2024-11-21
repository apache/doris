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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite ("test_duplicate_mv") {
    sql """ DROP TABLE IF EXISTS duplicate_table; """

    sql """
            create table duplicate_table(
                k1 int null,
                k2 int null,
                k3 bigint null,
                k4 bigint null
            )
            duplicate key (k1,k2,k3,k4)
            distributed BY hash(k4) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into duplicate_table select 1,2,3,4;"

    createMV("create materialized view deduplicate as select k1, k2, k3, k4 from duplicate_table group by k1, k2, k3, k4;")

   sql "insert into duplicate_table select 2,3,4,5;"
   sql "insert into duplicate_table select 1,2,3,4;"

    qt_select_star "select * from duplicate_table order by k1;"

    mv_rewrite_success("select k1, k2, k3, k4 from duplicate_table group by k1, k2, k3, k4;", "deduplicate")
    
    qt_select_mv1 "select k1, k2, k3, k4 from duplicate_table group by k1, k2, k3, k4 order by k1;"
 
}