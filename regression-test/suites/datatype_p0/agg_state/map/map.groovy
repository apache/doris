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

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

suite("test_agg_state_map") {
    sql "set enable_agg_state=true"
    sql "DROP TABLE IF EXISTS a_table"
    sql """
    create table a_table(
        k1 int null,
        k2 agg_state<map_agg(int,int)> generic
    )
    aggregate key (k1)
    distributed BY hash(k1) buckets 3
    properties("replication_num" = "1");
    """
    sql "insert into a_table values(1,map_agg_state(1,1));"
    sql "insert into a_table values(1,map_agg_state(2,2));"
    sql "insert into a_table values(1,map_agg_state(1,11));"
    sql "insert into a_table values(1,map_agg_state(2,22));"
    sql "insert into a_table values(1,map_agg_state(null, 100));"
    sql "insert into a_table values(2,map_agg_state(3,3));"
    sql "insert into a_table values(2,map_agg_state(4,null));"
    sql "insert into a_table values(2,map_agg_state(null,null));"
    sql "insert into a_table values(2,map_agg_state(null,400));"

    qt_test "select k1,map_agg_merge(k2) from a_table group by k1 order by k1;"
}
