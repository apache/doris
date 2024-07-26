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

suite ("test_row_store_page_size_cloud") {

    sql """ DROP TABLE IF EXISTS ps_table_1; """

    sql """
            create table ps_table_1(
                k1 int not null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) null
            )
            unique key (k1,k2)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1", "store_row_column" = "true");
        """

    test {
        sql "show create table ps_table_1;"
        check { result, exception, startTime, endTime ->
            assertTrue(result[0][1].contains("\"row_store_page_size\" = \"16384\""))
        }
    }

    sql "insert into ps_table_1 select 1,1,1,'a';"
    sql "insert into ps_table_1 select 2,2,2,'b';"
    sql "insert into ps_table_1 select 3,3,null,'c';"

    explain {
        sql("select * from ps_table_1 where k1=1 and k2=1;")
        contains("SHORT")
    }

    qt_select_star "select * from ps_table_1 where k1=1 and k2=1;"
    qt_select_star "select * from ps_table_1 where k1=3 and k2=3;"

    sql """ DROP TABLE IF EXISTS ps_table_1; """

    sql """ DROP TABLE IF EXISTS ps_table_2; """

    sql """
            create table ps_table_2(
                k1 int not null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) null
            )
            unique key (k1,k2)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1", "store_row_column" = "true", "row_store_page_size" = "8190");
        """

    test {
        sql "show create table ps_table_2;"
        check { result, exception, startTime, endTime ->
            assertTrue(result[0][1].contains("\"row_store_page_size\" = \"8192\""))
        }
    }
    
    sql "insert into ps_table_2 select 1,1,1,'a';"
    sql "insert into ps_table_2 select 2,2,2,'b';"
    sql "insert into ps_table_2 select 3,3,null,'c';"

    explain {
        sql("select * from ps_table_2 where k1=1 and k2=1;")
        contains("SHORT")
    }

    qt_select_star "select * from ps_table_2 where k1=1 and k2=1;"
    
    sql """ DROP TABLE IF EXISTS ps_table_2; """
}
