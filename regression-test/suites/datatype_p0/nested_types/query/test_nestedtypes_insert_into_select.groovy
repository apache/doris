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


import com.google.common.collect.Lists
import org.apache.commons.lang3.StringUtils
import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_nestedtypes_insert_into_select", "p0") {
    sql "set enable_nereids_planner=false"
    sql """ADMIN SET FRONTEND CONFIG ('disable_nested_complex_type' = 'false')"""

    // check array<struct>
    sql "DROP TABLE IF EXISTS ast;"
    sql """ CREATE TABLE IF NOT EXISTS ast (col1 varchar(64) NULL, col2 array<struct<a:int,b:string>>) DUPLICATE KEY(`col1`)  DISTRIBUTED BY HASH(`col1`) PROPERTIES ("replication_num" = "1"); """

    // test insert into with literal
    sql "INSERT INTO ast values ('text',[{3,'home'},{4,'work'}]);"
    sql "INSERT INTO ast values ('text1',[(31,'home1'),(41,'work1')]);"
    sql "insert into ast values ('text2' , array(named_struct('amory',1,'b','7777'),named_struct('commiter',2,'b','work')));"
    sql "insert into ast values ('text3' , array(struct(11, 'home'), struct(22, 'work')));"
    sql "insert into ast values ('text4' , array(named_struct('a',1,'b','home'), struct(2, 'work')));"

    order_qt_sql_as """ select * from ast; """

    test {
        sql "insert into ast values ('text' , [named_struct('a',1,'b','home'),named_struct('a',2,'b','work')]);"
        exception "errCode = 2, detailMessage = Sql parser can't convert the result to array, please check your sql."
    }
    test {
        sql "insert into ast values ('text' , [struct(1,'home'),struct(2,'work')]);"
        exception "errCode = 2, detailMessage = Sql parser can't convert the result to array, please check your sql."
    }

    sql "set enable_nereids_planner=true"
    sql " set enable_fallback_to_original_planner=false"

    qt_select "select(4, 'amo', '7', 'comm');"
}
