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

suite("test_string_pad_function") {
    sql """
        drop table if exists table_pad;
    """
    
    sql """
        create table if not exists table_pad (
        a int not null,
        b varchar(10) not null
        )
        ENGINE=OLAP
        distributed by hash(a)
        properties(
        'replication_num' = '1'
        );
    """

    sql """
        insert into table_pad values(1,'100000'), (2,'200000');
    """

    qt_select_lpad """
        select CASE WHEN table_pad.a = 1 THEN CONCAT(LPAD(b, 2, 0), ':00') END result from table_pad order by result;
    """

    qt_select_rpad """
        select CASE WHEN table_pad.a = 1 THEN CONCAT(RPAD(b, 2, 0), ':00') END result from table_pad order by result;
    """

    qt_rpad1 """ SELECT rpad("", 5, ""); """
    qt_rpad2 """ SELECT rpad("123", 5, ""); """
    qt_rpad3 """ SELECT rpad("123", -1, ""); """
    qt_rpad4 """ SELECT rpad(NULL, 1, ""); """
    qt_rpad5 """ SELECT rpad("123", 0, NULL); """
    qt_lpad1 """ SELECT lpad("", 5, ""); """
    qt_lpad2 """ SELECT lpad("123", 5, ""); """
    qt_lpad3 """ SELECT lpad("123", -1, ""); """
    qt_lpad4 """ SELECT lpad(NULL, 0, ""); """
    qt_lpad5 """ SELECT lpad("123", 2, NULL); """
}
