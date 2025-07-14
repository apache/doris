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

suite("test_like_table") {
    sql "drop table if exists tmp_table"
    sql """
    CREATE TABLE tmp_table (
	    `str` varchar(65533) NULL
    ) ENGINE=OLAP 
    DUPLICATE KEY(`str`) DISTRIBUTED BY RANDOM BUCKETS 15
    PROPERTIES("replication_allocation" = "tag.location.default: 1");
    """

    sql "insert into tmp_table values ('\b'),('\\b'),('b'),('\\b'),('%'),('a'),('\\%'),('\\\\%'),('\\\\\\%'),('\\\\\\\\%')"

    qt_test """
    select str, str like '\b' from tmp_table order by str;
    """
    qt_test """
    select str, str like '\\b' from tmp_table order by str;
    """
    qt_test """
    select str, str like 'b' from tmp_table order by str;
    """
    qt_test """
    select str, str like '\\\\%' from tmp_table order by str;
    """
    qt_test """
    select str, str like '\\\\\\%' from tmp_table order by str;
    """
    qt_test """
    select str, str like '\\\\\\\\%' from tmp_table order by str;
    """
    qt_test """
    select str, str like '\\%' from tmp_table order by str;
    """

    sql """
    set disable_nereids_expression_rules='LIKE_TO_EQUAL';
    """

    qt_test_d """
    select str, str like '\b' from tmp_table order by str;
    """
    qt_test_d """
    select str, str like '\\b' from tmp_table order by str;
    """
    qt_test_d """
    select str, str like 'b' from tmp_table order by str;
    """
    qt_test_d """
    select str, str like '\\\\%' from tmp_table order by str;
    """
    qt_test_d """
    select str, str like '\\\\\\%' from tmp_table order by str;
    """
    qt_test_d """
    select str, str like '\\\\\\\\%' from tmp_table order by str;
    """
    qt_test_d """
    select str, str like '\\%' from tmp_table order by str;
    """
}