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

// The parser counts a supplementary character such as an emoji as one character, while the Java string
// of the view query counts it as two; the view definition is rewritten on that string.
suite("test_create_view_supplementary_chars") {
    sql "drop table if exists test_create_view_supplementary_chars_base"
    sql """
        create table test_create_view_supplementary_chars_base (k1 int, v1 varchar(32))
        duplicate key(k1)
        distributed by hash(k1) buckets 1
        properties('replication_num' = '1')
    """
    sql "insert into test_create_view_supplementary_chars_base values (1, 'a😀'), (2, 'b'), (3, '😀😀😀')"

    sql "drop view if exists test_create_view_supplementary_chars_view"
    sql """
        create view test_create_view_supplementary_chars_view as
        select '😀😀' as e, k1, v1 from test_create_view_supplementary_chars_base where v1 != '😀😀😀'
    """
    qt_show_view "show create view test_create_view_supplementary_chars_view"
    order_qt_select_view "select * from test_create_view_supplementary_chars_view"

    sql "drop view if exists test_create_view_supplementary_chars_alias"
    sql """
        create view test_create_view_supplementary_chars_alias (c1, c2) as
        select '😀😀', k1 from test_create_view_supplementary_chars_base
    """
    qt_show_alias "show create view test_create_view_supplementary_chars_alias"
    order_qt_select_alias "select * from test_create_view_supplementary_chars_alias"
}
