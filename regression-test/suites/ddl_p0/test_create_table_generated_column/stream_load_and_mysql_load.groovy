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

suite("test_generated_column_stream_mysql_load") {
    sql "SET enable_nereids_planner=true;"
    sql "SET enable_fallback_to_original_planner=false;"
    String db = context.config.getDbNameByFile(context.file)

    sql "drop table if exists test_gen_col_common_steam_mysql_load"
    qt_common_default """create table test_gen_col_common_steam_mysql_load(a int,b int,c double generated always as (abs(a+b)) not null)
    DISTRIBUTED BY HASH(a)
    PROPERTIES("replication_num" = "1");
    ;"""

    sql "drop table if exists gencol_refer_gencol_steam_mysql_load"
    qt_gencol_refer_gencol """
    create table gencol_refer_gencol_steam_mysql_load(a int,c double generated always as (abs(a+b)) not null,b int, d int generated always as(c+1))
    DISTRIBUTED BY HASH(a)
    PROPERTIES("replication_num" = "1");
    """

    // test load
    streamLoad {
        table 'test_gen_col_common_steam_mysql_load'
        set 'column_separator', ','
        file 'gen_col_data.csv'
        set 'columns', 'a,b'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_test_stream_load_common "select * from test_gen_col_common_steam_mysql_load order by 1,2,3"

    streamLoad {
        table 'gencol_refer_gencol_steam_mysql_load'
        set 'column_separator', ','
        file 'gen_col_data.csv'
        set 'columns', 'a,b'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_test_stream_load_refer_gencol "select * from gencol_refer_gencol_steam_mysql_load order by 1,2,3,4"

    streamLoad {
        table 'test_gen_col_common_steam_mysql_load'
        set 'strip_outer_array', 'true'
        set 'format', 'json'
        set 'columns', 'a, b'
        file 'gen_col_data.json'
        set 'jsonpaths', '["$.a", "$.b"]'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_test_stream_load_common_json "select * from test_gen_col_common_steam_mysql_load order by 1,2,3"

    streamLoad {
        table 'gencol_refer_gencol_steam_mysql_load'
        set 'strip_outer_array', 'true'
        set 'format', 'json'
        set 'columns', 'a, b'
        file 'gen_col_data.json'
        set 'jsonpaths', '["$.a", "$.b"]'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_test_stream_load_refer_gencol_json "select * from gencol_refer_gencol_steam_mysql_load order by 1,2,3,4"

    streamLoad {
        set 'version', '1'
        file 'gen_col_data.csv'
        time 10000
        set 'sql',"""
            insert into ${db}.test_gen_col_common_steam_mysql_load(a,b) select * from http_stream("format"="csv", "column_separator"=",")
        """
    }
    sql "sync"
    qt_http_stream_load_common_ "select * from test_gen_col_common_steam_mysql_load order by 1,2,3"
    streamLoad {
        set 'version', '1'
        file 'gen_col_data.csv'
        time 10000
        set 'sql',"""
            insert into ${db}.gencol_refer_gencol_steam_mysql_load(a,b) select * from http_stream("format"="csv", "column_separator"=",")
        """
    }
    sql "sync"
    qt_http_stream_load_refer_gencol "select * from gencol_refer_gencol_steam_mysql_load order by 1,2,3,4"
    // mysql load
    def filepath = getLoalFilePath "gen_col_data.csv"
    sql """
        LOAD DATA LOCAL
        INFILE '${filepath}'
        INTO TABLE test_gen_col_common_steam_mysql_load
        COLUMNS TERMINATED BY ','
        (a,b); """
    sql """
        LOAD DATA LOCAL
        INFILE '${filepath}'
        INTO TABLE gencol_refer_gencol_steam_mysql_load
        COLUMNS TERMINATED BY ','
        (a,b);
    """
    sql "sync"
    qt_test_mysql_load_common "select * from test_gen_col_common_steam_mysql_load order by 1,2,3"
    qt_test_mysql_load_refer_gencol "select * from gencol_refer_gencol_steam_mysql_load order by 1,2,3,4"

    // specify value for generated column
    sql "truncate table test_gen_col_common_steam_mysql_load"
    filepath = getLoalFilePath "three_column_gen_col_data.csv"
    sql """
        LOAD DATA LOCAL
        INFILE '${filepath}'
        INTO TABLE test_gen_col_common_steam_mysql_load
        COLUMNS TERMINATED BY ','
        (a,b,c);
    """
    sql "sync"
    qt_specify_value_for_gencol "select * from test_gen_col_common_steam_mysql_load order by 1,2,3"

    sql "truncate table test_gen_col_common_steam_mysql_load"
    sql """
        LOAD DATA LOCAL
        INFILE '${filepath}'
        INTO TABLE test_gen_col_common_steam_mysql_load
        COLUMNS TERMINATED BY ','
        (a,b,tmp)
        set (c=tmp+1) ;
    """
    sql "sync"
    qt_specify_value_for_gencol "select * from test_gen_col_common_steam_mysql_load order by 1,2,3"
}