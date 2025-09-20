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

suite("test_all_escape_sequences") {
    sql "DROP TABLE IF EXISTS `escape_sequences_test`"
    sql """
        create table IF NOT EXISTS `escape_sequences_test` (
                `id` int NULL,
                `test_name` text NULL,
                `arr` array<text> NULL,
                `map_col` map<string, text> NULL,
                `struct_col` struct<name:text, des:text> NULL
        ) ENGINE=OLAP
          DUPLICATE KEY(`id`)  distributed by hash(`id`) buckets 1 properties("replication_num" = "1");
    """

    // 测试用例1: 双引号转义 \" - Insert Values测试
    sql """
        INSERT INTO escape_sequences_test VALUES 
        (1, 'double_quote_test', '["normal","with\\"quotes\\"","end\\"quote"]', 
         '{"key1":"value1","key2":"with\\"quotes\\"","key3":"end\\"quote"}', 
         '{"name":"test","des":"with\\"quotes\\"and\\"more\\"quotes"}'),
        (2, 'double_quote_map', NULL, 
         '{"key1":"value1","key2":"with\\"quotes\\"","key3":"end\\"quote"}', 
         NULL),
        (3, 'double_quote_struct', NULL, NULL, 
         '{"name":"test","des":"with\\"quotes\\"and\\"more\\"quotes"}')
    """
    
    qt_select_double_quote """
        SELECT id, test_name, arr, map_col, struct_col FROM escape_sequences_test WHERE id <= 3 ORDER BY id
    """
    qt_select_double_quote_array_size """
        SELECT id, test_name, array_size(arr) FROM escape_sequences_test WHERE id <= 3 ORDER BY id
    """
    qt_select_double_quote_map_size """
        SELECT id, test_name, map_size(map_col) FROM escape_sequences_test WHERE id <= 3 ORDER BY id
    """

    // 测试用例2: 反斜杠转义 \\ - Insert Values测试
    sql """
        INSERT INTO escape_sequences_test VALUES 
        (4, 'backslash_test', '["normal","with\\\\backslash","end\\\\backslash"]', 
         '{"key1":"value1","key2":"with\\\\backslash","key3":"end\\\\backslash"}', 
         '{"name":"test","des":"with\\\\backslash\\\\and\\\\more\\\\backslashes"}'),
        (5, 'backslash_map', NULL, 
         '{"key1":"value1","key2":"with\\\\backslash","key3":"end\\\\backslash"}', 
         NULL),
        (6, 'backslash_struct', NULL, NULL, 
         '{"name":"test","des":"with\\\\backslash\\\\and\\\\more\\\\backslashes"}')
    """
    
    qt_select_backslash """
        SELECT id, test_name, arr, map_col, struct_col FROM escape_sequences_test WHERE id BETWEEN 4 AND 6 ORDER BY id
    """
    qt_select_backslash_array_size """
        SELECT id, test_name, array_size(arr) FROM escape_sequences_test WHERE id BETWEEN 4 AND 6 ORDER BY id
    """
    qt_select_backslash_map_size """
        SELECT id, test_name, map_size(map_col) FROM escape_sequences_test WHERE id BETWEEN 4 AND 6 ORDER BY id
    """
    // 测试用例3: 换行符转义 \n - Insert Values测试
    sql """
        INSERT INTO escape_sequences_test VALUES 
        (7, 'newline_test', '["normal","with\\nnewline","end\\nnewline"]', 
         '{"key1":"value1","key2":"with\\nnewline","key3":"end\\nnewline"}', 
         '{"name":"test","des":"with\\nnewline\\nand\\nmore\\nnewlines"}'),
        (8, 'newline_map', NULL, 
         '{"key1":"value1","key2":"with\\nnewline","key3":"end\\nnewline"}', 
         NULL),
        (9, 'newline_struct', NULL, NULL, 
         '{"name":"test","des":"with\\nnewline\\nand\\nmore\\nnewlines"}')
    """
    
    qt_select_newline """
        SELECT id, test_name, arr, map_col, struct_col FROM escape_sequences_test WHERE id BETWEEN 7 AND 9 ORDER BY id
    """
    qt_select_newline_array_size """
        SELECT id, test_name, array_size(arr) FROM escape_sequences_test WHERE id BETWEEN 7 AND 9 ORDER BY id
    """
    qt_select_newline_map_size """
        SELECT id, test_name, map_size(map_col) FROM escape_sequences_test WHERE id BETWEEN 7 AND 9 ORDER BY id
    """
    // 测试用例4: 制表符转义 \t - Insert Values测试
    sql """
        INSERT INTO escape_sequences_test VALUES 
        (10, 'tab_test', '["normal","with\\ttab","end\\ttab"]', 
         '{"key1":"value1","key2":"with\\ttab","key3":"end\\ttab"}', 
         '{"name":"test","des":"with\\ttab\\tand\\tmore\\ttabs"}'),
        (11, 'tab_map', NULL, 
         '{"key1":"value1","key2":"with\\ttab","key3":"end\\ttab"}', 
         NULL),
        (12, 'tab_struct', NULL, NULL, 
         '{"name":"test","des":"with\\ttab\\tand\\tmore\\ttabs"}')
    """
    
    qt_select_tab """
        SELECT id, test_name, arr, map_col, struct_col FROM escape_sequences_test WHERE id BETWEEN 10 AND 12 ORDER BY id
    """
    qt_select_tab_array_size """
        SELECT id, test_name, array_size(arr) FROM escape_sequences_test WHERE id BETWEEN 10 AND 12 ORDER BY id
    """
    qt_select_tab_map_size """
        SELECT id, test_name, map_size(map_col) FROM escape_sequences_test WHERE id BETWEEN 10 AND 12 ORDER BY id
    """
    // 测试用例5: 回车符转义 \r - Insert Values测试
    sql """
        INSERT INTO escape_sequences_test VALUES 
        (13, 'carriage_return_test', '["normal","with\\rcarriage","end\\rcarriage"]', 
         '{"key1":"value1","key2":"with\\rcarriage","key3":"end\\rcarriage"}', 
         '{"name":"test","des":"with\\rcarriage\\rand\\rmore\\rcarriages"}'),
        (14, 'carriage_return_map', NULL, 
         '{"key1":"value1","key2":"with\\rcarriage","key3":"end\\rcarriage"}', 
         NULL),
        (15, 'carriage_return_struct', NULL, NULL, 
         '{"name":"test","des":"with\\rcarriage\\rand\\rmore\\rcarriages"}')
    """
    
    qt_select_carriage_return """
        SELECT id, test_name, arr, map_col, struct_col FROM escape_sequences_test WHERE id BETWEEN 13 AND 15 ORDER BY id
    """
    qt_select_carriage_return_array_size """
        SELECT id, test_name, array_size(arr) FROM escape_sequences_test WHERE id BETWEEN 13 AND 15 ORDER BY id
    """
    qt_select_carriage_return_map_size """
        SELECT id, test_name, map_size(map_col) FROM escape_sequences_test WHERE id BETWEEN 13 AND 15 ORDER BY id
    """
    // 测试用例6: 退格符转义 \b - Insert Values测试
    sql """
        INSERT INTO escape_sequences_test VALUES 
        (16, 'backspace_test', '["normal","with\\bbackspace","end\\bbackspace"]', 
         '{"key1":"value1","key2":"with\\bbackspace","key3":"end\\bbackspace"}', 
         '{"name":"test","des":"with\\bbackspace\\band\\bmore\\bbackspaces"}'),
        (17, 'backspace_map', NULL, 
         '{"key1":"value1","key2":"with\\bbackspace","key3":"end\\bbackspace"}', 
         NULL),
        (18, 'backspace_struct', NULL, NULL, 
         '{"name":"test","des":"with\\bbackspace\\band\\bmore\\bbackspaces"}')
    """
    
    qt_select_backspace """
        SELECT id, test_name, arr, map_col, struct_col FROM escape_sequences_test WHERE id BETWEEN 16 AND 18 ORDER BY id
    """
    qt_select_backspace_array_size """
        SELECT id, test_name, array_size(arr) FROM escape_sequences_test WHERE id BETWEEN 16 AND 18 ORDER BY id
    """
    qt_select_backspace_map_size """
        SELECT id, test_name, map_size(map_col) FROM escape_sequences_test WHERE id BETWEEN 16 AND 18 ORDER BY id
    """
    // 测试用例7: 换页符转义 \f - Insert Values测试
    sql """
        INSERT INTO escape_sequences_test VALUES 
        (19, 'form_feed_test', '["normal","with\\fform","end\\fform"]', 
         '{"key1":"value1","key2":"with\\fform","key3":"end\\fform"}', 
         '{"name":"test","des":"with\\fform\\fand\\fmore\\fforms"}'),
        (20, 'form_feed_map', NULL, 
         '{"key1":"value1","key2":"with\\fform","key3":"end\\fform"}', 
         NULL),
        (21, 'form_feed_struct', NULL, NULL, 
         '{"name":"test","des":"with\\fform\\fand\\fmore\\fforms"}')
    """
    
    qt_select_form_feed """
        SELECT id, test_name, arr, map_col, struct_col FROM escape_sequences_test WHERE id BETWEEN 19 AND 21 ORDER BY id
    """
    qt_select_form_feed_array_size """
        SELECT id, test_name, array_size(arr) FROM escape_sequences_test WHERE id BETWEEN 19 AND 21 ORDER BY id
    """
    qt_select_form_feed_map_size """
        SELECT id, test_name, map_size(map_col) FROM escape_sequences_test WHERE id BETWEEN 19 AND 21 ORDER BY id
    """
    // 测试用例8: 正斜杠转义 \/ - Insert Values测试
    sql """
        INSERT INTO escape_sequences_test VALUES 
        (22, 'forward_slash_test', '["normal","with\\/slash","end\\/slash"]', 
         '{"key1":"value1","key2":"with\\/slash","key3":"end\\/slash"}', 
         '{"name":"test","des":"with\\/slash\\/and\\/more\\/slashes"}'),
        (23, 'forward_slash_map', NULL, 
         '{"key1":"value1","key2":"with\\/slash","key3":"end\\/slash"}', 
         NULL),
        (24, 'forward_slash_struct', NULL, NULL, 
         '{"name":"test","des":"with\\/slash\\/and\\/more\\/slashes"}')
    """
    
    qt_select_forward_slash """
        SELECT id, test_name, arr, map_col, struct_col FROM escape_sequences_test WHERE id BETWEEN 22 AND 24 ORDER BY id
    """
    qt_select_forward_slash_array_size """
        SELECT id, test_name, array_size(arr) FROM escape_sequences_test WHERE id BETWEEN 22 AND 24 ORDER BY id
    """
    qt_select_forward_slash_map_size """
        SELECT id, test_name, map_size(map_col) FROM escape_sequences_test WHERE id BETWEEN 22 AND 24 ORDER BY id
    """
    // 测试用例9: 混合所有转义字符 - Insert Values测试
    sql """
        INSERT INTO escape_sequences_test VALUES 
        (25, 'mixed_all_test', '["all\\"escape\\"chars\\\\here\\nwith\\tnewlines\\rand\\btabs\\fand\\/slashes"]', 
         '{"all":"\\"escape\\"chars\\\\here\\nwith\\tnewlines\\rand\\btabs\\fand\\/slashes"}', 
         '{"name":"mixed","des":"all\\"escape\\"chars\\\\here\\nwith\\tnewlines\\rand\\btabs\\fand\\/slashes"}'),
        (26, 'mixed_all_map', NULL, 
         '{"all":"\\"escape\\"chars\\\\here\\nwith\\tnewlines\\rand\\btabs\\fand\\/slashes"}', 
         NULL),
        (27, 'mixed_all_struct', NULL, NULL, 
         '{"name":"mixed","des":"all\\"escape\\"chars\\\\here\\nwith\\tnewlines\\rand\\btabs\\fand\\/slashes"}')
    """
    
    qt_select_mixed_all """
        SELECT id, test_name, arr, map_col, struct_col FROM escape_sequences_test WHERE id BETWEEN 25 AND 27 ORDER BY id
    """
    qt_select_mixed_all_array_size """
        SELECT id, test_name, array_size(arr) FROM escape_sequences_test WHERE id BETWEEN 25 AND 27 ORDER BY id
    """
    qt_select_mixed_all_map_size """
        SELECT id, test_name, map_size(map_col) FROM escape_sequences_test WHERE id BETWEEN 25 AND 27 ORDER BY id
    """
    // 测试用例10: 边界情况和错误处理 - Insert Values测试
    sql """
        INSERT INTO escape_sequences_test VALUES 
        (28, 'empty_test', '["","\\"","\\\\","\\n","\\t","\\r","\\b","\\f","\\/"]', 
         '{"empty":"","quote":"\\"","backslash":"\\\\","newline":"\\n","tab":"\\t","carriage":"\\r","backspace":"\\b","form":"\\f","slash":"\\/"}', 
         '{"name":"empty","des":""}'),
        (29, 'empty_map', NULL, 
         '{"empty":"","quote":"\\"","backslash":"\\\\","newline":"\\n","tab":"\\t","carriage":"\\r","backspace":"\\b","form":"\\f","slash":"\\/"}', 
         NULL),
        (30, 'empty_struct', NULL, NULL, 
         '{"name":"empty","des":""}')
    """
    
    qt_select_edge_cases """
        SELECT id, test_name, arr, map_col, struct_col FROM escape_sequences_test WHERE id >= 28 ORDER BY id
    """
    qt_select_edge_cases_array_size """
        SELECT id, test_name, array_size(arr) FROM escape_sequences_test WHERE id >= 28 ORDER BY id
    """ 
    qt_select_edge_cases_map_size """
        SELECT id, test_name, map_size(map_col) FROM escape_sequences_test WHERE id >= 28 ORDER BY id
    """
    // 测试用例11: 统一Stream Load测试 - 使用预定义的JSON文件
    // 清空表数据
    sql "TRUNCATE TABLE escape_sequences_test"
    
    // 使用预定义的JSON文件进行Stream Load
    streamLoad {
        table "escape_sequences_test"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'strip_outer_array', 'false'
        file "escape_json_data.json"
        time 10000
    }
    
    // 验证所有数据
    qt_select_count """
        SELECT COUNT(*) as count FROM escape_sequences_test
    """
    
    // 验证所有数据
    qt_select_all_data """
        SELECT id, test_name, arr, map_col, struct_col FROM escape_sequences_test ORDER BY id
    """
    qt_select_all_data_array_size """
        SELECT id, test_name, array_size(arr) FROM escape_sequences_test ORDER BY id
    """
    qt_select_all_data_map_size """
        SELECT id, test_name, map_size(map_col) FROM escape_sequences_test ORDER BY id
    """
    // 清理测试数据
}