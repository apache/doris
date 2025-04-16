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

suite("test_str_to_map") {
    sql "drop table if exists str_to_map_args;"
    sql """
        create table str_to_map_args (
            k0 int,
            map_str_not_null string not null,
            map_str_null string null,
            key_delim_not_null string not null,
            key_delim_null string null,
            value_delim_not_null string not null,
            value_delim_null string null
        )
        DISTRIBUTED BY HASH(k0)
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """

    // Test empty table with different nullable combinations
    order_qt_all_not_null "select str_to_map(map_str_not_null, key_delim_not_null, value_delim_not_null) from str_to_map_args"
    
    order_qt_all_args_null "select str_to_map(map_str_null, key_delim_null, value_delim_null) from str_to_map_args"
    
    order_qt_partial_null "select str_to_map(map_str_not_null, key_delim_null, value_delim_null) from str_to_map_args"
    
    order_qt_nullable_no_null "select str_to_map(nullable(map_str_not_null), nullable(key_delim_not_null), nullable(value_delim_not_null)) from str_to_map_args"

    sql '''
    insert into str_to_map_args values
        (1, 'a:1,b:2,c:3', 'a:1,b:2,c:3', ',', ',', ':', ':'),
        (2, '', '', ',', ',', ':', ':'), -- Empty string test
        (3, 'a:1', 'a:1', ',', ',', ':', ':'), -- Single key-value pair
        (4, 'a:1,b:2,b:3', 'a:1,b:2,b:3', ',', ',', ':', ':'), -- Duplicate keys
        (5, 'a:,b:,c:', 'a:,b:,c:', ',', ',', ':', ':'), -- Empty values
        (6, ':1,:2,:3', ':1,:2,:3', ',', ',', ':', ':'), -- Empty keys
        (7, 'a=1;b=2;c=3', 'a=1;b=2;c=3', ';', ';', '=', '='), -- Different delimiters
        (8, '中文:值,英文:value', '中文:值,英文:value', ',', ',', ':', ':'), -- Unicode characters
        (9, 'special@#:123,chars!:456', 'special@#:123,chars!:456', ',', ',', ':', ':'), -- Special characters in keys
        (10, 'a:123!@#,b:456$%^', 'a:123!@#,b:456$%^', ',', ',', ':', ':'), -- Special characters in values
        (11, 'verylongkey:verylongvalue,anotherlongkey:anotherlongvalue', 'verylongkey:verylongvalue,anotherlongkey:anotherlongvalue', ',', ',', ':', ':'), -- Long strings
        (12, 'a::1,b::2', 'a::1,b::2', ',', ',', '::', '::'), -- Multi-character delimiter
        (13, 'a:1\nb:2\nc:3', 'a:1\nb:2\nc:3', '\n', '\n', ':', ':'), -- Newline as delimiter
        (14, 'a:1\tb:2\tc:3', 'a:1\tb:2\tc:3', '\t', '\t', ':', ':'), -- Tab as delimiter
        (15, ' a : 1 , b : 2 ', ' a : 1 , b : 2 ', ',', ',', ':', ':') -- Spaces in string
    '''

    // Test different nullable combinations with data
    order_qt_all_not_null_data """
        select str_to_map(map_str_not_null, key_delim_not_null, value_delim_not_null) 
        from str_to_map_args 
        order by k0;
    """

    order_qt_all_args_null_data """
        select str_to_map(map_str_null, key_delim_null, value_delim_null)
        from str_to_map_args
        order by k0;
    """

    order_qt_partial_null_data """
        select str_to_map(map_str_not_null, key_delim_null, value_delim_null)
        from str_to_map_args
        order by k0;
    """

    order_qt_nullable_no_null_data """
        select str_to_map(nullable(map_str_not_null), nullable(key_delim_not_null), nullable(value_delim_not_null))
        from str_to_map_args
        order by k0;
    """

    // Test mixed nullable combinations
    order_qt_mixed_null_1 """
        select str_to_map(map_str_null, key_delim_not_null, value_delim_not_null)
        from str_to_map_args
        order by k0;
    """

    order_qt_mixed_null_2 """
        select str_to_map(map_str_not_null, key_delim_null, value_delim_not_null)
        from str_to_map_args
        order by k0;
    """

    order_qt_mixed_null_3 """
        select str_to_map(map_str_not_null, key_delim_not_null, value_delim_null)
        from str_to_map_args
        order by k0;
    """

    // Test with constant null values
    order_qt_const_null_1 """
        select str_to_map(null, key_delim_not_null, value_delim_not_null)
        from str_to_map_args
        order by k0;
    """

    order_qt_const_null_2 """
        select str_to_map(map_str_not_null, null, value_delim_not_null)
        from str_to_map_args
        order by k0;
    """

    order_qt_const_null_3 """
        select str_to_map(map_str_not_null, key_delim_not_null, null)
        from str_to_map_args
        order by k0;
    """

    /// consts. most by BE-UT
    // Test const string with column delimiters
    order_qt_const_str """
        select str_to_map('a:1,b:2', key_delim_not_null, value_delim_not_null) 
        from str_to_map_args order by k0
    """
    
    // Test column string with const delimiters  
    order_qt_const_delims """
        select str_to_map(map_str_not_null, ',', ':') 
        from str_to_map_args order by k0
    """
        
    // Test const string with one const delimiter and one column delimiter
    order_qt_mixed_const1 """
        select str_to_map('x=1;y=2', ';', value_delim_not_null) 
        from str_to_map_args order by k0
    """
    
    order_qt_mixed_const2 """
        select str_to_map('p-1|q-2', key_delim_not_null, '-') 
        from str_to_map_args order by k0
    """
    
    // Test all const non-null arguments
    order_qt_all_const """
        select str_to_map('a=1|b=2', '|', '=') 
        from str_to_map_args order by k0
    """
    
    // Test const string with nullable column delimiters
    order_qt_const_str_null_delims """
        select str_to_map('m:1,n:2', key_delim_null, value_delim_null) 
        from str_to_map_args order by k0
    """
    
    // Test nullable column string with const delimiters
    order_qt_null_str_const_delims '''
        select str_to_map(map_str_null, '#', '$') 
        from str_to_map_args order by k0
    '''

    // Test basic str_to_map functionality with all parameters
    qt_basic_1 "select str_to_map('a:1,b:2,c:3', ',', ':');"
    qt_basic_2 "select str_to_map('key1=val1;key2=val2', ';', '=');"
    qt_basic_3 "select str_to_map('x-1|y-2|z-3', '|', '-');"

    // Test with default parameters (omitting both delimiters)
    // Default pair delimiter is ',' and key-value delimiter is ':'
    qt_default_both_1 "select str_to_map('a:1,b:2,c:3');"
    qt_default_both_2 "select str_to_map('key1:value1,key2:value2');"
    qt_default_both_3 "select str_to_map('x:1,y:2,z:');"
    qt_default_both_4 "select str_to_map('');"

    // Test with default key-value delimiter (omitting last parameter)
    // Default key-value delimiter is ':'
    qt_default_value_1 "select str_to_map('a:1;b:2;c:3', ';');"
    qt_default_value_2 "select str_to_map('key:val|foo:bar', '|');"
    qt_default_value_3 "select str_to_map('x:1#y:2#z:3', '#');"
    qt_default_value_4 "select str_to_map('a:1...b:2...c:3', '...');"

    // Test empty string cases
    qt_empty_1 "select str_to_map('');"
    qt_empty_2 "select str_to_map('a:1,,b:2');" 
    qt_empty_3 "select str_to_map('a:,b:2,c:');"
    qt_empty_4 "select str_to_map(',,,');"

    // Test missing key-value delimiter
    qt_missing_value_1 "select str_to_map('a,b:2,c');"
    qt_missing_value_2 "select str_to_map('val1,val2,val3');"
    qt_missing_value_3 "select str_to_map('key1,key2:val2,key3');"

    // Test with special characters
    qt_special_1 "select str_to_map('\ta:1\n,\tb:2\n');"
    qt_special_2 "select str_to_map('a\\nb:1,c\\td:2');"
    qt_special_3 "select str_to_map('key1:value1,key2:value2', ',', ':');"

    // Test with spaces
    qt_spaces_1 "select str_to_map('a : 1, b : 2');"
    qt_spaces_2 "select str_to_map(' a:1 , b:2 ');"
    qt_spaces_3 "select str_to_map('   a:1,   b:2   ');"
    qt_spaces_4 "select str_to_map(' ');"

    // Test with Unicode characters
    qt_unicode_1 "select str_to_map('键1:值1,键2:值2');"
    qt_unicode_2 "select str_to_map('标题①:内容①,标题②:内容②');"
    qt_unicode_3 "select str_to_map('🔑:🔒,📝:📖');"
    qt_unicode_4 "select str_to_map('あ:い,う:え');"

    // Test with duplicate keys
    qt_dup_1 "select str_to_map('a:1,b:2,a:3');"
    qt_dup_2 "select str_to_map('key:val1,key:val2,key:val3');"
    qt_dup_3 "select str_to_map('a:1,a:,a:3');"

    // Test edge cases
    qt_edge_1 "select str_to_map('a:1:2,b:3:4');"
    qt_edge_2 "select str_to_map(':::');"
    qt_edge_3 "select str_to_map('a:1:2');"
    qt_edge_4 "select str_to_map('key::value');"
    qt_edge_5 "select str_to_map(':');"

    // Test extremely long strings
    qt_long_1 "select str_to_map(repeat('a:1,', 1000));"
    qt_long_2 "select str_to_map(concat(repeat('key', 100), ':', repeat('value', 100)));"
}
