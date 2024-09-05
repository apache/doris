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

suite("test_variant_query", "nonConcurrent"){

    // Test case for match query with inverted index
    // case1. simple string case in variant
    def checkpoints_name = "match.invert_index_not_support_execute_match"
    try {
        GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name)
        qt_sql_string1 """ select    *    from    test_variant_inverted_index_null  where  col_variant['cell_type']  match 'markdown'  order  by  col_int_undef_signed_not_null""";
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name)
        qt_sql_string2 """ select    *    from    test_variant_inverted_index_null  where  col_variant_parser_eng['cell_type']  match 'markdown'  order  by  col_int_undef_signed_not_null""";
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name)
        qt_sql_string3 """ select    *    from    test_variant_inverted_index_null  where  col_variant_parser_uni['cell_type']  match 'markdown'  order  by  col_int_undef_signed_not_null""";
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
    }

    // case2. simple int case in variant
    try {
        GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name)
        qt_sql_int1 """ select    *    from    test_variant_inverted_index_null  where  col_variant['execution_count']  in (7,8)  order  by  col_int_undef_signed_not_null""";
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name)
        qt_sql_int2 """ select    *    from    test_variant_inverted_index_null  where  col_variant_parser_eng['execution_count']  in (7,8) order  by  col_int_undef_signed_not_null""";
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name)
        qt_sql_int3 """ select    *    from    test_variant_inverted_index_null  where  col_variant_parser_uni['execution_count']  in (7,8) order  by  col_int_undef_signed_not_null""";
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
    }

    // case2. array<string> case in variant
    try {
        GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name)
        qt_sql_as1 """ select    *    from    test_variant_inverted_index_null  where  array_contains(cast(col_variant['source'] as array<string>), "test_data.head()")  order  by  col_int_undef_signed_not_null""";
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name)
        qt_sql_as2 """ select    *    from    test_variant_inverted_index_null  where  array_contains(cast(col_variant_parser_eng['source'] as array<string>), "test_data.head()") order  by  col_int_undef_signed_not_null""";
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name)
        qt_sql_as3 """ select    *    from    test_variant_inverted_index_null  where  array_contains(cast(col_variant_parser_uni['source'] as array<string>), "test_data.head()") order  by  col_int_undef_signed_not_null""";
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
    }

    // now we not support variant to extract array object element with inverted index to speed up query
    // and inverted index can't support function in query like:
    // mysql> select * from test_variant_inverted_index_null where json_extract(col_variant['outputs'], '$.[1].output_type') match 'execute_result';
    //ERROR 1105 (HY000): errCode = 2, detailMessage = Only support match left operand is SlotRef, right operand is Literal. But meet expression (json_extract(cast(col_variant['outputs']#12 as VARCHAR(65533)), '$.[1].output_type') MATCH_ANY 'execute_result')
//    // case3. array<object> case in variant
//    // case3.1 array<object> for simple string case
//    try {
//        GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name)
//        sql """ select    *    from    test_variant_inverted_index_null  where  col_variant['output'][0]['output_type']  match 'execute_result'  order  by  col_int_undef_signed_not_null""";
//    } finally {
//        GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
//    }
//
//    try {
//        GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name)
//        sql """ select    *    from    test_variant_inverted_index_null  where  col_variant_parser_eng['output'][0]['output_type']  match 'mark'  order  by  col_int_undef_signed_not_null""";
//    } finally {
//        GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
//    }
//
//    try {
//        GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name)
//        sql """ select    *    from    test_variant_inverted_index_null  where  col_variant_parser_uni['output'][0]['output_type']  match 'mark'  order  by  col_int_undef_signed_not_null""";
//    } finally {
//        GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
//    }
//
//    // case3.2 array<object> for simple int case
//    try {
//        GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name)
//        sql """ select    *    from    test_variant_inverted_index_null  where  col_variant['output'][0]['execution_count']  in (7,8)  order  by  col_int_undef_signed_not_null""";
//    } finally {
//        GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
//    }
//
//    try {
//        GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name)
//        sql """ select    *    from    test_variant_inverted_index_null  where  col_variant_parser_eng['output'][0]['execution_count']  in (7,8) order  by  col_int_undef_signed_not_null""";
//    } finally {
//        GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
//    }
//
//    try {
//        GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name)
//        sql """ select    *    from    test_variant_inverted_index_null  where  col_variant_parser_uni['output'][0]['execution_count']  in (7,8) order  by  col_int_undef_signed_not_null""";
//    } finally {
//        GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
//    }
//
//    // case3.3 array<object> for array<string> case
//    try {
//        GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name)
//        sql """ select    *    from    test_variant_inverted_index_null  where  array_contains(cast(col_variant['output'][0]['data']['text/html'] as array<string>), "<td>i loooooooovvvvvveee my kindle not that the dx...</td>\\n")  order  by  col_int_undef_signed_not_null""";
//    } finally {
//        GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
//    }

}
