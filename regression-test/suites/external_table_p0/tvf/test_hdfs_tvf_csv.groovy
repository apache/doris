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

suite("test_hdfs_tvf_csv","external,hive,tvf,external_docker") {
    String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    // It's okay to use random `hdfsUser`, but can not be empty.
    def hdfsUserName = "doris"
    def format = "csv"
    def defaultFS = "hdfs://${externalEnvIp}:${hdfs_port}"
    def uri = ""

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            sql """ set trim_tailing_spaces_for_external_table_query = true; """
            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/decimal_separators/decimal_separators.csv"
            order_qt_decimal_separators """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ";",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/decimal_separators/decimal_separators_csv.csv"
            order_qt_decimal_separators_csv """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "trim_double_quotes" = "true",
                        "enclose" = '"',
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/decimal_separators/invalid_char.csv"
            order_qt_invalid_char """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/decimal_separators/mixed_format_fail.csv"
            order_qt_mixed_format_fail """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/headers/empty_1.csv"
            order_qt_headers_empty_1 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/headers/empty_2.csv"
            order_qt_headers_empty_2 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/headers/empty_3.csv"
            order_qt_headers_empty_3 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/headers/empty_4.csv"
            order_qt_headers_empty_4 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/headers/escaped_quote.csv"
            order_qt_headers_escaped_quote """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "enclose" = "'",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/headers/integer.csv"
            order_qt_headers_integer """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/headers/single_line.csv"
            order_qt_headers_single_line """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/headers/undetected_type.csv"
            order_qt_headers_undetected_type """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/headers/unescaped_quote.csv"
            order_qt_headers_unescaped_quote """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "enclose" = "'",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/headers/all_varchar.csv"
            order_qt_headers_all_varchar """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/headers/borked_type.csv"
            order_qt_headers_borked_type """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/multidelimiter/aa_delim.csv"
            order_qt_multidelim_aa_delim """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = "aa",
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/multidelimiter/aaa_delim.csv"
            order_qt_multidelim_aaa_delim """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = "aaa",
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/multidelimiter/aaaa_delim.csv"
            order_qt_multidelim_aaaa_delim """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = "aaaa",
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/multidelimiter/aa_delim_quoted.csv"
            order_qt_multidelim_aa_delim_quoted """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = "aa",
                        "trim_double_quotes" = "true",
                        "enclose" = '"',
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/multidelimiter/abac_incomplete_quote.csv"
            order_qt_multidelim_abac_incomplete_quote """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = "AB",
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/multidelimiter/abac_mix.csv"
            order_qt_multidelim_abac_mix """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = "AB",
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/multidelimiter/abac_newline_in_quote.csv"
            order_qt_multidelim_abac_newline_in_quote """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = "AB",
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/multidelimiter/carriage_feed_newline.csv"
            order_qt_multidelim_carriage_feed_newline """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = "AB",
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/multidelimiter/complex_unterminated_quote.csv"
            order_qt_multidelim_complex_unterminated_quote """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = "AC",
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/multidelimiter/escape_non_quote_escape.csv"
            order_qt_multidelim_escape_non_quote_escape """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = "|",
                        "enclose" ='"',
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/multidelimiter/escape_non_quote_escape_complex.csv"
            order_qt_multidelim_escape_non_quote_escape_complex """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = "B",
                        "enclose" ='"',
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/multidelimiter/file_ends_in_quoted_value.csv"
            order_qt_multidelim_file_ends_in_quoted_value """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "enclose" ='"',
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/multidelimiter/incomplete_multibyte_delimiter.csv"
            order_qt_multidelim_incomplete_multibyte_delimiter """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = "AB",
                        "enclose" ='"',
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/multidelimiter/many_bytes.csv"
            order_qt_multidelim_many_bytes """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = "thisisaverysuberverylargestring",
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/multidelimiter/simple_unterminated_quote.csv"
            order_qt_multidelim_simple_unterminated_quote """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "enclose" ='"',
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/multidelimiter/trailing_delimiter.csv"
            order_qt_multidelim_trailing_delimiter """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = "|",
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/multidelimiter/trailing_delimiter_complex.csv"
            order_qt_multidelim_trailing_delimiter_complex """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = "BA",
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/multidelimiter/unquote_without_delimiter.csv"
            order_qt_multidelim_unquote_without_delimiter """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = "BB",
                        "enclose" ='"',
                        "trim_double_quotes" = "true",
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/multidelimiter/unterminated_escape.csv"
            order_qt_multidelim_unterminated_escape """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "enclose" ='"',
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/multidelimiter/unterminated_escape_complex.csv"
            order_qt_multidelim_unterminated_escape_complex """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = "AB",
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/multidelimiter/unterminated_quote_escape.csv"
            order_qt_multidelim_unterminated_quote_escape """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = "C",
                        "trim_double_quotes" = "true",
                        "enclose" ='A',
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/multidelimiter/unterminated_quote_escape_complex.csv"
            order_qt_multidelim_unterminated_quote_escape_complex """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = "C",
                        "trim_double_quotes" = "true",
                        "enclose" ='A',
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/multidelimiter/unterminated_quote_multi_line.csv"
            order_qt_multidelim_unterminated_quote_multi_line """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = "AAA",
                        "enclose" ='"',
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/multidelimiter/unterminated_quote_with_escape.csv"
            order_qt_multidelim_unterminated_quote_with_escape """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = "|",
                        "enclose" ='"',
                        "trim_double_quotes" = "true",
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/multidelimiter/unterminated_quote_with_escape_complex.csv"
            order_qt_multidelim_unterminated_quote_with_escape_complex """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = "C",
                        "enclose" ='A',
                        "trim_double_quotes" = "true",
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/unescaped_quotes/end_quote.csv"
            order_qt_unescaped_quotes_end_quote """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "enclose" = '"',
                        "trim_double_quotes" = "true",
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/unescaped_quotes/end_quote_2.csv"
            order_qt_unescaped_quotes_end_quote_2 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "enclose" = '"',
                        "trim_double_quotes" = "true",
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/unescaped_quotes/end_quote_3.csv"
            order_qt_unescaped_quotes_end_quote_3 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "enclose" = '"',
                        "trim_double_quotes" = "true",
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/unescaped_quotes/end_quote_mixed.csv"
            order_qt_unescaped_quotes_end_quote_mixed """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "enclose" = '"',
                        "trim_double_quotes" = "true",
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/unescaped_quotes/some_escaped_some_not.csv"
            order_qt_unescaped_quotes_some_escaped_some_not """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "enclose" = '"',
                        "trim_double_quotes" = "true",
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/unescaped_quotes/unescaped_quote.csv"
            order_qt_unescaped_quotes_unescaped_quote """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ";",
                        "enclose" = '"',
                        "trim_double_quotes" = "true",
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/unescaped_quotes/unescaped_quote_new_line.csv"
            order_qt_unescaped_quotes_unescaped_quote_new_line """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ";",
                        "enclose" = '"',
                        "trim_double_quotes" = "true",
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/unescaped_quotes/unescaped_quote_new_line_rn.csv"
            order_qt_unescaped_quotes_unescaped_quote_new_line_rn """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ";",
                        "enclose" = '"',
                        "trim_double_quotes" = "true",
                        "line_delimiter" = "\r\n",
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/single_quote.csv"
            order_qt_csvtvf_single_quote """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "enclose" = "'",
                        "format" = "csv_with_names"); """

            // TODO: support complex type for csv_schema
            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/struct.csv"
            test {
                sql """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "enclose" = '"',
                        "trim_double_quotes" = "true",
                        "csv_schema" = "struct_col:struct<varchar(10), int>",
                        "format" = "csv_with_names"); """
                exception "unsupported column type: struct<varchar(10),int>"
            }

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/hits_problematic.csv"
            order_qt_csvtvf_hits_problematic """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "enclose" = '"',
                        "trim_double_quotes" = "true",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/error.csv"
            order_qt_csvtvf_error """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/escape.csv"
            order_qt_csvtvf_escape """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = "]",
                        "enclose" = '"',
                        "trim_double_quotes" = "true",
                        "format" = "csv"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/extra_delimiters.csv"
            order_qt_csvtvf_extra_delimiters """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/header_left_space.csv"
            order_qt_csvtvf_header_left_space """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/header_normalize.csv"
            order_qt_csvtvf_header_normalize """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ";",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/header_only.csv"
            order_qt_csvtvf_header_only """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/empty_first_line.csv"
            test {
                sql """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = " ",
                        "skip_lines" = "1",
                        "format" = "csv"); """
                exception "The first line is empty, can not parse column numbers"
            }

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/empty_space_start_value.csv"
            order_qt_csvtvf_empty_space_start_value """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "enclose" = '"',
                        "trim_double_quotes" = "true",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/dirty_line.csv"
            order_qt_csvtvf_dirty_line """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/double_quoted_header.csv"
            order_qt_csvtvf_double_quoted_header """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "enclose" = '"',
                        "trim_double_quotes" = "true",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/csv_quoted_newline_odd.csv"
            order_qt_csvtvf_csv_quoted_newline_odd """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "enclose" = '"',
                        "trim_double_quotes" = "true",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/decimal.csv"
            order_qt_csvtvf_decimal """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "csv_schema" = "col_a:decimal(17, 16)",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/bad_escape.csv"
            order_qt_csvtvf_bad_escape """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "enclose" = '"',
                        "trim_double_quotes" = "true",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/big_escape.csv"
            order_qt_csvtvf_big_escape """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ";",
                        "enclose" = '"',
                        "trim_double_quotes" = "true",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/blank_line.csv"
            order_qt_csvtvf_blank_line """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/bool.csv"
            order_qt_csvtvf_bool """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "csv_schema" = "col_a:boolean",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/all_quotes.csv"
            order_qt_csvtvf_all_quotes """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "enclose" = '"',
                        "trim_double_quotes" = "true",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/14512.csv"
            order_qt_csvtvf_14512 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "enclose" = '"',
                        "trim_double_quotes" = "true",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/14512_og.csv"
            order_qt_csvtvf_14512_og """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "enclose" = '"',
                        "trim_double_quotes" = "true",
                        "format" = "csv_with_names"); """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_tvf_data/16857.csv"
            order_qt_csvtvf_16857 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "enclose" = '"',
                        "trim_double_quotes" = "true",
                        "format" = "csv_with_names"); """
            
        } catch (Exception e) {
            println(e)
        } finally {
            sql """ set trim_tailing_spaces_for_external_table_query = false; """
        }
    }
}
