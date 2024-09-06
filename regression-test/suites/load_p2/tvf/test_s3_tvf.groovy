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

suite("test_s3_tvf", "p2") {
    def s3BucketName = getS3BucketName()
    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()
    def tables = [
            "agg_tbl_basic_tvf",
            "dup_tbl_array_tvf",
            "dup_tbl_basic_tvf",
            "mow_tbl_array_tvf",
            "mow_tbl_basic_tvf",
            "uniq_tbl_array_tvf",
            "uniq_tbl_basic_tvf",
            "nest_tbl_basic_tvf"
    ]

    //deal with agg tables in separate
    def basicTables = [
            "dup_tbl_basic_tvf",
            "mow_tbl_basic_tvf",
            "uniq_tbl_basic_tvf",
    ]

    def arrayTables = [
            "dup_tbl_array_tvf",
            "uniq_tbl_array_tvf",
            "mow_tbl_array_tvf"
    ]

    def uniqTable = [
            "uniq_tbl_basic_tvf",
            "mow_tbl_basic_tvf"
    ]

    def attributeList = [

    ]

    def partitionTables = [
        "uniq_tbl_basic_tvf"
    ]
    // ,
    // path partition key
    // NOTE: maybe path_partition_keys don't support with broker_load, we just calulate the insert_count equal to file rows
    // this parameter is similar with columns from path as (c1, c2, ...) in broker_load method, look at test case test_tvf_based_broker_load.groovy
   for (String table : basicTables) {
    attributeList.add(new TvfAttribute(table, "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, kd16", "K00,K01,K02,K03,K04,K05,K06,K07,K08,K09,K10,K11,K12,K13,K14,K15,K16,K17,K18,Kd16", "", "")
                 .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/kd16=abcdefg/basic_data.csv")
                 .addProperty("format", "csv")
                 .addProperty("column_separator", "|")
                 .addProperty("use_path_style", "true")
                 .addProperty("force_parsing_by_standard_uri", "true")
                 .addProperty("path_partition_keys", "kd16"))
   }
                                                                                                                                                                                                                                                        // SET (k19=to_bitmap(c6), k20=HLL_HASH(c6), k21=TO_QUANTILE_STATE(c5, 1.0), kd19=to_bitmap(c6), kd20=HLL_HASH(c6), kd21=TO_QUANTILE_STATE(c5, 1.0)
   attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21, kd16",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21,kd16", "", "")
            .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/kd16=abcdefg/basic_data.csv")
            .addProperty("format", "csv")
            .addProperty("column_separator", "|")
            .addProperty("use_path_style", "true")
            .addProperty("force_parsing_by_standard_uri", "true")
            .addProperty("path_partition_keys", "kd16"))
    // path partition key maybe don't support array type ? 
    // for(String table : arrayTables) {
    //     attributeList.add(new TvfAttribute(table, ["K00", "K01", "K02", "K03", "K04", "K05", "K06", "K07", "K08", "K09", "K10", "K11", "K12", "K13", "K14", "K15", "K16", "K17", "kd16"], "", "")
    //             .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/kd16=[1,2,3,4]/basic_array_data.csv")
    //             .addProperty("format", "csv")
    //             .addProperty("column_separator", "|")
    //             .addProperty("force_parsing_by_standard_uri", "true")
    //             .addProperty("path_partition_keys", "kd16"))
    // }

    for (String table : basicTables) {
        attributeList.add(new TvfAttribute(table, "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, kd16, kd17", "K00,K01,K02,K03,K04,K05,K06,K07,K08,K09,K10,K11,K12,K13,K14,K15,K16,K17,K18,Kd16, kd17", "", "")
                 .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/kd16=abcdefg/kd17=hello/basic_data.csv")
                 .addProperty("format", "csv")
                 .addProperty("column_separator", "|")
                 .addProperty("use_path_style", "true")
                 .addProperty("force_parsing_by_standard_uri", "true")
                 .addProperty("path_partition_keys", "kd16,kd17"))
   }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21, kd16",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21,kd16", "", "")
            .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/kd16=abcdefg/kd17=hello/basic_data.csv")
            .addProperty("format", "csv")
            .addProperty("column_separator", "|")
            .addProperty("use_path_style", "true")
            .addProperty("force_parsing_by_standard_uri", "true")
            .addProperty("path_partition_keys", "kd16,kd17"))
    
    // for (String table : basicTables) {
    //     attributeList.add(new TvfAttribute(table, "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, kd16, kd17", "K00,K01,K02,K03,K04,K05,K06,K07,K08,K09,K10,K11,K12,K13,K14,K15,K16,K17,K18,Kd16, kd17", "", "")
    //              .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/kd16=abcdefg/*/basic_data.csv")
    //              .addProperty("format", "csv")
    //              .addProperty("column_separator", "|")
    //              .addProperty("use_path_style", "true")
    //              .addProperty("force_parsing_by_standard_uri", "true")
    //              .addProperty("path_partition_keys", "kd16,kd17"))
    // }

    // attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21, kd16",
    //         "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21,kd16", "", "")
    //         .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/kd16=abcdefg/*/basic_data.csv")
    //         .addProperty("format", "csv")
    //         .addProperty("column_separator", "|")
    //         .addProperty("use_path_style", "true")
    //         .addProperty("force_parsing_by_standard_uri", "true")
    //         .addProperty("path_partition_keys", "kd16,kd17"))
    



    // trim_double_quotes
    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, ["K00", "K01", "K02", "K03", "K04", "K05", "K06", "K07", "K08", "K09", "K10", "K11", "K12", "K13", "K14", "K15", "K16", "K17", "K18"], "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data_trim_double_quotes.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("force_parsing_by_standard_uri", "true")
                .addProperty("trim_double_quotes", "true"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data_trim_double_quotes.csv")
            .addProperty("format", "csv")
            .addProperty("column_separator", "|")
            .addProperty("force_parsing_by_standard_uri", "true")
            .addProperty("trim_double_quotes", "true"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, ["K00", "K01", "K02", "K03", "K04", "K05", "K06", "K07", "K08", "K09", "K10", "K11", "K12", "K13", "K14", "K15", "K16", "K17"], "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data_trim_double_quotes.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("force_parsing_by_standard_uri", "true")
                .addProperty("trim_double_quotes", "true"))
    }

    // trim_double_quotes with skip_lines : 1
    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, ["K00", "K01", "K02", "K03", "K04", "K05", "K06", "K07", "K08", "K09", "K10", "K11", "K12", "K13", "K14", "K15", "K16", "K17", "K18"], "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data_trim_double_quotes.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("force_parsing_by_standard_uri", "true")
                .addProperty("skip_lines", "1")
                .addProperty("trim_double_quotes", "true"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data_trim_double_quotes.csv")
            .addProperty("format", "csv")
            .addProperty("column_separator", "|")
            .addProperty("force_parsing_by_standard_uri", "true")
            .addProperty("skip_lines", "1")
            .addProperty("trim_double_quotes", "true"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, ["K00", "K01", "K02", "K03", "K04", "K05", "K06", "K07", "K08", "K09", "K10", "K11", "K12", "K13", "K14", "K15", "K16", "K17"], "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data_trim_double_quotes.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("force_parsing_by_standard_uri", "true")
                .addProperty("skip_lines", "1")
                .addProperty("trim_double_quotes", "true"))
    }

    // stip_outer_array
    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, ["K00", "K01", "K02", "K03", "K04", "K05", "K06", "K07", "K08", "K09", "K10", "K11", "K12", "K13", "K14", "K15", "K16", "K17", "K18"], "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data_trim_double_quotes.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("force_parsing_by_standard_uri", "true")
                .addProperty("skip_lines", "1")
                .addProperty("strip_outer_array", "true")
                .addProperty("trim_double_quotes", "true"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data_trim_double_quotes.csv")
            .addProperty("format", "csv")
            .addProperty("column_separator", "|")
            .addProperty("force_parsing_by_standard_uri", "true")
            .addProperty("skip_lines", "1")
            .addProperty("strip_outer_array", "true")
            .addProperty("trim_double_quotes", "true"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, ["K00", "K01", "K02", "K03", "K04", "K05", "K06", "K07", "K08", "K09", "K10", "K11", "K12", "K13", "K14", "K15", "K16", "K17"], "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data_trim_double_quotes.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("force_parsing_by_standard_uri", "true")
                .addProperty("skip_lines", "1")
                .addProperty("strip_outer_array", "true")
                .addProperty("trim_double_quotes", "true"))
    }

    // read_json_by_line
    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, ["K00", "K01", "K02", "K03", "K04", "K05", "K06", "K07", "K08", "K09", "K10", "K11", "K12", "K13", "K14", "K15", "K16", "K17", "K18"], "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data_trim_double_quotes.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("force_parsing_by_standard_uri", "true")
                .addProperty("skip_lines", "1")
                .addProperty("strip_outer_array", "true")
                .addProperty("read_json_by_line", "false")
                .addProperty("trim_double_quotes", "true"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data_trim_double_quotes.csv")
            .addProperty("format", "csv")
            .addProperty("column_separator", "|")
            .addProperty("force_parsing_by_standard_uri", "true")
            .addProperty("skip_lines", "1")
            .addProperty("strip_outer_array", "true")
            .addProperty("read_json_by_line", "false")
            .addProperty("trim_double_quotes", "true"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, ["K00", "K01", "K02", "K03", "K04", "K05", "K06", "K07", "K08", "K09", "K10", "K11", "K12", "K13", "K14", "K15", "K16", "K17"], "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data_trim_double_quotes.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("force_parsing_by_standard_uri", "true")
                .addProperty("skip_lines", "1")
                .addProperty("strip_outer_array", "true")
                .addProperty("read_json_by_line", "false")
                .addProperty("trim_double_quotes", "true"))
    } 

    // json format
    // "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18","k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18"
    // def a3k = getS3AK()
    // def s3k = getS3SK()
    // def jsonpaths = '[\\"$.k00\\", \\"$.k01\\"]'
    // def sql_str =  """
    //         select * from s3(
    //             "s3.access_key" = "$a3k",
    //             "s3.secret_key" = "$s3k",
    //             "s3.region" = "${s3Region}",
    //             "uri" = "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.json",
    //             "format" = "json",
    //             "read_json_by_line" = "false",
    //             "strip_outer_array" = "true",
    //             "use_path_style" = "true",
    //             "jsonpaths" = "$jsonpaths",
    //             "force_parsing_by_standard_uri" = "true"
    //         ) limit 10;
    //     """
    // def sql_ret = sql """ ${sql_str} """ 
    // logger.info("sql_ret: $sql_ret")

    // jsonpaths
    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, "*","k00,k01,k12", "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.json")
                .addProperty("format", "json")
                .addProperty("read_json_by_line", "false")
                .addProperty("strip_outer_array", "true")
                .addProperty("column_separator", "|")
                .addProperty("use_path_style", "true")
                .addProperty("jsonpaths", '[\\"$.k00\\", \\"$.k01\\", \\"$.k12\\"]')
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "k00,k01,k02,k03,k04,k05, to_bitmap(k05) as k19, HLL_HASH(k05) as k20, TO_QUANTILE_STATE(k04, 1.0) as k21, to_bitmap(k05) as kd19, HLL_HASH(k05) as kd20, TO_QUANTILE_STATE(k04, 1.0) as kd21",
            "k00,k01,k02, k03,k04, k05,k19, k20, k21, kd19, kd20, kd21" ,"", "")
            .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.json")
            .addProperty("format", "json")
            .addProperty("read_json_by_line", "false")
            .addProperty("strip_outer_array", "true")
            .addProperty("column_separator", "|")
            .addProperty("jsonpaths", '[\\"$.k00\\", \\"$.k01\\", \\"$.k02\\", \\"$.k03\\", \\"$.k04\\", \\"$.k05\\"]')
            .addProperty("force_parsing_by_standard_uri", "true"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, "k00,k01,k02,k03,k04", "k00,k01,k02,k03,k04", "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data.json")
                .addProperty("format", "json")
                .addProperty("read_json_by_line", "false")
                .addProperty("strip_outer_array", "true")
                .addProperty("column_separator", "|")
                .addProperty("jsonpaths", '[\\"$.k00\\", \\"$.k01\\", \\"$.k02\\", \\"$.k03\\", \\"$.k04\\"]')
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, "*","k00,k01,k12", "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data_by_line.json")
                .addProperty("format", "json")
                .addProperty("read_json_by_line", "true")
                .addProperty("strip_outer_array", "true")
                .addProperty("column_separator", "|")
                .addProperty("use_path_style", "true")
                .addProperty("strip_outer_array", "false")
                .addProperty("jsonpaths", '[\\"$.k00\\", \\"$.k01\\", \\"$.k12\\"]')
                .addProperty("num_as_string", "true")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "k00,k01,k02,k03,k04,k05, to_bitmap(k05) as k19, HLL_HASH(k05) as k20, TO_QUANTILE_STATE(k04, 1.0) as k21, to_bitmap(k05) as kd19, HLL_HASH(k05) as kd20, TO_QUANTILE_STATE(k04, 1.0) as kd21",
            "k00,k01,k02, k03,k04, k05,k19, k20, k21, kd19, kd20, kd21" ,"", "")
            .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data_by_line.json")
            .addProperty("format", "json")
            .addProperty("read_json_by_line", "true")
            .addProperty("strip_outer_array", "false")
            .addProperty("column_separator", "|")
            .addProperty("jsonpaths", '[\\"$.k00\\", \\"$.k01\\", \\"$.k02\\", \\"$.k03\\", \\"$.k04\\", \\"$.k05\\"]')
            .addProperty("num_as_string", "true")
            .addProperty("force_parsing_by_standard_uri", "true"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, "k00,k01,k02,k03,k04", "k00,k01,k02,k03,k04", "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data_by_line.json")
                .addProperty("format", "json")
                .addProperty("read_json_by_line", "true")
                .addProperty("strip_outer_array", "false")
                .addProperty("column_separator", "|")
                .addProperty("jsonpaths", '[\\"$.k00\\", \\"$.k01\\", \\"$.k02\\", \\"$.k03\\", \\"$.k04\\"]')
                .addProperty("num_as_string", "true")
                .addProperty("force_parsing_by_standard_uri", "true"))
    } 

    // json_root
    attributeList.add(new TvfAttribute("nest_tbl_basic_tvf", "id, city, code", "id, city, code", "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/nest_json.json")
                .addProperty("format", "json")
                .addProperty("read_json_by_line", "true")
                .addProperty("json_root", '$.item')
                .addProperty("force_parsing_by_standard_uri", "true"))

    // num_as_string
    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18","k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18", "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data_by_line.json")
                .addProperty("format", "json")
                .addProperty("read_json_by_line", "true")
                .addProperty("strip_outer_array", "false")
                .addProperty("column_separator", "|")
                .addProperty("num_as_string", "true")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18, to_bitmap(k05) as k19, HLL_HASH(k05) as k20, TO_QUANTILE_STATE(k04, 1.0) as k21, to_bitmap(k05) as kd19, HLL_HASH(k05) as kd20, TO_QUANTILE_STATE(k04, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data_by_line.json")
            .addProperty("format", "json")
            .addProperty("read_json_by_line", "true")
            .addProperty("strip_outer_array", "false")
            .addProperty("num_as_string", "true")
            .addProperty("column_separator", "|")
            .addProperty("force_parsing_by_standard_uri", "true"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17", "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17", "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data_by_line.json")
                .addProperty("format", "json")
                .addProperty("read_json_by_line", "true")
                .addProperty("strip_outer_array", "false")
                .addProperty("column_separator", "|")
                .addProperty("num_as_string", "true")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18","k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18", "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.json")
                .addProperty("format", "json")
                .addProperty("read_json_by_line", "false")
                .addProperty("strip_outer_array", "true")
                .addProperty("num_as_string", "true")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18, to_bitmap(k05) as k19, HLL_HASH(k05) as k20, TO_QUANTILE_STATE(k04, 1.0) as k21, to_bitmap(k05) as kd19, HLL_HASH(k05) as kd20, TO_QUANTILE_STATE(k04, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.json")
            .addProperty("format", "json")
            .addProperty("read_json_by_line", "false")
            .addProperty("strip_outer_array", "true")
            .addProperty("column_separator", "|")
            .addProperty("num_as_string", "true")
            .addProperty("force_parsing_by_standard_uri", "true"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17", "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17", "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data.json")
                .addProperty("format", "json")
                .addProperty("read_json_by_line", "false")
                .addProperty("strip_outer_array", "true")
                .addProperty("column_separator", "|")
                .addProperty("num_as_string", "true")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    // fuzzy_parse
    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18","k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18", "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.json")
                .addProperty("format", "json")
                .addProperty("read_json_by_line", "false")
                .addProperty("strip_outer_array", "true")
                .addProperty("fuzzy_parse", "true")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18, to_bitmap(k05) as k19, HLL_HASH(k05) as k20, TO_QUANTILE_STATE(k04, 1.0) as k21, to_bitmap(k05) as kd19, HLL_HASH(k05) as kd20, TO_QUANTILE_STATE(k04, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.json")
            .addProperty("format", "json")
            .addProperty("read_json_by_line", "false")
            .addProperty("strip_outer_array", "true")
            .addProperty("fuzzy_parse", "true")
            .addProperty("force_parsing_by_standard_uri", "true"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17", "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17", "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data.json")
                .addProperty("format", "json")
                .addProperty("read_json_by_line", "false")
                .addProperty("strip_outer_array", "true")
                .addProperty("fuzzy_parse", "true")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }    



    // errors: no capital prop, no column mappings, array type loading stuck

    /* normal */
    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, ["K00", "K01", "K02", "K03", "K04", "K05", "K06", "K07", "K08", "K09", "K10", "K11", "K12", "K13", "K14", "K15", "K16", "K17", "K18"], "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.csv")
            .addProperty("format", "csv")
            .addProperty("column_separator", "|")
            .addProperty("force_parsing_by_standard_uri", "true"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, ["K00", "K01", "K02", "K03", "K04", "K05", "K06", "K07", "K08", "K09", "K10", "K11", "K12", "K13", "K14", "K15", "K16", "K17"], "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17", "k18"], "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.csv")
            .addProperty("format", "csv")
            .addProperty("column_separator", "|")
            .addProperty("force_parsing_by_standard_uri", "true"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17"], "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17", "k18"], "", "")
                .addProperty("uri", "https://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "https://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.csv")
            .addProperty("format", "csv")
            .addProperty("column_separator", "|"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17"], "", "")
                .addProperty("uri", "https://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|"))
    }

//    for(String table : basicTables) {
//        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17", "k18"], "", "")
//                .addProperty("uri", "https://${s3Endpoint}/${s3BucketName}/regression/load/data/basic_data.csv")
//                .addProperty("format", "csv")
//                .addProperty("column_separator", "|")
//                .addProperty("use_path_style", "true"))
//    }
//
//    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
//            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
//            .addProperty("uri", "https://${s3Endpoint}/${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.csv")
//            .addProperty("format", "csv")
//            .addProperty("column_separator", "|")
//            .addProperty("use_path_style", "true"))
//
//    for(String table : arrayTables) {
//        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17"], "", "")
//                .addProperty("uri", "https://${s3Endpoint}/${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data.csv")
//                .addProperty("format", "csv")
//                .addProperty("column_separator", "|")
//                .addProperty("use_path_style", "true"))
//    }

    /* error */
    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17", "k18"], "", "", true)
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data_with_errors.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "", true)
            .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data_with_errors.csv")
            .addProperty("format", "csv")
            .addProperty("column_separator", "|")
            .addProperty("force_parsing_by_standard_uri", "true"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17", "kd01", "kd02", "kd03", "kd04", "kd05", "kd06", "kd07", "kd08", "kd09", "kd10", "kd11", "kd12", "kd13", "kd14", "kd15", "kd16"], "", "", true)
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data_with_errors.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    /* skip lines */
    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17", "k18"], "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data_with_errors.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("skip_lines", "10")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data_with_errors.csv")
            .addProperty("format", "csv")
            .addProperty("column_separator", "|")
            .addProperty("skip_lines", "10")
            .addProperty("force_parsing_by_standard_uri", "true"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17"], "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data_with_errors.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("skip_lines", "10")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    /* compress type */
    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17", "k18"], "", "")
                .addProperty("uri", "https://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.csv.gz")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|").addProperty("compress_type", "GZ"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "https://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.csv.gz")
            .addProperty("format", "csv")
            .addProperty("column_separator", "|")
            .addProperty("compress_type", "GZ"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17"], "", "")
                .addProperty("uri", "https://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data.csv.gz")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("compress_type", "GZ"))
    }

    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17", "k18"], "", "")
                .addProperty("uri", "https://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.csv.bz2")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|").addProperty("compress_type", "BZ2"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "https://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.csv.bz2")
            .addProperty("format", "csv")
            .addProperty("column_separator", "|")
            .addProperty("compress_type", "BZ2"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17"], "", "")
                .addProperty("uri", "https://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data.csv.bz2")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("compress_type", "BZ2"))
    }

    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17", "k18"], "", "")
                .addProperty("uri", "https://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.csv.lz4")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|").addProperty("compress_type", "LZ4FRAME"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "https://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.csv.lz4")
            .addProperty("format", "csv")
            .addProperty("column_separator", "|")
            .addProperty("compress_type", "LZ4FRAME"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17"], "", "")
                .addProperty("uri", "https://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data.csv.lz4")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("compress_type", "LZ4FRAME"))
    }
    
    // compress_type: lzo
    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17", "k18"], "", "")
                .addProperty("uri", "https://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.csv.lzo")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|").addProperty("compress_type", "LZO"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "https://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.csv.lzo")
            .addProperty("format", "csv")
            .addProperty("column_separator", "|")
            .addProperty("compress_type", "LZO"))

    // we can upload basic_array_data.csv.lzo to s3
    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17"], "", "")
                .addProperty("uri", "https://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data.csv.lzo")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("compress_type", "LZO"))
    }

    // compress_type: deflate
    // for(String table : basicTables) {
    //     attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17", "k18"], "", "")
    //             .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data_test.csv.deflate")
    //             .addProperty("format", "csv")
    //             .addProperty("column_separator", "|")
    //             .addProperty("compress_type", "DEFLATE")
    //             .addProperty("force_parsing_by_standard_uri", "true"))
    // }

    // attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
    //         "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
    //         .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data_test.csv.deflate")
    //         .addProperty("format", "csv")
    //         .addProperty("column_separator", "|")
    //         .addProperty("compress_type", "DEFLATE")
    //         .addProperty("force_parsing_by_standard_uri", "true"))

    // for(String table : arrayTables) {
    //     attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17"], "", "")
    //             .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data.csv.deflate")
    //             .addProperty("format", "csv")
    //             .addProperty("column_separator", "|")
    //             .addProperty("compress_type", "DEFLATE")
    //             .addProperty("force_parsing_by_standard_uri", "true"))
    // }

    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17", "k18"], "WHERE c1 > 50", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"WHERE c1 > 50", "")
            .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.csv")
            .addProperty("format", "csv")
            .addProperty("column_separator", "|")
            .addProperty("force_parsing_by_standard_uri", "true"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17"], "WHERE c1 > 50", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    for(String table : uniqTable) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17", "k18"], "", "ORDER BY c1")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18","k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18", "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.parq")
                .addProperty("format", "parquet")
                .addProperty("column_separator", "|")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18, to_bitmap(k05) as k19, HLL_HASH(k05) as k20, TO_QUANTILE_STATE(k04, 1.0) as k21, to_bitmap(k05) as kd19, HLL_HASH(k05) as kd20, TO_QUANTILE_STATE(k04, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.parq")
            .addProperty("format", "parquet")
            .addProperty("column_separator", "|")
            .addProperty("force_parsing_by_standard_uri", "true"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17", "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17", "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data.parq")
                .addProperty("format", "parquet")
                .addProperty("column_separator", "|")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18","k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18", "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.orc")
                .addProperty("format", "orc")
                .addProperty("column_separator", "|")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18, to_bitmap(k05) as k19, HLL_HASH(k05) as k20, TO_QUANTILE_STATE(k04, 1.0) as k21, to_bitmap(k05) as kd19, HLL_HASH(k05) as kd20, TO_QUANTILE_STATE(k04, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.orc")
            .addProperty("format", "orc")
            .addProperty("column_separator", "|")
            .addProperty("force_parsing_by_standard_uri", "true"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17", "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17", "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data.orc")
                .addProperty("format", "orc")
                .addProperty("column_separator", "|")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18","k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18", "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.json")
                .addProperty("format", "json")
                .addProperty("read_json_by_line", "false")
                .addProperty("strip_outer_array", "true")
                .addProperty("column_separator", "|")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18, to_bitmap(k05) as k19, HLL_HASH(k05) as k20, TO_QUANTILE_STATE(k04, 1.0) as k21, to_bitmap(k05) as kd19, HLL_HASH(k05) as kd20, TO_QUANTILE_STATE(k04, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.json")
            .addProperty("format", "json")
            .addProperty("read_json_by_line", "false")
            .addProperty("strip_outer_array", "true")
            .addProperty("column_separator", "|")
            .addProperty("force_parsing_by_standard_uri", "true"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17", "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17", "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data.json")
                .addProperty("format", "json")
                .addProperty("read_json_by_line", "false")
                .addProperty("strip_outer_array", "true")
                .addProperty("column_separator", "|")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18","k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18", "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data_by_line.json")
                .addProperty("format", "json")
                .addProperty("read_json_by_line", "true")
                .addProperty("strip_outer_array", "false")
                .addProperty("column_separator", "|")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18, to_bitmap(k05) as k19, HLL_HASH(k05) as k20, TO_QUANTILE_STATE(k04, 1.0) as k21, to_bitmap(k05) as kd19, HLL_HASH(k05) as kd20, TO_QUANTILE_STATE(k04, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data_by_line.json")
            .addProperty("format", "json")
            .addProperty("read_json_by_line", "true")
            .addProperty("strip_outer_array", "false")
            .addProperty("column_separator", "|")
            .addProperty("force_parsing_by_standard_uri", "true"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17", "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17", "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data_by_line.json")
                .addProperty("format", "json")
                .addProperty("read_json_by_line", "true")
                .addProperty("strip_outer_array", "false")
                .addProperty("column_separator", "|")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }



   // line_delimiter: \t
   for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, ["K00", "K01", "K02", "K03", "K04", "K05", "K06", "K07", "K08", "K09", "K10", "K11", "K12", "K13", "K14", "K15", "K16", "K17", "K18"], "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data_by_line_delimiter.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("line_delimiter", "\t")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data_by_line_delimiter.csv")
            .addProperty("format", "csv")
            .addProperty("column_separator", "|")
            .addProperty("line_delimiter", "\t")
            .addProperty("force_parsing_by_standard_uri", "true"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, ["K00", "K01", "K02", "K03", "K04", "K05", "K06", "K07", "K08", "K09", "K10", "K11", "K12", "K13", "K14", "K15", "K16", "K17"], "", "")
                .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data_by_tab_line_delimiter.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("line_delimiter", "\t")
                .addProperty("force_parsing_by_standard_uri", "true"))
    }

    // invalid line delimiter, this will case error
    // for(String table : basicTables) {
    //     attributeList.add(new TvfAttribute(table, ["K00", "K01", "K02", "K03", "K04", "K05", "K06", "K07", "K08", "K09", "K10", "K11", "K12", "K13", "K14", "K15", "K16", "K17", "K18"], "", "")
    //             .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.csv")
    //             .addProperty("format", "csv")
    //             .addProperty("column_separator", "|")
    //             .addProperty("line_delimiter", ",")
    //             .addProperty("force_parsing_by_standard_uri", "true"))
    // }

    // attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
    //         "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
    //         .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_data.csv")
    //         .addProperty("format", "csv")
    //         .addProperty("column_separator", "|")
    //         .addProperty("line_delimiter", ",")
    //         .addProperty("force_parsing_by_standard_uri", "true"))

    // for(String table : arrayTables) {
    //     attributeList.add(new TvfAttribute(table, ["K00", "K01", "K02", "K03", "K04", "K05", "K06", "K07", "K08", "K09", "K10", "K11", "K12", "K13", "K14", "K15", "K16", "K17"], "", "")
    //             .addProperty("uri", "s3://${s3BucketName}.${s3Endpoint}/regression/load/data/basic_array_data.csv")
    //             .addProperty("format", "csv")
    //             .addProperty("column_separator", "|")
    //             .addProperty("line_delimiter", ",")
    //             .addProperty("force_parsing_by_standard_uri", "true"))
    // }


    def ak = getS3AK()
    def sk = getS3SK()

    for(String tbl : tables) {
        sql new File("""${ context.file.parent }/ddl/${tbl}_drop.sql""").text
        sql new File("""${ context.file.parent }/ddl/${tbl}.sql""").text
    }

    def i = 0
 
    for (TvfAttribute attribute : attributeList) {

        def prop = attribute.getPropertiesStr()
        def insertList = attribute.getInsertList()
        def selectList = attribute.getSelectList()
        def sqlStr = """
        INSERT INTO ${attribute.tableName} (${insertList})
        SELECT ${selectList}
        FROM S3 (
            "s3.access_key" = "$ak",
            "s3.secret_key" = "$sk",
            "s3.region" = "${s3Region}",
            ${prop}
        ) ${attribute.whereClause}
          ${attribute.orderByClause}
        """

        logger.info("submit sql: ${sqlStr}");
        try {
            sql """${sqlStr}"""
        } catch (Exception ex) {
            assertTrue(attribute.expectFiled)
            logger.info("error: ", ex)
        }
        qt_select """ select count(*) from $attribute.tableName """
        ++i
    } 



}

class TvfAttribute {
    public String tableName
    public String[] columns = new ArrayList<>()
    public Map<String, String> properties = new HashMap<>()
    public String whereClause
    public String orderByClause
    public boolean expectFiled
    public String selectList
    public String insertList

    TvfAttribute(String tableName, List<String> columns, String whereClause, String orderByClause, boolean expectFiled = false) {
        this.tableName = tableName
        this.columns = columns
        this.whereClause = whereClause
        this.orderByClause = orderByClause
        this.expectFiled = expectFiled
        this.insertList = ""
        this.selectList = ""
    }

    TvfAttribute(String tableName, String selectList, String insertList, String whereClause, String orderByClause, boolean expectFiled = false) {
        this.tableName = tableName
        this.columns = columns
        this.whereClause = whereClause
        this.orderByClause = orderByClause
        this.expectFiled = expectFiled
        this.insertList = insertList
        this.selectList = selectList
    }

    String getSelectList() {
        if (selectList.length() != 0) {
            return selectList
        }
        if ("csv".equalsIgnoreCase(properties.get("format"))) {
            String res = ""
            def i = 1
            for(String column : columns) {
                res += ("c$i as $column")
                res += ","
                ++i
            }
            return res.substring(0, res.size() - 1)
        }
        return getInsertList()
    }



    String getInsertList() {
        if (insertList.length() != 0) {
            return insertList
        }
        String res = ""
        for(String column : columns) {
            res += column
            res += ","
        }
        return res.substring(0, res.size() - 1)
    }

    String getPropertiesStr() {
        if (properties.isEmpty()) {
            return "*"
        }
        String prop = ""
        properties.forEach (k, v) -> {
            prop += "\"${k}\" = \"${v}\","
        }
        prop = prop.substring(0, prop.size() - 1)
        return prop
    }

    String get(String k) {
        return properties.get(k)
    }

    TvfAttribute addProperty(String k, String v) {
        properties.put(k, v)
        return this
    }


}
