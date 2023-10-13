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

suite("test_s3_tvf", "load_p0") {

    def tables = [
            "agg_tbl_basic_tvf",
            "dup_tbl_array_tvf",
            "dup_tbl_basic_tvf",
            "mow_tbl_array_tvf",
            "mow_tbl_basic_tvf",
            "uniq_tbl_array_tvf",
            "uniq_tbl_basic_tvf"
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

    // errors: no capital prop, no column mappings, array type loading stuck

    /* normal */
    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, ["K00", "K01", "K02", "K03", "K04", "K05", "K06", "K07", "K08", "K09", "K10", "K11", "K12", "K13", "K14", "K15", "K16", "K17", "K18"], "", "")
                .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data.csv")
            .addProperty("format", "csv")
            .addProperty("column_separator", "|"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, ["K00", "K01", "K02", "K03", "K04", "K05", "K06", "K07", "K08", "K09", "K10", "K11", "K12", "K13", "K14", "K15", "K16", "K17"], "", "").addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_array_data.csv")
                .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_array_data.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|"))
    }

    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17", "k18"], "", "")
                .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data.csv")
            .addProperty("format", "csv")
            .addProperty("column_separator", "|"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17"], "", "")
                .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_array_data.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|"))
    }

    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17", "k18"], "", "")
                .addProperty("uri", "https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data.csv")
            .addProperty("format", "csv")
            .addProperty("column_separator", "|"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17"], "", "")
                .addProperty("uri", "https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_array_data.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|"))
    }

//    for(String table : basicTables) {
//        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17", "k18"], "", "")
//                .addProperty("uri", "https://cos.ap-beijing.myqcloud.com/doris-build-1308700295/regression/load/data/basic_data.csv")
//                .addProperty("format", "csv")
//                .addProperty("column_separator", "|")
//                .addProperty("use_path_style", "true"))
//    }
//
//    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
//            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
//            .addProperty("uri", "https://cos.ap-beijing.myqcloud.com/doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data.csv")
//            .addProperty("format", "csv")
//            .addProperty("column_separator", "|")
//            .addProperty("use_path_style", "true"))
//
//    for(String table : arrayTables) {
//        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17"], "", "")
//                .addProperty("uri", "https://cos.ap-beijing.myqcloud.com/doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_array_data.csv")
//                .addProperty("format", "csv")
//                .addProperty("column_separator", "|")
//                .addProperty("use_path_style", "true"))
//    }

    /* error */
    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17", "k18"], "", "", true)
                .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data_with_errors.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "", true)
            .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data_with_errors.csv")
            .addProperty("format", "csv")
            .addProperty("column_separator", "|"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17"], "", "", true)
                .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_array_data_with_errors.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|"))
    }

    /* skip lines */
    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17", "k18"], "", "")
                .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data_with_errors.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("skip_lines", "10"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data_with_errors.csv")
            .addProperty("format", "csv")
            .addProperty("column_separator", "|")
            .addProperty("skip_lines", "10"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17"], "", "")
                .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_array_data_with_errors.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("skip_lines", "10"))
    }

    /* compress type */
    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17", "k18"], "", "")
                .addProperty("uri", "https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data.csv.gz")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|").addProperty("compress_type", "GZ"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data.csv.gz")
            .addProperty("format", "csv")
            .addProperty("column_separator", "|")
            .addProperty("compress_type", "GZ"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17"], "", "")
                .addProperty("uri", "https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_array_data.csv.gz")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("compress_type", "GZ"))
    }

    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17", "k18"], "", "")
                .addProperty("uri", "https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data.csv.bz2")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|").addProperty("compress_type", "BZ2"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data.csv.bz2")
            .addProperty("format", "csv")
            .addProperty("column_separator", "|")
            .addProperty("compress_type", "BZ2"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17"], "", "")
                .addProperty("uri", "https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_array_data.csv.bz2")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("compress_type", "BZ2"))
    }

    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17", "k18"], "", "")
                .addProperty("uri", "https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data.csv.lz4")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|").addProperty("compress_type", "LZ4FRAME"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data.csv.lz4")
            .addProperty("format", "csv")
            .addProperty("column_separator", "|")
            .addProperty("compress_type", "LZ4FRAME"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17"], "", "")
                .addProperty("uri", "https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_array_data.csv.lz4")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|")
                .addProperty("compress_type", "LZ4FRAME"))
    }

    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17", "k18"], "WHERE c1 > 50", "")
                .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "c1 as k00,c2 as k01,c3 as k02,c4 as k03,c5 as k04,c6 as k05,c7 as k06,c8 as k07,c9 as k08,c10 as k09,c11 as k10,c12 as k11,c13 as k12,c14 as k13,c15 as k14,c16 as k15,c17 as k16,c18 as k17,c19 as k18, to_bitmap(c6) as k19, HLL_HASH(c6) as k20, TO_QUANTILE_STATE(c5, 1.0) as k21, to_bitmap(c6) as kd19, HLL_HASH(c6) as kd20, TO_QUANTILE_STATE(c5, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"WHERE c1 > 50", "")
            .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data.csv")
            .addProperty("format", "csv")
            .addProperty("column_separator", "|"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17"], "WHERE c1 > 50", "")
                .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_array_data.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|"))
    }

    for(String table : uniqTable) {
        attributeList.add(new TvfAttribute(table, ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17", "k18"], "", "ORDER BY c1")
                .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data.csv")
                .addProperty("format", "csv")
                .addProperty("column_separator", "|"))
    }

    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18","k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18", "", "")
                .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data.parq")
                .addProperty("format", "parquet")
                .addProperty("column_separator", "|"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18, to_bitmap(k05) as k19, HLL_HASH(k05) as k20, TO_QUANTILE_STATE(k04, 1.0) as k21, to_bitmap(k05) as kd19, HLL_HASH(k05) as kd20, TO_QUANTILE_STATE(k04, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data.parq")
            .addProperty("format", "parquet")
            .addProperty("column_separator", "|"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17", "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17", "", "")
                .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_array_data.parq")
                .addProperty("format", "parquet")
                .addProperty("column_separator", "|"))
    }

    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18","k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18", "", "")
                .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data.orc")
                .addProperty("format", "orc")
                .addProperty("column_separator", "|"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18, to_bitmap(k05) as k19, HLL_HASH(k05) as k20, TO_QUANTILE_STATE(k04, 1.0) as k21, to_bitmap(k05) as kd19, HLL_HASH(k05) as kd20, TO_QUANTILE_STATE(k04, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data.orc")
            .addProperty("format", "orc")
            .addProperty("column_separator", "|"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17", "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17", "", "")
                .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_array_data.orc")
                .addProperty("format", "orc")
                .addProperty("column_separator", "|"))
    }

    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18","k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18", "", "")
                .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data.json")
                .addProperty("format", "json")
                .addProperty("read_json_by_line", "false")
                .addProperty("strip_outer_array", "true")
                .addProperty("column_separator", "|"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18, to_bitmap(k05) as k19, HLL_HASH(k05) as k20, TO_QUANTILE_STATE(k04, 1.0) as k21, to_bitmap(k05) as kd19, HLL_HASH(k05) as kd20, TO_QUANTILE_STATE(k04, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data.json")
            .addProperty("format", "json")
            .addProperty("read_json_by_line", "false")
            .addProperty("strip_outer_array", "true")
            .addProperty("column_separator", "|"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17", "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17", "", "")
                .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_array_data.json")
                .addProperty("format", "json")
                .addProperty("read_json_by_line", "false")
                .addProperty("strip_outer_array", "true")
                .addProperty("column_separator", "|"))
    }

    for(String table : basicTables) {
        attributeList.add(new TvfAttribute(table, "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18","k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18", "", "")
                .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data_by_line.json")
                .addProperty("format", "json")
                .addProperty("read_json_by_line", "true")
                .addProperty("strip_outer_array", "false")
                .addProperty("column_separator", "|"))
    }

    attributeList.add(new TvfAttribute("agg_tbl_basic_tvf", "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18, to_bitmap(k05) as k19, HLL_HASH(k05) as k20, TO_QUANTILE_STATE(k04, 1.0) as k21, to_bitmap(k05) as kd19, HLL_HASH(k05) as kd20, TO_QUANTILE_STATE(k04, 1.0) as kd21",
            "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19,k20,k21,kd19,kd20,kd21" ,"", "")
            .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_data_by_line.json")
            .addProperty("format", "json")
            .addProperty("read_json_by_line", "true")
            .addProperty("strip_outer_array", "false")
            .addProperty("column_separator", "|"))

    for(String table : arrayTables) {
        attributeList.add(new TvfAttribute(table, "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17", "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17", "", "")
                .addProperty("uri", "s3://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/load/data/basic_array_data_by_line.json")
                .addProperty("format", "json")
                .addProperty("read_json_by_line", "true")
                .addProperty("strip_outer_array", "false")
                .addProperty("column_separator", "|"))
    }

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
            "s3.region" = "ap-beijing",
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

    TvfAttribute addProperty(String k, String v) {
        properties.put(k, v)
        return this
    }
}