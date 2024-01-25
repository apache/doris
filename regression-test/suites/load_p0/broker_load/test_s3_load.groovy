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

suite("test_s3_load", "load_p0") {

    def tables = [
            "agg_tbl_basic",
            "dup_tbl_array",
            "dup_tbl_basic",
            "mow_tbl_array",
            "mow_tbl_basic",
            "uniq_tbl_array",
            "uniq_tbl_basic"
    ]

    //deal with agg tables in separate
    def basicTables = [
            "dup_tbl_basic",
            "mow_tbl_basic",
            "uniq_tbl_basic",
    ]

    def arrayTables = [
            "dup_tbl_array",
            "uniq_tbl_array",
            "mow_tbl_array"
    ]

    def uniqTables = [
            "mow_tbl_basic",
            "uniq_tbl_basic"
    ]

    def attributesList = [

    ]

    /* ========================================================== normal ========================================================== */
    for (String table : basicTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.csv",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
                "", "", "", "", ""))
    }

    attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.csv",
            "agg_tbl_basic", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\" ", "FORMAT AS \"csv\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "SET (k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0))", "", ""))

    for (String table : arrayTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_array_data.csv",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17)",
                "", "", "", "", ""))
    }

    for (String table : basicTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.csv",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(K00,K01,K02,K03,K04,K05,K06,K07,K08,K09,K10,K11,K12,K13,K14,K15,K16,K17,K18)",
                "", "", "", "", ""))
    }

    attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.csv",
            "agg_tbl_basic", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\" ", "FORMAT AS \"csv\"", "(K00,K01,K02,K03,K04,K05,K06,k07,K08,K09,K10,K11,K12,K13,K14,K15,K16,K17,K18)",
            "", "", "SET (K19=to_bitmap(k04),K20=HLL_HASH(k04),K21=TO_QUANTILE_STATE(K04,1.0),Kd19=to_bitmap(K05),kd20=HLL_HASH(K05),KD21=TO_QUANTILE_STATE(K05,1.0))", "", ""))

    for (String table : arrayTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_array_data.csv",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(K00,K01,K02,K03,K04,K05,K06,K07,K08,K09,K10,K11,K12,K13,K14,K15,K16,K17)",
                "", "", "", "", ""))
    }
    // TODO: should be success ?
//    for (String table : basicTables) {
//        attributesList.add(new LoadAttributes("s3://cos.ap-beijing.myqcloud.com/doris-build-1308700295/regression/load/data/basic_data.csv",
//                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
//                "", "", "", "", "").withPathStyle())
//    }
//
//    attributesList.add(new LoadAttributes("s3://cos.ap-beijing.myqcloud.com/doris-build-1308700295/regression/load/data/basic_data.csv",
//            "agg_tbl_basic", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\" ", "FORMAT AS \"csv\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
//            "", "", "SET (k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0))", "", "").withPathStyle())
//
//    for (String table : arrayTables) {
//        attributesList.add(new LoadAttributes("s3://cos.ap-beijing.myqcloud.com/doris-build-1308700295/regression/load/data/basic_array_data.csv",
//                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17)",
//                "", "", "", "", "").withPathStyle())
//    }
//
//    for (String table : basicTables) {
//        attributesList.add(new LoadAttributes("s3://cos.ap-beijing.myqcloud.com/doris-build-1308700295/regression/load/data/basic_data.csv",
//                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
//                "", "", "", "", "").withPathStyle())
//    }
//
//    attributesList.add(new LoadAttributes("s3://cos.ap-beijing.myqcloud.com/doris-build-1308700295/regression/load/data/basic_data.csv",
//            "agg_tbl_basic", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\" ", "FORMAT AS \"csv\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
//            "", "", "SET (k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0))", "", "").withPathStyle())
//
//    for (String table : arrayTables) {
//        attributesList.add(new LoadAttributes("s3://cos.ap-beijing.myqcloud.com/doris-build-1308700295/regression/load/data/basic_array_data.csv",
//                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17)",
//                "", "", "", "", "").withPathStyle())
//    }

    /* ========================================================== error ========================================================== */
    for (String table : basicTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data_with_errors.csv",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
                "", "", "", "", "", true))
    }

    attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data_with_errors.csv",
            "agg_tbl_basic", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\" ", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "SET (k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0))", "", "", true))

    for (String table : arrayTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_array_data_with_errors.csv",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"csv\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17)",
                "", "", "", "", "", true))
    }

// has problem, should be success
//    for(String table: basicTables) {
//        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data_with_errors.csv",
//                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
//                "", "", "", "","").addProperties("max_filter_ratio", "0.5"))
//    }
//
//    attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data_with_errors.csv",
//            "agg_tbl_basic", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\" ", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
//            "", "", "SET (k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0))", "", "").addProperties("max_filter_ratio", "0.5"))
//
//    for(String table : arrayTables) {
//        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_array_data_with_errors.csv",
//                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17)",
//                "", "", "", "","").addProperties("max_filter_ratio", "0.5"))
//    }

//    for(String table: basicTables) {
//        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data_with_errors.csv",
//                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
//                "", "", "", "","", true).addProperties("max_filter_ratio", "0.4"))
//    }
//
//    attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data_with_errors.csv",
//            "agg_tbl_basic", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\" ", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
//            "", "", "SET (k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0))", "", "", true).addProperties("max_filter_ratio", "0.4"))
//
//    for(String table : arrayTables) {
//        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_array_data_with_errors.csv",
//                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17)",
//                "", "", "", "","", true).addProperties("max_filter_ratio", "0.4"))
//    }

    // skip lines
//    for(String table: basicTables) {
//        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data_with_errors.csv",
//                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
//                "", "", "", "","").addProperties("skip_lines", "10"))
//    }
//
//    attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data_with_errors.csv",
//            "agg_tbl_basic", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\" ", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
//            "", "", "SET (k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0))", "", "").addProperties("skip_lines", "10"))
//
//    for(String table : arrayTables) {
//        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_array_data_with_errors.csv",
//                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17)",
//                "", "", "", "","").addProperties("skip_lines", "10"))
//    }

    /* ========================================================== wrong column sep ========================================================== */
    for (String table : basicTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.csv",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \",\"", "FORMAT AS \"csv\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
                "", "", "", "", "", true))
    }

    attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.csv",
            "agg_tbl_basic", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \",\" ", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "SET (k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0))", "", "", true))

    for (String table : arrayTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_array_data.csv",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \",\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17)",
                "", "", "", "", "", true))
    }

    /* ========================================================== wrong line delim ========================================================== */
    for (String table : basicTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.csv",
                "${table}", "LINES TERMINATED BY \"\t\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
                "", "", "", "", "", true))
    }

    attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.csv",
            "agg_tbl_basic", "LINES TERMINATED BY \"\t\"", "COLUMNS TERMINATED BY \"|\" ", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "SET (k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0))", "", "", true))

    for (String table : arrayTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_array_data.csv",
                "${table}", "LINES TERMINATED BY \"\t\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17)",
                "", "", "", "", "", true))
    }

    /* ========================================================== strict mode ========================================================== */
    for(String table: basicTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data_with_errors.csv",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
                "", "", "", "","", true).addProperties("strict_mode", "true"))
    }

    attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data_with_errors.csv",
            "agg_tbl_basic", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\" ", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "SET (k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0))", "", "", true).addProperties("strict_mode","true"))

    for(String table : arrayTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_array_data_with_errors.csv",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17)",
                "", "", "", "","", true).addProperties("strict_mode", "true"))
    }

    /* ========================================================== timezone ========================================================== */

    for(String table: basicTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.csv",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
                "", "", "SET (k00=unix_timestamp('2023-09-01 12:00:00'))", "","").addProperties("timezone", "Asia/Shanghai"))
    }

    attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.csv",
            "agg_tbl_basic", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\" ", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "SET (k00=unix_timestamp('2023-09-01 12:00:00'),k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0))", "", "").addProperties("timezone", "Asia/Shanghai"))

    for(String table : arrayTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_array_data.csv",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17)",
                "", "", "SET (k00=unix_timestamp('2023-09-01 12:00:00'))", "","").addProperties("timezone", "Asia/Shanghai"))
    }

    for(String table: basicTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.csv",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
                "", "", "SET (k00=unix_timestamp('2023-09-01 12:00:00'))", "","").addProperties("timezone", "America/Chicago"))
    }

    attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.csv",
            "agg_tbl_basic", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\" ", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "SET (k00=unix_timestamp('2023-09-01 12:00:00'),k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0))", "", "").addProperties("timezone", "America/Chicago"))

    for(String table : arrayTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_array_data.csv",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17)",
                "", "", "SET (k00=unix_timestamp('2023-09-01 12:00:00'))", "","").addProperties("timezone", "America/Chicago"))
    }

    /* ========================================================== compress type ========================================================== */
    for (String table : basicTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.csv.gz",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
                "", "", "", "", ""))
    }

    attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.csv.gz",
            "agg_tbl_basic", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\" ", "FORMAT AS \"csv\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "SET (k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0))", "", ""))

    for (String table : arrayTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_array_data.csv.gz",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17)",
                "", "", "", "", ""))
    }

    for (String table : basicTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.csv.bz2",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
                "", "", "", "", ""))
    }

    attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.csv.bz2",
            "agg_tbl_basic", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\" ", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "SET (k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0))", "", ""))

    for (String table : arrayTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_array_data.csv.bz2",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17)",
                "", "", "", "", ""))
    }

    for (String table : basicTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.csv.lz4",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
                "", "", "", "", ""))
    }

    attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.csv.lz4",
            "agg_tbl_basic", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\" ", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "SET (k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0))", "", ""))

    for (String table : arrayTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_array_data.csv.lz4",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17)",
                "", "", "", "", ""))
    }


    for (String table : basicTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.csv.gz",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
                "", "", "", "", ""))
    }

    attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.csv.gz",
            "agg_tbl_basic", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\" ", "", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "SET (k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0))", "", ""))

    for (String table : arrayTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_array_data.csv.gz",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17)",
                "", "", "", "", ""))
    }

    for (String table : basicTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.csv.bz2",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
                "", "", "", "", ""))
    }

    attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.csv.bz2",
            "agg_tbl_basic", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\" ", "", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "SET (k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0))", "", ""))

    for (String table : arrayTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_array_data.csv.bz2",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17)",
                "", "", "", "", ""))
    }

    for (String table : basicTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.csv.lz4",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
                "", "", "", "", ""))
    }

    attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.csv.lz4",
            "agg_tbl_basic", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\" ", "", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "SET (k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0))", "", ""))

    for (String table : arrayTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_array_data.csv.lz4",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17)",
                "", "", "", "", ""))
    }

    /*========================================================== order by ==========================================================*/

    for (String table : uniqTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.csv",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
                "", "", "", "", "ORDER BY k01"))
    }

    /*========================================================== json ==========================================================*/

    for (String table : basicTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.json",
                "${table}", "", "", "FORMAT AS \"json\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
                "", "", "", "", "PROPERTIES(\"strip_outer_array\" = \"true\", \"fuzzy_parse\" = \"true\")"))
    }

    attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.json",
            "agg_tbl_basic", "", "", "FORMAT AS \"json\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "SET (k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0))", "", "PROPERTIES(\"strip_outer_array\" = \"true\", \"fuzzy_parse\" = \"true\")"))

    for (String table : arrayTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_array_data.json",
                "${table}", "", "", "FORMAT AS \"json\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17)",
                "", "", "", "", "PROPERTIES(\"strip_outer_array\" = \"true\", \"fuzzy_parse\" = \"true\")"))
    }

    for (String table : basicTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data_by_line.json",
                "${table}", "", "", "FORMAT AS \"JSON\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
                "", "", "", "", "PROPERTIES(\"read_json_by_line\" = \"true\")"))
    }

    attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data_by_line.json",
            "agg_tbl_basic", "", "", "FORMAT AS \"JSON\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "SET (k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0))", "", "PROPERTIES(\"read_json_by_line\" = \"true\")"))

    for (String table : arrayTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_array_data_by_line.json",
                "${table}", "", "", "FORMAT AS \"JSON\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17)",
                "", "", "", "", "PROPERTIES(\"read_json_by_line\" = \"true\")"))
    }

    for (String table : basicTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.parq",
                "${table}", "", "", "FORMAT AS \"parquet\"", "(K00,K01,K02,K03,K04,K05,K06,K07,K08,K09,K10,K11,K12,K13,K14,K15,K16,K17,K18)",
                "", "", "", "", ""))
    }

    attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.parq",
            "agg_tbl_basic", "", "", "FORMAT AS \"PARQUET\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "SET (k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0))", "", ""))

//    for (String table : arrayTables) {
//        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_array_data.parq",
//                "${table}", "", "", "FORMAT AS \"parquet\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17)",
//                "", "", "", "", ""))
//    }

    for (String table : basicTables) {
        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.orc",
                "${table}", "", "", "FORMAT AS \"orc\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
                "", "", "", "", ""))
    }

    attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data.orc",
            "agg_tbl_basic", "", "", "FORMAT AS \"ORC\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "SET (k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0))", "", ""))

//    for (String table : arrayTables) {
//        attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_array_data.parq",
//                "${table}", "", "", "FORMAT AS \"parquet\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17)",
//                "", "", "", "", ""))
//    }

    for(String table : uniqTables) {
        def attributes = new LoadAttributes("s3://doris-build-1308700295/regression/load/data/basic_data_delete.csv",
                "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,__DEL__)",
                "", "", "", "", "DELETE ON __DEL__=true")
        attributes.dataDesc.mergeType = "MERGE"
        attributesList.add(attributes)
    }

    def ak = getS3AK()
    def sk = getS3SK()

    for(String tbl : tables) {
        sql new File("""${ context.file.parent }/ddl/${tbl}_drop.sql""").text
        sql new File("""${ context.file.parent }/ddl/${tbl}.sql""").text
    }

    def i = 0
    for (LoadAttributes attributes : attributesList) {
        def label = "test_s3_load_" + UUID.randomUUID().toString().replace("-", "_") + "_" + i
        attributes.label = label
        def prop = attributes.getPropertiesStr()

        def sql_str = """
            LOAD LABEL $label (
                $attributes.dataDesc.mergeType
                DATA INFILE("$attributes.dataDesc.path")
                INTO TABLE $attributes.dataDesc.tableName
                $attributes.dataDesc.columnTermClause
                $attributes.dataDesc.lineTermClause
                $attributes.dataDesc.formatClause
                $attributes.dataDesc.columns
                $attributes.dataDesc.columnsFromPathClause
                $attributes.dataDesc.columnMappingClause
                $attributes.dataDesc.precedingFilterClause
                $attributes.dataDesc.orderByClause
                $attributes.dataDesc.whereExpr
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "$ak",
                "AWS_SECRET_KEY" = "$sk",
                "AWS_ENDPOINT" = "cos.ap-beijing.myqcloud.com",
                "AWS_REGION" = "ap-beijing",
                "use_path_style" = "$attributes.usePathStyle"
            )
            ${prop}
            """
        logger.info("submit sql: ${sql_str}");
        sql """${sql_str}"""
        logger.info("Submit load with lable: $label, table: $attributes.dataDesc.tableName, path: $attributes.dataDesc.path")

        def max_try_milli_secs = 600000
        while (max_try_milli_secs > 0) {
            String[][] result = sql """ show load where label="$attributes.label" order by createtime desc limit 1; """
            if (result[0][2].equals("FINISHED")) {
                if (attributes.isExceptFailed) {
                    assertTrue(false, "load should be failed but was success: $result")
                }
                logger.info("Load FINISHED " + attributes.label + ": $result")
                break
            }
            if (result[0][2].equals("CANCELLED")) {
                if (attributes.isExceptFailed) {
                    logger.info("Load FINISHED " + attributes.label)
                    break
                }
                assertTrue(false, "load failed: $result")
                break
            }
            Thread.sleep(1000)
            max_try_milli_secs -= 1000
            if (max_try_milli_secs <= 0) {
                assertTrue(false, "load Timeout: $attributes.label")
            }
        }
        qt_select """ select count(*) from $attributes.dataDesc.tableName """
        ++i
    }

//    for (LoadAttributes attributes : attributesList) {
//        def max_try_milli_secs = 600000
//        while (max_try_milli_secs > 0) {
//            String[][] result = sql """ show load where label="$attributes.label" order by createtime desc limit 1; """
//            if (result[0][2].equals("FINISHED")) {
//                if (attributes.isExceptFailed) {
//                    assertTrue(false, "load should be failed but was success: $result")
//                }
//                logger.info("Load FINISHED " + attributes.label + ": $result")
//                break
//            }
//            if (result[0][2].equals("CANCELLED")) {
//                if (attributes.isExceptFailed) {
//                    logger.info("Load FINISHED " + attributes.label)
//                    break
//                }
//                assertTrue(false, "load failed: $result")
//                break
//            }
//            Thread.sleep(1000)
//            max_try_milli_secs -= 1000
//            if (max_try_milli_secs <= 0) {
//                assertTrue(false, "load Timeout: $attributes.label")
//            }
//        }
//    }

    for(String tbl : tables) {
        qt_select """ select count(*) from ${tbl} """
    }
}

class DataDesc {
    public String mergeType = ""
    public String path
    public String tableName
    public String lineTermClause
    public String columnTermClause
    public String formatClause
    public String columns
    public String columnsFromPathClause
    public String precedingFilterClause
    public String columnMappingClause
    public String whereExpr
    public String orderByClause
}

class LoadAttributes {
    LoadAttributes(String path, String tableName, String lineTermClause, String columnTermClause, String formatClause,
                   String columns, String columnsFromPathClause, String precedingFilterClause, String columnMappingClause, String whereExpr, String orderByClause, boolean isExceptFailed = false) {
        this.dataDesc = new DataDesc()
        this.dataDesc.path = path
        this.dataDesc.tableName = tableName
        this.dataDesc.lineTermClause = lineTermClause
        this.dataDesc.columnTermClause = columnTermClause
        this.dataDesc.formatClause = formatClause
        this.dataDesc.columns = columns
        this.dataDesc.columnsFromPathClause = columnsFromPathClause
        this.dataDesc.precedingFilterClause = precedingFilterClause
        this.dataDesc.columnMappingClause = columnMappingClause
        this.dataDesc.whereExpr = whereExpr
        this.dataDesc.orderByClause = orderByClause

        this.isExceptFailed = isExceptFailed

        properties = new HashMap<>()
        properties.put("use_new_load_scan_node", "true")
    }

    LoadAttributes addProperties(String k, String v) {
        properties.put(k, v)
        return this
    }

    String getPropertiesStr() {
        if (properties.isEmpty()) {
            return ""
        }
        String prop = "PROPERTIES ("
        properties.forEach (k, v) -> {
            prop += "\"${k}\" = \"${v}\","
        }
        prop = prop.substring(0, prop.size() - 1)
        prop += ")"
        return prop
    }

    LoadAttributes withPathStyle() {
        usePathStyle = "true"
        return this
    }

    public DataDesc dataDesc
    public Map<String, String> properties
    public String label
    public String usePathStyle = "false"
    public boolean isExceptFailed
}
