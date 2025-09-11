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

suite("test_date_floor_ceil") {
    sql "set enable_nereids_planner=true;"
    sql "set enable_fallback_to_original_planner=false;"
    sql "set enable_fold_constant_by_be=false;"

    qt_sql1 """select date_floor("2023-07-14 10:51:11",interval 5 second); """
    qt_sql2 """select date_floor("2023-07-14 10:51:00",interval 5 minute); """
    qt_sql3 """select date_floor("2023-07-14 10:51:00",interval 5 hour); """
    qt_sql4 """select date_floor("2023-07-14 10:51:00",interval 5 day);   """
    qt_sql5 """select date_floor("2023-07-14 10:51:00",interval 5 month); """
    qt_sql6 """select date_floor("2023-07-14 10:51:00",interval 5 year); """

    qt_sql7 """select date_ceil("2023-07-14 10:51:11",interval 5 second); """
    qt_sql8 """select date_ceil("2023-07-14 10:51:00",interval 5 minute); """
    qt_sql9 """select date_ceil("2023-07-14 10:51:00",interval 5 hour); """
    qt_sql10 """select date_ceil("2023-07-14 10:51:00",interval 5 day);   """
    qt_sql11 """select date_ceil("2023-07-14 10:51:00",interval 5 month); """
    qt_sql12 """select date_ceil("2023-07-14 10:51:00",interval 5 year); """

    // test hour_floor
    qt_sql1 """select hour_floor("2023-07-14 10:51:00", 5, "0001-01-01 00:00:00");"""
    qt_sql2 """select hour_floor("2023-07-14 10:51:00", 5, "1970-01-01 00:00:00");"""
    qt_sql3 """select hour_floor("2023-07-14 10:51:00", 5);"""
    sql """set debug_skip_fold_constant = true"""
    qt_sql1 """select hour_floor("2023-07-14 10:51:00", 5, "0001-01-01 00:00:00");"""
    qt_sql2 """select hour_floor("2023-07-14 10:51:00", 5, "1970-01-01 00:00:00");"""
    qt_sql3 """select hour_floor("2023-07-14 10:51:00", 5);"""
    

    sql """set debug_skip_fold_constant = false"""
    qt_x1 """ select date_floor('9999-12-31 23:59:59.999999', interval 5 minute); """
    qt_x2 """ select date_floor('9999-12-31 23:59:59.999999', interval 33333 year); """
    qt_x3 """ select date_floor('9999-12-31 23:59:59.999999', interval -10 year); """
    qt_x4 """ select date_floor('1923-12-31 23:59:59.999999', interval -10 year); """
    // qt_x5 """ select date_floor('0000-01-01 00:00:00', interval 7 minute); """//wrong
    qt_x6 """ select date_floor('0001-01-01 00:00:00', interval 7 minute); """
    qt_x7 """ select date_ceil('9999-12-31 23:59:59.999999', interval 5 minute); """
    qt_x8 """ select date_ceil('9999-12-31 23:59:59.999999', interval 1 second); """
    qt_x9 """ select date_ceil('9999-12-31 23:59:59.999999', interval 100 year); """
    // qt_x10 """ select date_ceil('0000-01-01 23:59:59.999999', interval 7 month); """//wrong
    qt_x11 """ select date_ceil('0001-01-01 23:59:59.999999', interval 7 month); """
    qt_x12 """ select date_ceil('0001-09-01 23:59:59.999999', interval -7 month); """
    qt_x13 """ select date_ceil('0002-02-01 23:59:59.999999', interval -7 month); """
    qt_x14 """ select date_ceil('9999-12-31 23:54:59.999999', interval 5 minute); """

    qt_f_positive_1 """ select date_floor('2023-07-14 15:30:45', interval 3 second); """
    qt_f_positive_2 """ select date_floor('2023-07-14 15:30:45', interval 10 minute); """
    qt_f_positive_3 """ select date_floor('2023-07-14 15:30:45', interval 6 hour); """
    qt_f_positive_4 """ select date_floor('2023-07-14 15:30:45', interval 7 day); """
    qt_f_positive_5 """ select date_floor('2023-07-14 15:30:45', interval 3 month); """
    qt_f_positive_6 """ select date_floor('2023-07-14 15:30:45', interval 2 quarter); """
    qt_f_positive_7 """ select date_floor('2023-07-14 15:30:45', interval 5 year); """
    
    qt_f_neg_1 """ select date_floor('2023-07-14 15:30:45', interval -3 second); """
    qt_f_neg_2 """ select date_floor('2023-07-14 15:30:45', interval -10 minute); """
    qt_f_neg_3 """ select date_floor('2023-07-14 15:30:45', interval -6 hour); """
    qt_f_neg_4 """ select date_floor('2023-07-14 15:30:45', interval -7 day); """
    qt_f_neg_5 """ select date_floor('2023-07-14 15:30:45', interval -3 month); """
    qt_f_neg_6 """ select date_floor('2023-07-14 15:30:45', interval -2 quarter); """
    qt_f_neg_7 """ select date_floor('2023-07-14 15:30:45', interval -5 year); """
    
    qt_c_positive_1 """ select date_ceil('2023-07-14 15:30:45', interval 3 second); """
    qt_c_positive_2 """ select date_ceil('2023-07-14 15:30:45', interval 10 minute); """
    qt_c_positive_3 """ select date_ceil('2023-07-14 15:30:45', interval 6 hour); """
    qt_c_positive_4 """ select date_ceil('2023-07-14 15:30:45', interval 7 day); """
    qt_c_positive_5 """ select date_ceil('2023-07-14 15:30:45', interval 3 month); """
    qt_c_positive_6 """ select date_ceil('2023-07-14 15:30:45', interval 2 quarter); """
    qt_c_positive_7 """ select date_ceil('2023-07-14 15:30:45', interval 5 year); """

    qt_c_neg_1 """ select date_ceil('2023-07-14 15:30:45', interval -3 second); """
    qt_c_neg_2 """ select date_ceil('2023-07-14 15:30:45', interval -10 minute); """
    qt_c_neg_3 """ select date_ceil('2023-07-14 15:30:45', interval -6 hour); """
    qt_c_neg_4 """ select date_ceil('2023-07-14 15:30:45', interval -7 day); """
    qt_c_neg_5 """ select date_ceil('2023-07-14 15:30:45', interval -3 month); """
    qt_c_neg_6 """ select date_ceil('2023-07-14 15:30:45', interval -2 quarter); """
    qt_c_neg_7 """ select date_ceil('2023-07-14 15:30:45', interval -5 year); """
    
    qt_zero_interval_1 """ select date_floor('2023-07-14 15:30:45', interval 0 second); """
    qt_zero_interval_2 """ select date_ceil('2023-07-14 15:30:45', interval 0 minute); """
    
    qt_boundary_1 """ select date_floor('9999-12-31 23:59:58', interval 1 second); """
    qt_boundary_2 """ select date_floor('9999-12-31 22:59:58', interval 1 hour); """
    qt_boundary_3 """ select date_floor('9999-11-17 23:59:58', interval 1 month); """
    qt_boundary_4 """ select date_ceil('9999-12-31 23:59:58', interval 1 second); """
    qt_boundary_5 """ select date_ceil('9999-12-31 21:00:01', interval 1 hour); """
    qt_boundary_6 """ select date_ceil('9999-10-17 23:59:58', interval 1 month); """

    qt_three_param_1 """ select hour_floor('2023-07-14 15:30:45', 3, '2023-01-01 00:00:00'); """
    qt_three_param_2 """ select hour_floor('2023-07-14 15:30:45', 6, '2023-01-01 02:00:00'); """
    qt_three_param_3 """ select hour_floor('2023-07-14 15:30:45', 12, '1970-01-01 00:00:00'); """
    qt_three_param_4 """ select hour_floor('2023-07-14 15:30:45', 24, '2023-01-01 00:00:00'); """

    qt_three_param_5 """ select minute_floor('2023-07-14 15:30:45', 15, '2023-01-01 00:00:00'); """
    qt_three_param_6 """ select minute_floor('2023-07-14 15:30:45', 30, '2023-01-01 00:05:00'); """
    qt_three_param_7 """ select minute_floor('2023-07-14 15:30:45', 60, '1970-01-01 00:00:00'); """

    qt_three_param_8 """ select second_floor('2023-07-14 15:30:45', 10, '2023-01-01 00:00:00'); """
    qt_three_param_9 """ select second_floor('2023-07-14 15:30:45', 30, '2023-01-01 00:00:05'); """
    qt_three_param_10 """ select second_floor('2023-07-14 15:30:45', 60, '1970-01-01 00:00:00'); """

    qt_three_param_11 """ select hour_ceil('2023-07-14 15:30:45', 3, '2023-01-01 00:00:00'); """
    qt_three_param_12 """ select hour_ceil('2023-07-14 15:30:45', 6, '2023-01-01 02:00:00'); """
    qt_three_param_13 """ select hour_ceil('2023-07-14 15:30:45', 12, '1970-01-01 00:00:00'); """

    qt_three_param_14 """ select minute_ceil('2023-07-14 15:30:45', 15, '2023-01-01 00:00:00'); """
    qt_three_param_15 """ select minute_ceil('2023-07-14 15:30:45', 30, '2023-01-01 00:05:00'); """

    qt_three_param_16 """ select second_ceil('2023-07-14 15:30:45', 10, '2023-01-01 00:00:00'); """
    qt_three_param_17 """ select second_ceil('2023-07-14 15:30:45', 30, '2023-01-01 00:00:05'); """

    qt_three_param_18 """ select hour_floor('2023-07-14 15:30:45', 1, '2023-07-14 15:30:45'); """
    qt_three_param_19 """ select minute_floor('2023-07-14 15:30:45', 1, '2023-07-14 15:30:45'); """
    qt_three_param_20 """ select second_floor('2023-07-14 15:30:45', 1, '2023-07-14 15:30:45'); """

    qt_three_param_21 """ select hour_floor('2023-07-14 15:30:45', 2, '2023-07-15 00:00:00'); """
    qt_three_param_22 """ select minute_floor('2023-07-14 15:30:45', 10, '2023-07-14 16:00:00'); """
    qt_three_param_23 """ select second_floor('2023-07-14 15:30:45', 30, '2023-07-14 15:31:00'); """

    qt_three_param_24 """ select hour_floor('2023-07-14 15:30:45', 4, '2020-01-01 00:00:00'); """
    qt_three_param_25 """ select hour_floor('2023-07-14 15:30:45', 8, '2025-01-01 00:00:00'); """

    qt_three_param_26 """ select hour_floor('2023-07-14 15:30:45', 100, '2023-01-01 00:00:00'); """
    qt_three_param_27 """ select minute_floor('2023-07-14 15:30:45', 500, '2023-01-01 00:00:00'); """
    qt_three_param_28 """ select second_floor('2023-07-14 15:30:45', 3600, '2023-01-01 00:00:00'); """

    qt_three_param_29 """ select hour_ceil('2023-07-14 15:00:00', 3, '2023-01-01 00:00:00'); """
    qt_three_param_30 """ select minute_ceil('2023-07-14 15:30:00', 15, '2023-01-01 00:00:00'); """
    qt_three_param_31 """ select second_ceil('2023-07-14 15:30:45', 15, '2023-01-01 00:00:00'); """

    qt_three_param_32 """ select hour_floor('0001-01-01 01:00:00', 2, '0001-01-01 00:00:00'); """
    qt_three_param_33 """ select hour_floor('9999-12-31 23:00:00', 3, '9999-12-31 00:00:00'); """
    qt_three_param_34 """ select minute_floor('0001-01-01 00:01:00', 5, '0001-01-01 00:00:00'); """
    qt_three_param_35 """ select minute_floor('9999-12-31 23:59:00', 10, '9999-12-31 23:00:00'); """
    
    qt_three_param_36 """ select day_floor('2023-07-14 15:30:45', 3, '2023-01-01 00:00:00'); """
    qt_three_param_37 """ select day_floor('2023-07-14 15:30:45', 7, '2023-07-01 00:00:00'); """
    qt_three_param_38 """ select day_floor('2023-07-14 15:30:45', 15, '1970-01-01 00:00:00'); """
    qt_three_param_39 """ select day_floor('2023-07-14 15:30:45', 30, '2023-01-15 00:00:00'); """
    
    qt_three_param_40 """ select day_ceil('2023-07-14 15:30:45', 3, '2023-01-01 00:00:00'); """
    qt_three_param_41 """ select day_ceil('2023-07-14 15:30:45', 7, '2023-07-01 00:00:00'); """
    qt_three_param_42 """ select day_ceil('2023-07-14 15:30:45', 15, '1970-01-01 00:00:00'); """
    qt_three_param_43 """ select day_ceil('2023-07-14 15:30:45', 30, '2023-01-15 00:00:00'); """
    
    qt_three_param_44 """ select month_floor('2023-07-14 15:30:45', 2, '2023-01-01 00:00:00'); """
    qt_three_param_45 """ select month_floor('2023-07-14 15:30:45', 3, '2023-02-01 00:00:00'); """
    qt_three_param_46 """ select month_floor('2023-07-14 15:30:45', 6, '1970-01-01 00:00:00'); """
    qt_three_param_47 """ select month_floor('2023-07-14 15:30:45', 12, '2022-06-01 00:00:00'); """
    
    qt_three_param_48 """ select month_ceil('2023-07-14 15:30:45', 2, '2023-01-01 00:00:00'); """
    qt_three_param_49 """ select month_ceil('2023-07-14 15:30:45', 3, '2023-02-01 00:00:00'); """
    qt_three_param_50 """ select month_ceil('2023-07-14 15:30:45', 6, '1970-01-01 00:00:00'); """
    qt_three_param_51 """ select month_ceil('2023-07-14 15:30:45', 12, '2022-06-01 00:00:00'); """

    qt_three_param_52 """ select year_floor('2023-07-14 15:30:45', 2, '2020-01-01 00:00:00'); """
    qt_three_param_53 """ select year_floor('2023-07-14 15:30:45', 5, '2021-01-01 00:00:00'); """
    qt_three_param_54 """ select year_floor('2023-07-14 15:30:45', 10, '1970-01-01 00:00:00'); """
    qt_three_param_55 """ select year_floor('2023-07-14 15:30:45', 100, '1900-01-01 00:00:00'); """
    
    qt_three_param_56 """ select year_ceil('2023-07-14 15:30:45', 2, '2020-01-01 00:00:00'); """
    qt_three_param_57 """ select year_ceil('2023-07-14 15:30:45', 5, '2021-01-01 00:00:00'); """
    qt_three_param_58 """ select year_ceil('2023-07-14 15:30:45', 10, '1970-01-01 00:00:00'); """
    qt_three_param_59 """ select year_ceil('2023-07-14 15:30:45', 100, '1900-01-01 00:00:00'); """

    qt_three_param_60 """ select quarter_floor('2023-07-14 15:30:45', 1, '2023-01-01 00:00:00'); """
    qt_three_param_61 """ select quarter_floor('2023-07-14 15:30:45', 2, '2023-01-01 00:00:00'); """
    qt_three_param_62 """ select quarter_floor('2023-07-14 15:30:45', 1, '2022-10-01 00:00:00'); """
    qt_three_param_63 """ select quarter_floor('2023-11-20 10:15:30', 1, '2023-01-01 00:00:00'); """
    qt_three_param_64 """ select quarter_floor('2023-11-20 10:15:30', 2, '2022-07-01 00:00:00'); """
    qt_three_param_65 """ select quarter_floor('2023-02-28 12:00:00', 1, '2023-01-01 00:00:00'); """
    qt_three_param_66 """ select quarter_floor('2023-05-15 18:45:00', 1, '2023-04-01 00:00:00'); """
    qt_three_param_67 """ select quarter_floor('2023-08-10 09:30:00', 3, '2020-01-01 00:00:00'); """
    qt_three_param_68 """ select quarter_floor('2023-12-31 23:59:59', 1, '2023-10-01 00:00:00'); """
    qt_three_param_69 """ select quarter_floor('2024-01-01 00:00:00', 2, '2023-01-01 00:00:00'); """
    qt_three_param_70 """ select quarter_floor('2023-07-14 15:30:45', 4, '1970-01-01 00:00:00'); """
    qt_three_param_71 """ select quarter_floor('2023-07-14 15:30:45', 1, '2023-07-01 00:00:00'); """

    qt_three_param_72 """ select quarter_ceil('2023-07-14 15:30:45', 1, '2023-01-01 00:00:00'); """
    qt_three_param_73 """ select quarter_ceil('2023-07-14 15:30:45', 2, '2023-01-01 00:00:00'); """
    qt_three_param_74 """ select quarter_ceil('2023-07-14 15:30:45', 1, '2022-10-01 00:00:00'); """
    qt_three_param_75 """ select quarter_ceil('2023-11-20 10:15:30', 1, '2023-01-01 00:00:00'); """
    qt_three_param_76 """ select quarter_ceil('2023-11-20 10:15:30', 2, '2022-07-01 00:00:00'); """
    qt_three_param_77 """ select quarter_ceil('2023-02-28 12:00:00', 1, '2023-01-01 00:00:00'); """
    qt_three_param_78 """ select quarter_ceil('2023-05-15 18:45:00', 1, '2023-04-01 00:00:00'); """
    qt_three_param_79 """ select quarter_ceil('2023-08-10 09:30:00', 3, '2020-01-01 00:00:00'); """
    qt_three_param_80 """ select quarter_ceil('2023-12-31 23:59:59', 1, '2023-10-01 00:00:00'); """
    qt_three_param_81 """ select quarter_ceil('2024-01-01 00:00:00', 2, '2023-01-01 00:00:00'); """
    qt_three_param_82 """ select quarter_ceil('2023-07-14 15:30:45', 4, '1970-01-01 00:00:00'); """
    qt_three_param_83 """ select quarter_ceil('2023-07-14 15:30:45', 1, '2023-07-01 00:00:00'); """

    qt_three_param_84 """ select quarter_floor('2023-01-01 00:00:00', 1, '2023-01-01 00:00:00'); """
    qt_three_param_85 """ select quarter_ceil('2023-01-01 00:00:00', 1, '2023-01-01 00:00:00'); """
    qt_three_param_86 """ select quarter_floor('2023-03-31 23:59:59', 1, '2023-01-01 00:00:00'); """
    qt_three_param_87 """ select quarter_ceil('2023-04-01 00:00:01', 1, '2023-01-01 00:00:00'); """
    qt_three_param_88 """ select quarter_floor('9999-12-31 23:59:59', 1, '9999-10-01 00:00:00'); """
    qt_three_param_89 """ select quarter_ceil('0001-01-01 00:00:00', 1, '0001-01-01 00:00:00'); """
    qt_three_param_90 """ select quarter_floor('2023-06-30 23:59:59', 2, '2022-01-01 00:00:00'); """
    qt_three_param_91 """ select quarter_ceil('2023-07-01 00:00:01', 2, '2022-01-01 00:00:00'); """
}
