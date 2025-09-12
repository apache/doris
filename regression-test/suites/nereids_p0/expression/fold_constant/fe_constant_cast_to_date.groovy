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

suite("fe_constant_cast_to_date") {
    sql """set enable_nereids_planner=true"""
    sql """set enable_fallback_to_original_planner=false"""
    sql """set debug_skip_fold_constant=false"""
    sql """set enable_strict_cast=true"""

    // *******************TEST OF STRICT MODE*************************

    qt_date1("""select cast("2023-07-16T19:20:30.123+08:00" as date)""")
    qt_date2("""select cast("2023-07-16T19+08:00" as date)""")
    qt_date3("""select cast("2023-07-16T1920+08:00" as date)""")
    qt_date4("""select cast("70-1-1T00:00:00-0000" as date)""")
    qt_date5("""select cast("19991231T235959.5UTC" as date)""")
    qt_date6("""select cast("2024-02-29 12:00:00 Europe/Paris" as date)""")
    qt_date7("""select cast("2024-05-01T00:00Asia/Shanghai" as date)""")
    qt_date8("""select cast("20231005T081530Europe/London" as date)""")
    qt_date9("""select cast("85-12-25T0000gMt" as date)""")
    qt_date10("""select cast("2024-05-01" as date)""")
    qt_date11("""select cast("24-5-1" as date)""")
    qt_date12("""select cast("2024-05-01 0:1:2.333" as date)""")
    qt_date13("""select cast("2024-05-01 0:1:2." as date)""")
    qt_date14("""select cast("20240501 01" as date)""")
    qt_date15("""select cast("20230716 1920Z" as date)""")
    qt_date16("""select cast("20240501T0000" as date)""")
    qt_date17("""select cast("2024-12-31 23:59:59.9999999" as date)""")
    qt_date18("""select cast("20250615T00:00:00.99999999999999" as date)""")
    qt_date19("""select cast("2020-12-12 13:12:12-03:00" as date)""")
    qt_date20("""select cast("0023-01-01T00:00Z" as date)""")
    qt_date21("""select cast("69-12-31" as date)""")
    qt_date22("""select cast("70-01-01" as date)""")
    qt_date23("""select cast("230102" as date)""")
    qt_date24("""select cast("19230101" as date)""")
    qt_date25("""select cast("120102 030405" as date)""")
    qt_date26("""select cast("20120102 030405" as date)""")
    qt_date27("""select cast("120102 030405.999" as date)""")
    qt_date28("""select cast("2020-05-05 12:30:59" as date)""")

    qt_date29("""select cast(123.123 as date)""")
    qt_date30("""select cast(20150102030405 as date)""")
    qt_date31("""select cast(20150102030405.123456 as date)""")
    qt_date32("""select cast(20151231235959.99999999999 as date)""")

    test {
        sql """select cast("2023-07-16T19.123+08:00" as date)"""
        exception "can't cast to DATETIMEV2"
    }
    test {
        sql """select cast("2024/05/01" as date)"""
        exception "can't cast to DATETIMEV2"
    }
    test {
        sql """select cast("24012" as date)"""
        exception "can't cast to DATETIMEV2"
    }
    test {
        sql """select cast("2411 123" as date)"""
        exception "can't cast to DATETIMEV2"
    }
    test {
        sql """select cast("2024-05-01 01:030:02" as date)"""
        exception "can't cast to DATETIMEV2"
    }
    test {
        sql """select cast("10000-01-01 00:00:00" as date)"""
        exception "can't cast to DATETIMEV2"
    }
    test {
        sql """select cast("2024-0131T12:00" as date)"""
        exception "can't cast to DATETIMEV2"
    }
    test {
        sql """select cast("2024-05-01@00:00" as date)"""
        exception "can't cast to DATETIMEV2"
    }
    test {
        sql """select cast("20120212051" as date)"""
        exception "can't cast to DATETIMEV2"
    }
    test {
        sql """select cast("2024-05-01T00:00XYZ" as date)"""
        exception ""
    }
    test {
        sql """select cast("2024-5-1T24:00" as date)"""
        exception "date/datetime literal [2024-5-1T24:00:00] is invalid"
    }
    test {
        sql """select cast("2024-02-30" as date)"""
        exception "date/datetime literal [2024-02-30T00:00:00] is invalid"
    }
    test {
        sql """select cast("2024-05-01T12:60" as date)"""
        exception "date/datetime literal [2024-05-01T12:60:00] is invalid"
    }
    test {
        sql """select cast("2012-06-30T23:59:60" as date)"""
        exception "date/datetime literal [2012-06-30T23:59:60] is invalid"
    }
    test {
        sql """select cast("2024-05-01T00:00+14:30" as date)"""
        exception "Time zone offset couldn't be larger than 14:00"
    }
    test {
        sql """select cast("2024-05-01T00:00+08:25" as date)"""
        exception "can't cast to DATETIMEV2"
    }
    test {
        sql """select cast(1000 as date)"""
        exception " date/datetime literal [2000-10-0 0:0:0] is invalid"
    }
    test {
        sql """select cast(-123.123 as date)"""
        exception "-123.123 can't cast to DATEV2 in strict mode"
    }

    // *******************TEST OF NON STRICT MODE*************************
    sql """set enable_strict_cast=false"""

    qt_date1("""select cast("2023-07-16T19:20:30.123+08:00" as date)""")
    qt_date2("""select cast("2023-07-16T19+08:00" as date)""")
    qt_date3("""select cast("2023-07-16T1920+08:00" as date)""")
    qt_date4("""select cast("70-1-1T00:00:00-0000" as date)""")
    qt_date5("""select cast("19991231T235959.5UTC" as date)""")
    qt_date6("""select cast("2024-02-29 12:00:00 Europe/Paris" as date)""")
    qt_date7("""select cast("2024-05-01T00:00Asia/Shanghai" as date)""")
    qt_date8("""select cast("20231005T081530Europe/London" as date)""")
    qt_date9("""select cast("85-12-25T0000gMt" as date)""")
    qt_date10("""select cast("2024-05-01" as date)""")
    qt_date11("""select cast("24-5-1" as date)""")
    qt_date12("""select cast("2024-05-01 0:1:2.333" as date)""")
    qt_date13("""select cast("2024-05-01 0:1:2." as date)""")
    qt_date14("""select cast("20240501 01" as date)""")
    qt_date15("""select cast("20230716 1920Z" as date)""")
    qt_date16("""select cast("20240501T0000" as date)""")
    qt_date17("""select cast("2024-12-31 23:59:59.9999999" as date)""")
    qt_date18("""select cast("20250615T00:00:00.99999999999999" as date)""")
    qt_date19("""select cast("2020-12-12 13:12:12-03:00" as date)""")
    qt_date20("""select cast("0023-01-01T00:00Z" as date)""")
    qt_date21("""select cast("69-12-31" as date)""")
    qt_date22("""select cast("70-01-01" as date)""")
    qt_date23("""select cast("230102" as date)""")
    qt_date24("""select cast("19230101" as date)""")
    qt_date25("""select cast("120102 030405" as date)""")
    qt_date26("""select cast("20120102 030405" as date)""")
    qt_date27("""select cast("120102 030405.999" as date)""")
    qt_date28("""select cast("2020-05-05 12:30:59" as date)""")
    qt_date29("""select cast(" 2023-7-4T9-5-3.1Z " as date)""")
    qt_date30("""select cast("99.12.31 23.59.59+05:30" as date)""")
    qt_date31("""select cast("2000/01/01T00/00/00-230" as date)""")
    qt_date32("""select cast("85 1 1T0 0 0. Z" as date)""")
    qt_date33("""select cast("2024-02-29T23:59:59.999999 UTC" as date)""")
    qt_date34("""select cast("70-01-01T00:00:00+14" as date)""")
    qt_date35("""select cast("0023-1-1T1:2:3. -00:00" as date)""")
    qt_date36("""select cast("2025/06/15T00:00:00.0-0" as date)""")
    qt_date37("""select cast("2025/06/15T00:00:00.99999999999" as date)""")

    qt_date38("""select cast(123.123 as date)""")
    qt_date39("""select cast(20150102030405 as date)""")
    qt_date40("""select cast(20150102030405.123456 as date)""")
    qt_date41("""select cast(20151231235959.99999999999 as date)""")

    qt_date42("""select cast("2024-02-29T23-59-60ZULU" as date)""")
    qt_date43("""select cast("2024 12 31T121212.123456 America/New_York" as date)""")
    qt_date44("""select cast("123.123" as date)""")
    qt_date45("""select cast("123" as date)""")
    qt_date46("""select cast(1000 as date)""")
    qt_date47("""select cast(-123.123 as date)""")

    qt_date48("""select cast("2020-12-12:12:12:12" as date)""")
}
