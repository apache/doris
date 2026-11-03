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

suite("test_timezone_parser_contract") {
    sql "DROP TABLE IF EXISTS timezone_parser_contract_inputs"
    sql """CREATE TABLE timezone_parser_contract_inputs (id INT, v STRING)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num'='1')"""

    def suffixes = ["+00:00", "-00:30", "+14:00", "-12:00", "-13:00", "-14:00",
                    "+8", "+08", "+0800", "+8:30", "+08:45", "+14:30", "-14:45",
                    "+08:01", "+08:15", "+08:60", "+24:00", "UTC", "utc", "GMT", "Z",
                    "zulu", "CST", "cst", "Asia/Shanghai", "asia/shanghai", "Etc/GMT+3",
                    "America/Port-au-Prince", "America/Does_not_exist", "UTC+08:01",
                    "GMT-13:00", "UTC+8", "+08:05:43", "-14:21", "+08:00:00", "+18:00", "+18:01",
                    "+19:00", "GMT0", "EST5EDT", "W-SU",
                    "+00:00:00", "-00:00:00", "UTC8", "GMT8:30", "UTC0", "GMT0:00"]
    def inputs = suffixes.collect { "2024-01-02 12:00:00 ${it}" }
    inputs.addAll(["1900-01-01 08:05:43 CST", "1900-01-01 08:05:43 Asia/Shanghai",
                   "1900-01-01 08:05:43+08:05:43", "20240102120000+0800", "20240102T120000+8:30",
                   "20240102120000UTC", "20240102T120000UTC"])
    inputs.add(null)
    sql "INSERT INTO timezone_parser_contract_inputs VALUES " + inputs.withIndex().collect { v, id ->
        "(${id}, ${v == null ? 'NULL' : "'${v}'"})"
    }.join(",")
    sql "SET enable_sql_cache=false"
    sql "SET time_zone='+00:00'"
    sql "SET enable_strict_cast=false"

    // Literal folding requests, folding disabled, stored slots and Lambda inputs.
    [[false, false], [false, true], [true, false]].eachWithIndex { settings, path ->
        sql "SET enable_fold_constant_by_be=${settings[0]}"
        sql "SET debug_skip_fold_constant=${settings[1]}"
        ["DATETIME(6)", "TIMESTAMPTZ(6)"].eachWithIndex { type, typeId ->
            inputs.eachWithIndex { value, id ->
                def literal = value == null ? "NULL" : "'${value}'"
                "qt_literal_${path}_${typeId}_${id}"("SELECT CAST(${literal} AS ${type})")
            }
        }
    }
    sql "SET enable_fold_constant_by_be=false"
    sql "SET debug_skip_fold_constant=true"
    ["DATETIME(6)", "TIMESTAMPTZ(6)"].eachWithIndex { type, typeId ->
        "qt_slots_${typeId}"("SELECT id, CAST(v AS ${type}) FROM timezone_parser_contract_inputs ORDER BY id")
        "qt_lambda_${typeId}"("SELECT id, array_map(x -> CAST(x AS ${type}), ARRAY(v)) FROM timezone_parser_contract_inputs ORDER BY id")
    }
    sql "SET time_zone='+08:00'"
    [false, true].eachWithIndex { skip, path ->
        sql "SET debug_skip_fold_constant=${skip}"
        ["+0800", "America/Port-au-Prince", "Etc/GMT+3"].eachWithIndex { suffix, id ->
            "qt_session_literal_${path}_${id}"("SELECT CAST('2024-01-02 12:00:00 ${suffix}' AS TIMESTAMPTZ(6))")
        }
    }
    sql "SET debug_skip_fold_constant=true"
    qt_session_slots "SELECT id, CAST(v AS TIMESTAMPTZ(6)) FROM timezone_parser_contract_inputs ORDER BY id"

    ["utc", "Z", "Zulu", "cst", "Asia/Shanghai", "UTC+8", "GMT-6", "+14:00", "-12:00"].eachWithIndex { zone, id ->
        sql "SET time_zone='${zone}'"
        "qt_session_zone_${id}"("SELECT @@time_zone, CAST('2024-01-02 12:00:00+00:00' AS DATETIME(6))")
    }
    sql "SET time_zone='+00:00'"
    ["UTC8", "GMT8:30", "UTC0", "GMT0:00", "+00:00:00", "-00:00:00"].each { zone ->
        test {
            sql "SET time_zone='${zone}'"
            exception "Unknown or incorrect time zone"
        }
    }
    [false, true].eachWithIndex { skip, path ->
        sql "SET debug_skip_fold_constant=${skip}"
        ["", "UTC", "GMT", "utc", "gmt"].eachWithIndex { prefix, prefixId ->
            ["+14:00", "-12:00", "+08:01", "+08:15", "-00:30"].eachWithIndex { offset, id ->
                "qt_convert_${path}_${prefixId}_${id}"("SELECT CONVERT_TZ(CAST('2024-01-02 12:00:00.123456' AS DATETIME(6)), '${prefix}${offset}', 'UTC')")
            }
            ["+14:01", "+14:30", "+14:45", "-12:01", "-12:30", "-12:45", "+15:00", "-13:00", "+08:60"].each { offset ->
                test {
                    sql "SELECT CONVERT_TZ(CAST('2024-01-02 12:00:00' AS DATETIME(6)), '${prefix}${offset}', 'UTC')"
                    exception "timezone"
                }
                test {
                    sql "SET time_zone='${prefix}${offset}'"
                    exception "Unknown or incorrect time zone"
                }
            }
        }
    }
    sql "SET debug_skip_fold_constant=true"
    test {
        sql "SELECT CONVERT_TZ(CAST('2024-01-02 12:00:00' AS DATETIME(6)), split_part(v, ' ', 3), 'UTC') FROM timezone_parser_contract_inputs WHERE id=11"
        exception "timezone"
    }
    sql "SET debug_skip_fold_constant=true"
    sql "SET enable_strict_cast=true"
    ["-14:00", "+8:30", "UTC+8"].eachWithIndex { suffix, id ->
        "qt_strict_${id}"("SELECT CAST('2024-01-02 12:00:00 ${suffix}' AS DATETIME(6))")
    }
    ["+14:30", "+08:01", "+08:60", "+00:00:00", "UTC8", "GMT8:30"].each { suffix ->
        test {
            sql "SELECT CAST('2024-01-02 12:00:00 ${suffix}' AS DATETIME(6))"
            exception "parse"
        }
    }
    [false, true].eachWithIndex { skip, path ->
        sql "SET debug_skip_fold_constant=${skip}"
        ["+18:00", "-18:00", "+08:05:43", "20240102120000UTC"].eachWithIndex { suffix, id ->
            def value = suffix.startsWith("2024") ? suffix : "2024-01-02 12:00:00 ${suffix}"
            "qt_strict_timestamptz_${path}_${id}"("SELECT CAST('${value}' AS TIMESTAMPTZ(6))")
        }
        ["+18:01", "-18:01", "+18:00:01", "-18:00:01", "+19:00", "-19:00"].each { suffix ->
            test {
                sql "SELECT CAST('2024-01-02 12:00:00 ${suffix}' AS TIMESTAMPTZ(6))"
                exception "parse"
            }
        }
    }
}
