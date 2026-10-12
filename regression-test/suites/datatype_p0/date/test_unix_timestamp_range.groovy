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

suite("test_unix_timestamp_range") {
    sql "DROP TABLE IF EXISTS test_unix_timestamp_range_values"
    sql """
        CREATE TABLE test_unix_timestamp_range_values (
            id INT,
            seconds BIGINT,
            milliseconds BIGINT,
            microseconds BIGINT
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    // The first two inputs reach civil years 67506 and 133042. Both years used
    // to wrap to 1970 when passed to the uint16_t date setter.
    def invalidValues = [
        [2068116364800L, 2068116364800000L, 2068116364800000000L],
        [4136232816000L, 4136232816000000L, 4136232816000000000L],
        [Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE]
    ]
    invalidValues.eachWithIndex { values, index ->
        sql """INSERT INTO test_unix_timestamp_range_values
               VALUES (${index}, ${values[0]}, ${values[1]}, ${values[2]})"""
    }
    sql "INSERT INTO test_unix_timestamp_range_values VALUES (3, NULL, NULL, NULL)"

    def functions = [
        ["from_unixtime", "seconds", 0, ""],
        ["from_unixtime", "seconds", 0, ", '%Y-%m-%d %H:%i:%s'"],
        ["from_second", "seconds", 0, ""],
        ["from_millisecond", "milliseconds", 1, ""],
        ["from_microsecond", "microseconds", 2, ""]
    ]
    def zones = [
        ["+00:00", 253402300799L],
        ["+14:00", 253402250399L],
        ["-12:00", 253402343999L]
    ]
    zones.eachWithIndex { zone, zoneIndex ->
        sql "SET time_zone = '${zone[0]}'"
        [false, true].each { skipFold ->
            sql "SET debug_skip_fold_constant = ${skipFold}"
            invalidValues.eachWithIndex { values, index ->
                functions.each { function ->
                    def name = function[0]
                    def argument = function[1]
                    def value = values[function[2]]
                    def format = function[3]
                    test {
                        sql "SELECT ${name}(${value}${format})"
                        exception "Operation ${name}"
                    }
                    test {
                        sql """SELECT ${name}(${argument}${format})
                               FROM test_unix_timestamp_range_values WHERE id = ${index}"""
                        exception "Operation ${name}"
                    }
                }
            }

            // The exact upper bound depends on the session time zone. In -12:00,
            // a valid local date can have a UTC timestamp beyond the UTC year boundary.
            def lastSecond = zone[1]
            "order_qt_upper_${zoneIndex}_${skipFold}" """
                SELECT from_unixtime(${lastSecond}),
                       from_unixtime(CAST('${lastSecond}.999999' AS DECIMAL(18,6))),
                       from_second(${lastSecond}),
                       from_millisecond(${lastSecond * 1000L + 999L}),
                       from_microsecond(${lastSecond * 1000000L + 999999L})
            """
            functions.each { function ->
                def nextSecond = lastSecond + 1L
                def ratios = [1L, 1000L, 1000000L]
                test {
                    sql "SELECT ${function[0]}(${nextSecond * ratios[function[2]]}${function[3]})"
                    exception "Operation ${function[0]}"
                }
            }
        }
        "qt_null_${zoneIndex}" """
            SELECT from_unixtime(seconds), from_second(seconds),
                   from_millisecond(milliseconds), from_microsecond(microseconds)
            FROM test_unix_timestamp_range_values WHERE id = 3 ORDER BY id
        """
    }
}
