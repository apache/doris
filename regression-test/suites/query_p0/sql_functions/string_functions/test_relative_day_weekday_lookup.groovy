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

suite("test_relative_day_weekday_lookup") {
    sql "DROP TABLE IF EXISTS relative_day_weekday_lookup"
    sql """CREATE TABLE relative_day_weekday_lookup (
        id INT,
        d DATE NOT NULL,
        ts DATETIME NOT NULL,
        weekday STRING NOT NULL,
        null_date DATE NULL
    ) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num" = "1")"""

    def weekdays = ["MO", "mon", "MoNdAy", "TU", "tue", "TuEsDaY", "WE", "wed", "WeDnEsDaY",
                    "TH", "thu", "ThUrSdAy", "FR", "fri", "FrIdAy", "SA", "sat", "SaTuRdAy",
                    "SU", "sun", "SuNdAy"]
    def values = weekdays.withIndex().collect { weekday, index ->
        "(${index}, '2024-01-01', '2024-01-01 12:34:56', '${weekday}', NULL)"
    }.join(",")
    sql "INSERT INTO relative_day_weekday_lookup VALUES ${values}"

    qt_aliases """SELECT id, next_day(d, weekday), previous_day(d, weekday),
        next_day(ts, weekday), previous_day(ts, weekday)
        FROM relative_day_weekday_lookup ORDER BY id"""
    qt_constant_weekdays """SELECT id, next_day(d, 'mOnDaY'), previous_day(d, 'wEdNeSdAy'),
        next_day(ts, nullable('sUn')), previous_day(ts, nullable('tH'))
        FROM relative_day_weekday_lookup ORDER BY id"""
    qt_nullable_dates """SELECT id, next_day(if(id % 2 = 0, NULL, d), weekday),
        previous_day(if(id % 2 = 0, NULL, d), weekday)
        FROM relative_day_weekday_lookup ORDER BY id"""
    qt_nullable_weekdays """SELECT id, next_day(d, if(id % 2 = 0, NULL, weekday)),
        previous_day(d, if(id % 2 = 0, NULL, weekday))
        FROM relative_day_weekday_lookup ORDER BY id"""
    qt_null_before_invalid """SELECT id, next_day(null_date, 'MONDDY'),
        previous_day(null_date, 'MONDDY')
        FROM relative_day_weekday_lookup ORDER BY id"""
    qt_empty """SELECT next_day(d, 'MONDDY'), previous_day(d, 'MONDDY')
        FROM relative_day_weekday_lookup WHERE id < 0 ORDER BY id"""

    for (def functionName : ["next_day", "previous_day"]) {
        for (def invalid : ["", "M", "MOND", "MONDDY", "MONDAYX", "MO ", " MON", "SO",
                            "TUES", "THUR", "WEDNESDA", "SATURDAX", "SUNDAX", "WEDNESDAYZ"]) {
            test {
                sql "SELECT ${functionName}(d, '${invalid}') FROM relative_day_weekday_lookup"
                exception "Function ${functionName} failed to parse weekday: ${invalid}"
            }
        }
        test {
            sql """SELECT ${functionName}(d, concat('MON', char(0), 'AY'))
                FROM relative_day_weekday_lookup"""
            exception "Function ${functionName} failed to parse weekday:"
        }
        test {
            sql """SELECT ${functionName}(d, if(id = 0, 'MON', 'MONDDY'))
                FROM relative_day_weekday_lookup"""
            exception "Function ${functionName} failed to parse weekday: MONDDY"
        }
        test {
            sql """SELECT ${functionName}(if(id = 0, NULL, d), 'MONDDY')
                FROM relative_day_weekday_lookup"""
            exception "Function ${functionName} failed to parse weekday: MONDDY"
        }
    }
}
