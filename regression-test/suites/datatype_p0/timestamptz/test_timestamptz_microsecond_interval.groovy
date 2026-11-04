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

suite("test_timestamptz_microsecond_interval") {
    sql "set time_zone = '+08:00'"

    sql "DROP TABLE IF EXISTS test_timestamptz_microsecond_interval_result"
    sql "DROP TABLE IF EXISTS test_timestamptz_microsecond_interval_input"

    sql """
        CREATE TABLE test_timestamptz_microsecond_interval_input (
            id INT,
            tz0 TIMESTAMPTZ(0),
            tz3 TIMESTAMPTZ(3),
            tz6 TIMESTAMPTZ(6)
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """
    sql """
        INSERT INTO test_timestamptz_microsecond_interval_input VALUES
        (1,
         CAST('2024-01-02 03:04:05.123456+08:00' AS TIMESTAMPTZ(0)),
         CAST('2024-01-02 03:04:05.123456+08:00' AS TIMESTAMPTZ(3)),
         CAST('2024-01-02 03:04:05.123456+08:00' AS TIMESTAMPTZ(6)))
    """

    def operations = [
        [name: 'day_microsecond', unit: 'DAY_MICROSECOND', interval: "'1 02:03:04.123456'"],
        [name: 'hour_microsecond', unit: 'HOUR_MICROSECOND', interval: "'02:03:04.123456'"],
        [name: 'minute_microsecond', unit: 'MINUTE_MICROSECOND', interval: "'03:04.123456'"],
        [name: 'second_microsecond', unit: 'SECOND_MICROSECOND', interval: "'04.123456'"],
        [name: 'microseconds', function: 'microseconds', interval: '1'],
        [name: 'milliseconds', function: 'milliseconds', interval: '1']
    ]
    def projections = []
    for (int scale : [0, 3, 6]) {
        for (def operation : operations) {
            for (String op : ['add', 'sub']) {
                def expression = operation.function
                        ? "${operation.function}_${op}(tz${scale}, ${operation.interval})"
                        : "date_${op}(tz${scale}, INTERVAL ${operation.interval} ${operation.unit})"
                projections.add("${expression} AS ${operation.name}_${op}_p${scale}")
            }
        }
    }
    sql """
        CREATE TABLE test_timestamptz_microsecond_interval_result
        PROPERTIES ('replication_num' = '1') AS
        SELECT id, ${projections.join(',\n')}
        FROM test_timestamptz_microsecond_interval_input
    """

    order_qt_result_types """
        SELECT column_name, column_type
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'test_timestamptz_microsecond_interval_result'
        ORDER BY ordinal_position
    """

    order_qt_results """
        SELECT * FROM test_timestamptz_microsecond_interval_result ORDER BY id
    """
}
