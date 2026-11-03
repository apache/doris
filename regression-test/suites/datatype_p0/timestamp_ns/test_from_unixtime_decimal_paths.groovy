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

suite("test_from_unixtime_decimal_paths") {
    sql "set time_zone = '+00:00'"
    sql "drop table if exists timestamp_ns_from_unixtime_decimal_paths"
    sql """
        create table timestamp_ns_from_unixtime_decimal_paths (
            id int,
            d18_6 decimal(18, 6) null,
            d21_9 decimal(21, 9) null,
            d10_3 decimal(10, 3) null,
            d18_9 decimal(18, 9) null,
            dv2_27_9 decimalv2(27, 9) null
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_from_unixtime_decimal_paths values
        (1, 0.999999, 0.999999500, 0.999, 0.999999500, 0.999999500),
        (2, null, null, null, null, null)
    """

    order_qt_decimal_column_paths """
        select id,
               from_unixtime(d18_6, '%s.%f'),
               from_unixtime(d21_9, '%s.%f'),
               from_unixtime(d10_3, '%s.%f'),
               from_unixtime(d18_9, '%s.%n'),
               from_unixtime(dv2_27_9, '%s.%n')
        from timestamp_ns_from_unixtime_decimal_paths
        order by id
    """
}
