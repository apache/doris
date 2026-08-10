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

suite("test_timestamp_ns_complex_type") {
    sql "drop table if exists timestamp_ns_complex_type"
    sql """
        create table timestamp_ns_complex_type (
            id int,
            dt_array array<timestamp_ns>,
            dt_map map<string, timestamp_ns>,
            dt_struct struct<minimum:timestamp_ns, maximum:timestamp_ns>,
            dt_json json
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_complex_type values
        (1,
         array(
             cast('1677-09-21 00:12:43.145224192' as timestamp_ns),
             cast('1970-01-01 00:00:00.123456789' as timestamp_ns),
             cast('2262-04-11 23:47:16.854775807' as timestamp_ns)),
         map(
             'before', cast('1969-12-31 23:59:59.999999999' as timestamp_ns),
             'after', cast('1970-01-01 00:00:00.000000001' as timestamp_ns)),
         named_struct(
             'minimum', cast('1677-09-21 00:12:43.145224192' as timestamp_ns),
             'maximum', cast('2262-04-11 23:47:16.854775807' as timestamp_ns)),
         json_object('dt', '1970-01-01 00:00:00.123456789')),
        (2, null, null, null, null)
    """

    order_qt_complex_storage """
        select id, dt_array, dt_map, dt_struct, dt_json
        from timestamp_ns_complex_type
        order by id
    """
    order_qt_json_round_trip """
        select cast(json_extract_string(dt_json, '\$.dt') as timestamp_ns)
        from timestamp_ns_complex_type
        where dt_json is not null
        order by id
    """
}
