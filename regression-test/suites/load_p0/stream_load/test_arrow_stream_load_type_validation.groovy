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

suite("test_arrow_stream_load_type_validation", "p0") {
    sql "set enable_decimal256 = true"
    def backendIps = [:]
    def backendPorts = [:]
    getBackendIpHttpPort(backendIps, backendPorts)
    // Pin every failed load and its recovery load to the same BE, without HTTP retries.
    def backendId = backendIps.keySet().first()
    def backendIp = backendIps[backendId]
    def backendPort = backendPorts[backendId] as int

    def load = { tableName, fixture, expectedError, typeError = true ->
        streamLoad {
            table tableName
            directToBe backendIp, backendPort
            retryIfHttpError false
            set 'format', 'arrow'
            set 'columns', 'k,v'
            set 'strict_mode', 'true'
            set 'max_filter_ratio', '0'
            file "arrow_type_validation/${fixture}.arrow"
            time 30000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def response = parseJson(result)
                if (expectedError != null) {
                    assertEquals('Fail', response.Status)
                    if (typeError) {
                        assertTrue(response.Message.contains("Arrow column 'v'"), response.Message)
                        assertTrue(response.Message.contains('->'), response.Message)
                    }
                    assertTrue(response.Message.toLowerCase().contains(expectedError.toLowerCase()), response.Message)
                } else {
                    assertEquals('Success', response.Status)
                    assertEquals(0, response.NumberFilteredRows)
                }
            }
        }
    }

    // Different decimal storage widths share the decimal128 wire representation.
    def cases = [
        [name: 'bool', type: 'BOOLEAN', good: 'bool', bad: 'bad_bool', reason: 'boolean',
            conversions: ['int32'], late: 'bool_late_failure'],
        [name: 'decimal32', type: 'DECIMAL(9,2)', good: 'decimal128', bad: 'bad_decimal',
            reason: 'decimal', conversions: ['decimal_string', 'decimal256_good'],
            late: 'decimal_late_failure'],
        [name: 'decimal64', type: 'DECIMAL(18,2)', good: 'decimal128', bad: 'bad_decimal',
            reason: 'decimal', conversions: ['decimal_string', 'int32', 'decimal256_good'],
            late: 'decimal_late_failure'],
        [name: 'decimal128', type: 'DECIMAL(38,2)', good: 'decimal128', bad: 'bad_decimal',
            reason: 'decimal', conversions: ['decimal_string', 'int32', 'decimal256_good'],
            late: 'decimal_late_failure'],
        [name: 'decimal256', type: 'DECIMAL(76,2)', good: 'decimal256', bad: 'bad_decimal',
            reason: 'decimal', conversions: ['decimal_string', 'int32', 'decimal128_good'],
            late: 'decimal_late_failure'],
        [name: 'array', type: 'ARRAY<BOOLEAN>', good: 'array', bad: 'bad_array',
            reason: 'boolean', conversions: ['array_nested_null']],
        [name: 'map', type: 'MAP<STRING,BOOLEAN>', good: 'map', bad: 'bad_map',
            reason: 'boolean', conversions: ['map_nested_null']],
        [name: 'struct', type: 'STRUCT<b:BOOLEAN>', good: 'struct', bad: 'bad_struct',
            reason: 'boolean', conversions: ['struct_nested_null']],
    ]
    cases.each { c ->
        [false, true].each { nullable ->
            // This is a type/nullability matrix, so each case retains a separate table for debugging.
            def suffix = "${c.name}_${nullable ? 'nullable' : 'required'}"
            def tableName = "test_arrow_type_${suffix}"
            sql "DROP TABLE IF EXISTS ${tableName}"
            sql """CREATE TABLE ${tableName} (k INT NOT NULL, v ${c.type} ${nullable ? 'NULL' : 'NOT NULL'})
                DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES ("replication_num" = "1")"""

            load(tableName, c.bad, c.reason)
            "qt_${suffix}_bad"("SELECT count(*) FROM ${tableName}")
            if (c.name.startsWith('decimal')) {
                load(tableName, c.name == 'decimal256' ? 'overflow_decimal256' : 'overflow_decimal128', 'precision')
                "qt_${suffix}_overflow"("SELECT count(*) FROM ${tableName}")
                load(tableName, 'scale_loss_decimal', 'data loss')
                "qt_${suffix}_scale_loss"("SELECT count(*) FROM ${tableName}")
            }
            if (c.name == 'decimal32') {
                // Arrow requires precision 12 to cast int32 to a scale-two decimal, even for 0/1.
                load(tableName, 'int32', 'precision')
                "qt_${suffix}_integer_precision"("SELECT count(*) FROM ${tableName}")
                if (nullable) {
                    load(tableName, 'int32_null', 'precision')
                    "qt_${suffix}_null_integer_precision"("SELECT count(*) FROM ${tableName}")
                }
            }
            if (c.name in ['array', 'map', 'struct']) {
                load(tableName, 'int32', '')
                "qt_${suffix}_container"("SELECT count(*) FROM ${tableName}")
            }
            if (c.name == 'struct') {
                ['short_struct', 'long_struct'].each { fixture ->
                    load(tableName, fixture, 'field count')
                    "qt_${suffix}_${fixture}"("SELECT count(*) FROM ${tableName}")
                }
            }
            if (c.late != null) {
                load(tableName, c.late, c.reason)
                "qt_${suffix}_rollback"("SELECT count(*) FROM ${tableName}")
            }

            if (!nullable) {
                // Correctly typed NULL input still has to obey the destination NOT NULL constraint.
                load(tableName, "${c.good}_null", '', false)
                "qt_${suffix}_not_null"("SELECT count(*) FROM ${tableName}")
            }
            // An empty batch and multiple nonempty batches precede the final NULL in nullable controls.
            load(tableName, "${c.good}_${nullable ? 'nullable' : 'good'}", null)
            sql 'sync'
            "order_qt_${suffix}_recovery"("SELECT * FROM ${tableName} ORDER BY k")

            c.conversions.eachWithIndex { fixture, index ->
                sql "TRUNCATE TABLE ${tableName}"
                load(tableName, fixture, null)
                sql 'sync'
                "order_qt_${suffix}_conversion_${index}"("SELECT * FROM ${tableName} ORDER BY k")
            }
            if (nullable && !(c.name in ['array', 'map', 'struct'])) {
                sql "TRUNCATE TABLE ${tableName}"
                load(tableName, c.name == 'decimal32' ? 'decimal128_null' : 'int32_null', null)
                sql 'sync'
                "order_qt_${suffix}_typed_null"("SELECT * FROM ${tableName} ORDER BY k")
            }
        }
    }
}
