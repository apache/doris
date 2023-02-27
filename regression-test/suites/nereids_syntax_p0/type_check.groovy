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

suite("type_check") {
    sql '''
        create table if not exists type_tb (
            id int, 
            json jsonb,
            dcml decimalv3(15, 2),
            arr array<int>
        )
        DUPLICATE KEY(id) 
        distributed by hash(id) buckets 2
        properties (
            "replication_num"="1"
        )
    '''

    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    // jsonb
    test {
        sql 'select * from type_tb'
        exception 'type unsupported for nereids planner'
    }

    test {
        sql 'select jsonb_parse(\'{"k1":"v31","k2":300}\')'
        exception 'type unsupported for nereids planner'
    }

    // array
    test {
        sql 'select array_range(10)'
        exception 'type unsupported for nereids planner'
    }

    // decimalv3
    test {
        sql 'select cast(0.3 as decimalv3(12, 2))'
        exception 'type unsupported for nereids planner'
    }
}