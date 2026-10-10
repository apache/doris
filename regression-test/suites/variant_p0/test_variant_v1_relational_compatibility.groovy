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

suite("test_variant_v1_relational_compatibility", "p0,nonConcurrent") {
    setFeConfigTemporary([enable_variant_v2: false]) {
        test {
            sql '''
                SELECT v FROM (
                    SELECT CAST('2' AS VARIANT) AS v
                    UNION ALL SELECT CAST('1' AS VARIANT) AS v
                ) t ORDER BY v
            '''
            exception "Doris hll, bitmap"
        }
        test {
            sql '''
                SELECT v FROM (
                    SELECT CAST('2' AS VARIANT) AS v
                    UNION ALL SELECT CAST('1' AS VARIANT) AS v
                ) t ORDER BY v LIMIT 1
            '''
            exception "Doris hll, bitmap"
        }
        test {
            sql '''
                SELECT row_number() OVER (ORDER BY parse_to_variant(CAST(number AS STRING)))
                FROM numbers("number" = "2")
            '''
            exception "Doris hll, bitmap"
        }
        test {
            sql '''
                SELECT * FROM (SELECT CAST('1' AS VARIANT) AS v) a
                JOIN (SELECT CAST('1' AS VARIANT) AS v) b ON a.v = b.v
            '''
            exception "could not used in ComparisonPredicate"
        }
        order_qt_scalar_comparison '''
            SELECT CAST('1' AS VARIANT) = 1, CAST('2' AS VARIANT) > 1,
                   CAST(CAST('1' AS VARIANT) AS STRING) = '1'
        '''
        order_qt_json_extract '''
            SELECT json_extract(CAST('{"id":1}' AS VARIANT), '$.id')
        '''
    }
}
