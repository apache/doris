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

suite("test_explain_tpch_sf_1_q17", "tpch_sf1") {
    explain {
        sql """
                SELECT sum(l_extendedprice) / 7.0 AS avg_yearly
                FROM
                  lineitem,
                  part
                WHERE
                  p_partkey = l_partkey
                  AND p_brand = 'Brand#23'
                  AND p_container = 'MED BOX'
                  AND l_quantity < (
                    SELECT 0.2 * avg(l_quantity)
                    FROM
                      lineitem
                    WHERE
                      l_partkey = p_partkey
                  )
            """
        check {
            explainStr -> {
                explainStr.contains("PREDICATES: `p_brand` = 'Brand#23', `p_container` = 'MED BOX'") &&
                explainStr.contains("runtime filters: RF001[in_or_bloom] -> `l_partkey`") &&
                explainStr.contains("output slot ids: 6 8 7") &&
                explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" +
                        "  |  equal join conjunct: `l_partkey` = `p_partkey`\n" +
                        "  |  runtime filters: RF001[in_or_bloom] <- `p_partkey`") &&
                explainStr.contains("output slot ids: 8 \n" +
                        "  |  hash output slot ids: 8 6 3") &&
                explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" +
                        "  |  equal join conjunct: `p_partkey` = <slot 2> `l_partkey`\n" +
                        "  |  other join predicates: `l_quantity` < 0.2 * <slot 3> avg(`l_quantity`)\n" +
                        "  |  runtime filters: RF000[in_or_bloom] <- <slot 2> `l_partkey`")
            }
        }
    }
}