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

suite("test_explain_tpch_sf_1_q18", "tpch_sf1") {
    explain {
        sql """
            SELECT
                  c_name,
                  c_custkey,
                  o_orderkey,
                  o_orderdate,
                  o_totalprice,
                  sum(l_quantity)
                FROM
                  customer,
                  orders,
                  lineitem
                WHERE
                  o_orderkey IN (
                    SELECT l_orderkey
                    FROM
                      lineitem
                    GROUP BY
                      l_orderkey
                    HAVING
                      sum(l_quantity) > 300
                  )
                  AND c_custkey = o_custkey
                  AND o_orderkey = l_orderkey
                GROUP BY
                  c_name,
                  c_custkey,
                  o_orderkey,
                  o_orderdate,
                  o_totalprice
                ORDER BY
                  o_totalprice DESC,
                  o_orderdate
                LIMIT 100

            """
        check {
            explainStr -> {
                explainStr.contains("TABLE: orders(orders), PREAGGREGATION: ON\n" +
                        "     runtime filters: RF001[in_or_bloom] -> `o_orderkey`, RF002[in_or_bloom] -> `o_custkey`") &&
                explainStr.contains("TABLE: lineitem(lineitem), PREAGGREGATION: ON\n" +
                        "     runtime filters: RF000[in_or_bloom] -> `l_orderkey`, RF003[in_or_bloom] -> `l_orderkey`") &&
                explainStr.contains("output slot ids: 11 16 5 14 15 17 \n" +
                        "  |  hash output slot ids: 11 16 5 14 15 17") &&
                explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" +
                        "  |  equal join conjunct: `l_orderkey` = `o_orderkey`\n" +
                        "  |  runtime filters: RF003[in_or_bloom] <- `o_orderkey`") &&
                explainStr.contains("output slot ids: 11 16 5 14 15 12 13 \n" +
                        "  |  hash output slot ids: 11 16 5 14 15 12 13") &&
                explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" +
                        "  |  equal join conjunct: `o_custkey` = `c_custkey`\n" +
                        "  |  runtime filters: RF002[in_or_bloom] <- `c_custkey`") &&
                explainStr.contains("output slot ids: 11 16 5 14 15 12 13 \n" +
                        "  |  hash output slot ids: 11 16 5 14 15 12 13") &&
                explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" +
                        "  |  equal join conjunct: `o_orderkey` = <slot 2> `l_orderkey`\n" +
                        "  |  runtime filters: RF001[in_or_bloom] <- <slot 2> `l_orderkey`") &&
                explainStr.contains("output slot ids: 16 5 14 15 12 13 \n" +
                        "  |  hash output slot ids: 16 5 14 15 12 13") &&
                explainStr.contains("join op: LEFT SEMI JOIN(COLOCATE[])[]\n" +
                        "  |  equal join conjunct: `l_orderkey` = <slot 8> `l_orderkey`\n" +
                        "  |  runtime filters: RF000[in_or_bloom] <- <slot 8> `l_orderkey`") &&
                explainStr.contains("VTOP-N\n" +
                        "  |  order by: <slot 24> <slot 22> `o_totalprice` DESC, <slot 25> <slot 21> `o_orderdate` ASC");
            }
        }
    }
}