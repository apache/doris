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

suite("test_explain_tpch_sf_1_q15", "tpch_sf1") {

    explain {
        sql """
            
            SELECT
              s_suppkey,
              s_name,
              s_address,
              s_phone,
              total_revenue
            FROM
              supplier,
              revenue1
            WHERE
              s_suppkey = supplier_no
              AND total_revenue = (
                SELECT max(total_revenue)
                FROM
                  revenue1
              )
            ORDER BY
              s_suppkey;
      """
      
        check {
            explainStr -> {
                explainStr.contains("PREDICATES: `l_shipdate` >= '1996-01-01 00:00:00', `l_shipdate` < '1996-04-01 00:00:00'") &&
                        explainStr.contains("PREDICATES: `l_shipdate` >= '1996-01-01 00:00:00', `l_shipdate` < '1996-04-01 00:00:00'") &&
                        explainStr.contains("runtime filters: RF000[in_or_bloom] <- <slot 4> `l_suppkey`") &&
                        explainStr.contains("output slot ids: 19 20 21 22 5") &&
                        explainStr.contains("hash output slot ids: 19 20 21 22 5") &&
                        explainStr.contains("equal join conjunct: `s_suppkey` = <slot 4> `l_suppkey`") &&
                        explainStr.contains("equal join conjunct: <slot 5> sum(`l_extendedprice` * (1 - `l_discount`)) = <slot 17> max(`total_revenue`)") &&
                        explainStr.contains("order by: <slot 23> `s_suppkey` ASC");
            }
        }
    }
}