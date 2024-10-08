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
suite("q44_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q44 """
SELECT
  asceding.rnk
, i1.i_product_name best_performing
, i2.i_product_name worst_performing
FROM
  (
   SELECT *
   FROM
     (
      SELECT
        item_sk
      , rank() OVER (ORDER BY rank_col ASC) rnk
      FROM
        (
         SELECT
           ss_item_sk item_sk
         , avg(ss_net_profit) rank_col
         FROM
           store_sales ss1
         WHERE (ss_store_sk = 4)
         GROUP BY ss_item_sk
         HAVING (avg(ss_net_profit) > (CAST('0.9' AS DECIMAL(2,1)) * (
                  SELECT avg(ss_net_profit) rank_col
                  FROM
                    store_sales
                  WHERE (ss_store_sk = 4)
                     AND (ss_addr_sk IS NULL)
                  GROUP BY ss_store_sk
               )))
      )  v1
   )  v11
   WHERE (rnk < 11)
)  asceding
, (
   SELECT *
   FROM
     (
      SELECT
        item_sk
      , rank() OVER (ORDER BY rank_col DESC) rnk
      FROM
        (
         SELECT
           ss_item_sk item_sk
         , avg(ss_net_profit) rank_col
         FROM
           store_sales ss1
         WHERE (ss_store_sk = 4)
         GROUP BY ss_item_sk
         HAVING (avg(ss_net_profit) > (CAST('0.9' AS DECIMAL(2,1)) * (
                  SELECT avg(ss_net_profit) rank_col
                  FROM
                    store_sales
                  WHERE (ss_store_sk = 4)
                     AND (ss_addr_sk IS NULL)
                  GROUP BY ss_store_sk
               )))
      )  v2
   )  v21
   WHERE (rnk < 11)
)  descending
, item i1
, item i2
WHERE (asceding.rnk = descending.rnk)
   AND (i1.i_item_sk = asceding.item_sk)
   AND (i2.i_item_sk = descending.item_sk)
ORDER BY asceding.rnk ASC
LIMIT 100
"""
}
