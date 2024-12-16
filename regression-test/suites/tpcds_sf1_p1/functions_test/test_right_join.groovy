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

suite("test_right_join") {
    sql """set disable_join_reorder=true"""
    sql """set parallel_fragment_exec_instance_num=1"""
    sql "use regression_test_tpcds_sf1_p1"

    qt_sql_right_outer_join_with_other_conjuncts """
      WITH customer_total_return AS (
          SELECT
              ca_state ctr_state,
              sum(cr_return_amt_inc_tax) ctr_total_return
          FROM
              catalog_returns,
              date_dim,
              customer_address
          WHERE
              (cr_returned_date_sk = d_date_sk)
              AND (d_year = 2000)
              AND (cr_returning_addr_sk = ca_address_sk)
          GROUP BY
              cr_returning_customer_sk,
              ca_state
      ),
      ctr2 as (
          SELECT
              (avg(ctr_total_return)) ctr_total_return,
              ctr_state
          FROM
              customer_total_return
          group by
              ctr_state
      )
      SELECT
          count(ctr1.ctr_state)
      FROM
          ctr2 right
          join customer_total_return ctr1 on ctr1.ctr_state = ctr2.ctr_state
          and ctr1.ctr_total_return > ctr2.ctr_total_return;
    """

    qt_sql_full_join_with_other_conjuncts """
      WITH customer_total_return AS (
          SELECT
              ca_state ctr_state,
              sum(cr_return_amt_inc_tax) ctr_total_return
          FROM
              catalog_returns,
              date_dim,
              customer_address
          WHERE
              (cr_returned_date_sk = d_date_sk)
              AND (d_year = 2000)
              AND (cr_returning_addr_sk = ca_address_sk)
          GROUP BY
              cr_returning_customer_sk,
              ca_state
      ),
      ctr2 as (
          SELECT
              (avg(ctr_total_return)) ctr_total_return,
              ctr_state
          FROM
              customer_total_return
          group by
              ctr_state
      )
      SELECT
          count(ctr1.ctr_state)
      FROM
          ctr2 full
          join customer_total_return ctr1 on ctr1.ctr_state = ctr2.ctr_state
          and ctr1.ctr_total_return > ctr2.ctr_total_return;
    """

    qt_sql_right_semi_join_with_other_conjuncts """
      WITH customer_total_return AS (
          SELECT
              ca_state ctr_state,
              sum(cr_return_amt_inc_tax) ctr_total_return
          FROM
              catalog_returns,
              date_dim,
              customer_address
          WHERE
              (cr_returned_date_sk = d_date_sk)
              AND (d_year = 2000)
              AND (cr_returning_addr_sk = ca_address_sk)
          GROUP BY
              cr_returning_customer_sk,
              ca_state
      ),
      ctr2 as (
          SELECT
              (avg(ctr_total_return)) ctr_total_return,
              ctr_state
          FROM
              customer_total_return
          group by
              ctr_state
      )
      SELECT
          count(ctr1.ctr_state)
      FROM
          ctr2 right semi
          join customer_total_return ctr1 on ctr1.ctr_state = ctr2.ctr_state
          and ctr1.ctr_total_return > ctr2.ctr_total_return;
    """

    qt_sql_right_anti_join_with_other_conjuncts """
      WITH customer_total_return AS (
          SELECT
              ca_state ctr_state,
              sum(cr_return_amt_inc_tax) ctr_total_return
          FROM
              catalog_returns,
              date_dim,
              customer_address
          WHERE
              (cr_returned_date_sk = d_date_sk)
              AND (d_year = 2000)
              AND (cr_returning_addr_sk = ca_address_sk)
          GROUP BY
              cr_returning_customer_sk,
              ca_state
      ),
      ctr2 as (
          SELECT
              (avg(ctr_total_return)) ctr_total_return,
              ctr_state
          FROM
              customer_total_return
          group by
              ctr_state
      )
      SELECT
          count(ctr1.ctr_state)
      FROM
          ctr2 right anti
          join customer_total_return ctr1 on ctr1.ctr_state = ctr2.ctr_state
          and ctr1.ctr_total_return > ctr2.ctr_total_return;
    """
}
