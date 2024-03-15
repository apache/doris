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

suite("test_bucket_hash_local_shuffle") {
    try {
      sql """
          CREATE TABLE IF NOT EXISTS date_dim (
              d_date_sk bigint not null
          )
          DUPLICATE KEY(d_date_sk)
          DISTRIBUTED BY HASH(d_date_sk) BUCKETS 12
          PROPERTIES (
            "replication_num" = "1"
          );
          """
      sql """ insert into date_dim values(1) """
      sql """
          CREATE TABLE IF NOT EXISTS store_sales (
              ss_item_sk bigint not null,
              ss_ticket_number bigint not null,
              ss_sold_date_sk bigint
          )
          DUPLICATE KEY(ss_item_sk, ss_ticket_number)
          DISTRIBUTED BY HASH(ss_item_sk, ss_ticket_number) BUCKETS 32
          PROPERTIES (
            "replication_num" = "1",
            "colocate_with" = "store"
          );
          """
      sql """ insert into store_sales values(1, 1, 1),(1, 2, 1),(3, 2, 1),(100, 2, 1),(12130, 2, 1)  """
        sql """
            CREATE TABLE IF NOT EXISTS store_returns (
                sr_item_sk bigint not null,
                sr_ticket_number bigint not null
            )
            duplicate key(sr_item_sk, sr_ticket_number)
            distributed by hash (sr_item_sk, sr_ticket_number) buckets 32
            properties (
              "replication_num" = "1",
              "colocate_with" = "store"
            );
            """
        sql """ insert into store_returns values(1, 1),(1, 2),(3, 2),(100, 2),(12130, 2)"""
        qt_bucket_shuffle_join """ select /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=false)*/ count(*)
                      from store_sales
                      join date_dim on ss_sold_date_sk = d_date_sk
                      left join store_returns on sr_ticket_number=ss_ticket_number and ss_item_sk=sr_item_sk where sr_ticket_number is null """
        qt_colocate_join """ select /*+SET_VAR(disable_join_reorder=true,ignore_storage_data_distribution=false)*/ count(*)
                      from store_sales
                      join date_dim on ss_sold_date_sk = d_date_sk
                      left join store_returns on sr_ticket_number=ss_ticket_number and ss_item_sk=sr_item_sk where sr_ticket_number is null """
        qt_analytic """ select /*+SET_VAR(ignore_storage_data_distribution=false)*/ max(ss_sold_date_sk)
                                OVER (PARTITION BY ss_ticket_number, ss_item_sk) from (select *
                                                      from store_sales
                                                      join date_dim on ss_sold_date_sk = d_date_sk) result """
    } finally {
        sql """ DROP TABLE IF EXISTS store_sales """
        sql """ DROP TABLE IF EXISTS date_dim """
        sql """ DROP TABLE IF EXISTS store_returns """
    }
}

