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

suite("join_order_dphyper") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    sql """ drop table if exists dphyper_store_sales;"""
    sql """
        CREATE TABLE IF NOT EXISTS dphyper_store_sales (
            ss_sold_date_sk bigint,
            ss_customer_sk bigint,
            ss_cdemo_sk bigint,
            ss_hdemo_sk bigint,
            ss_addr_sk bigint,
            ss_store_sk bigint,
            ss_ticket_number bigint
        )
        DUPLICATE KEY(ss_sold_date_sk, ss_customer_sk)
        DISTRIBUTED BY HASH(ss_customer_sk) BUCKETS 3
        PROPERTIES (
        "replication_num" = "1"
        )
    """
    sql """ drop table if exists dphyper_store_returns;"""
    sql """
        CREATE TABLE IF NOT EXISTS dphyper_store_returns (
            sr_ticket_number bigint
        )
        DUPLICATE KEY(sr_ticket_number)
        DISTRIBUTED BY HASH(sr_ticket_number) BUCKETS 3
        PROPERTIES (
        "replication_num" = "1"
        )
    """
    sql """ drop table if exists dphyper_date_dim;"""
    sql """
        CREATE TABLE IF NOT EXISTS dphyper_date_dim (
            d_date_sk bigint
        )
        DUPLICATE KEY(d_date_sk)
        DISTRIBUTED BY HASH(d_date_sk) BUCKETS 3
        PROPERTIES (
        "replication_num" = "1"
        )
    """
    sql """ drop table if exists dphyper_store;"""
    sql """
        CREATE TABLE IF NOT EXISTS dphyper_store (
            s_store_sk bigint
        )
        DUPLICATE KEY(s_store_sk)
        DISTRIBUTED BY HASH(s_store_sk) BUCKETS 3
        PROPERTIES (
        "replication_num" = "1"
        )
    """
    sql """ drop table if exists dphyper_customer;"""
    sql """
        CREATE TABLE IF NOT EXISTS dphyper_customer (
            c_customer_sk bigint,
            c_current_cdemo_sk bigint,
            c_current_hdemo_sk bigint,
            c_current_addr_sk bigint
        )
        DUPLICATE KEY(c_customer_sk)
        DISTRIBUTED BY HASH(c_customer_sk) BUCKETS 3
        PROPERTIES (
        "replication_num" = "1"
        )
    """
    sql """ drop table if exists dphyper_customer_demographics;"""
    sql """
        CREATE TABLE IF NOT EXISTS dphyper_customer_demographics (
            cd_demo_sk bigint,
            cd_marital_status char(1)
        )
        DUPLICATE KEY(cd_demo_sk)
        DISTRIBUTED BY HASH(cd_demo_sk) BUCKETS 3
        PROPERTIES (
        "replication_num" = "1"
        )
    """
    sql """ drop table if exists dphyper_household_demographics;"""
    sql """
        CREATE TABLE IF NOT EXISTS dphyper_household_demographics (
            hd_demo_sk bigint
        )
        DUPLICATE KEY(hd_demo_sk)
        DISTRIBUTED BY HASH(hd_demo_sk) BUCKETS 3
        PROPERTIES (
        "replication_num" = "1"
        )
    """
    sql """ drop table if exists dphyper_customer_address;"""
    sql """
        CREATE TABLE IF NOT EXISTS dphyper_customer_address (
            ca_address_sk bigint
        )
        DUPLICATE KEY(ca_address_sk)
        DISTRIBUTED BY HASH(ca_address_sk) BUCKETS 3
        PROPERTIES (
        "replication_num" = "1"
        )
    """

    // explain {
    //     sql("""SELECT
    //             count(*)
    //             FROM
    //             dphyper_store_sales
    //             , dphyper_store_returns
    //             , dphyper_date_dim d1
    //             , dphyper_store
    //             , dphyper_customer
    //             , dphyper_customer_demographics cd1
    //             , dphyper_customer_demographics cd2
    //             , dphyper_household_demographics hd1
    //             , dphyper_household_demographics hd2
    //             , dphyper_customer_address ad1
    //             , dphyper_customer_address ad2
    //             WHERE (ss_store_sk = s_store_sk)
    //                 AND (ss_sold_date_sk = d1.d_date_sk)
    //                 AND (ss_customer_sk = c_customer_sk)
    //                 AND (ss_cdemo_sk = cd1.cd_demo_sk)
    //                 AND (ss_hdemo_sk = hd1.hd_demo_sk)
    //                 AND (ss_addr_sk = ad1.ca_address_sk)
    //                 AND (ss_ticket_number = sr_ticket_number)
    //                 AND (c_current_cdemo_sk = cd2.cd_demo_sk)
    //                 AND (c_current_hdemo_sk = hd2.hd_demo_sk)
    //                 AND (c_current_addr_sk = ad2.ca_address_sk)
    //                 AND (cd1.cd_marital_status <> cd2.cd_marital_status);""")
    //     notContains "VNESTED LOOP JOIN"
    // }
}
