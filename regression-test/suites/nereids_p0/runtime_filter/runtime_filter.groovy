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

suite("runtime_filter") {
    sql ''' drop table if exists rf_dws_asset_domain_statistics_daily'''
    sql '''CREATE TABLE rf_dws_asset_domain_statistics_daily (
        account_id int(11) NULL,
        ssp_id int(11) NULL,
        account_name varchar(500) NULL,
        d_s date NOT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(account_id, ssp_id, account_name) COMMENT 'OLAP'
        PARTITION BY RANGE(d_s) (PARTITION p20231220 VALUES [('2023-12-20'), ('2023-12-21')))
        DISTRIBUTED BY HASH(account_name) BUCKETS 9
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ); '''

    sql "set runtime_filter_mode=GLOBAL"

    explain {
        sql """
            SELECT count(*) FROM
            rf_dws_asset_domain_statistics_daily t1
            INNER JOIN ( 
                SELECT account_id, account_name
                FROM rf_dws_asset_domain_statistics_daily
                WHERE d_s = '2023-12-20'
            ) t2 
            ON (t1.account_id <=> t2.account_id);
            """
        notContains("RFs")
    }
}