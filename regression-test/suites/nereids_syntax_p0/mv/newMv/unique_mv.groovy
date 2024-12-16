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

suite ("unique_mv") {
    sql """set enable_nereids_planner=true;"""
    sql """set enable_fallback_to_original_planner=false"""
    sql """ DROP TABLE IF EXISTS c5816_t; """

    sql """
            create table c5816_t(
                org_id bigint,
                campaign_id bigint,
                call_time datetime,
                id bigint,
                call_uuid varchar(128),
                aa bigint
            )
            unique KEY(org_id,campaign_id,call_time,id,call_uuid)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES
            (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "false"
            );
        """

    createMV("""create materialized view mv_1 as select call_uuid,org_id,call_time,id,campaign_id,aa from c5816_t""")
    sql """insert into c5816_t values (1,2,"2023-11-20 00:00:00",4,"adc",12);"""

    sql "analyze table c5816_t with sync;"
    sql """set enable_stats=false;"""

    mv_rewrite_success("SELECT * FROM c5816_t WHERE call_uuid='adc';", "mv_1")

    sql """set enable_stats=true;"""
    sql """alter table c5816_t modify column org_id set stats ('row_count'='1');"""
    mv_rewrite_success("SELECT * FROM c5816_t WHERE call_uuid='adc';", "mv_1")

}
