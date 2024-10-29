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

import org.junit.jupiter.api.Assertions;

suite("docs/admin-manual/maint-monitor/tablet-repair-and-balance.md") {
    try {
        multi_sql """
        create database demo;
        
        
        create table demo.tbl1(
            id      int,
            name    varchar(20)
        ) 
        partition by list(id) (
            PARTITION `p1` VALUES in (1),
            PARTITION `p2` VALUES in (2)
        );
        """


        multi_sql """
        SHOW PROC '/cluster_health/tablet_health';
        SHOW REPLICA STATUS FROM tbl1 PARTITION (p1, p2) WHERE STATUS = "OK";
        SHOW TABLETS FROM tbl1;
        SHOW REPLICA DISTRIBUTION FROM tbl1;
        SHOW PROC '/cluster_balance/pending_tablets';
        SHOW PROC '/cluster_balance/cluster_load_stat/location_default';
        SHOW PROC '/cluster_balance/cluster_load_stat/location_default/HDD';
        SHOW PROC '/cluster_balance/working_slots';
        SHOW PROC '/cluster_balance/sched_stat';
        ADMIN SET FRONTEND CONFIG ("disable_balance" = "true");
        ADMIN SET FRONTEND CONFIG ("disable_tablet_scheduler" = "true");
        ADMIN SET FRONTEND CONFIG ("disable_colocate_balance" = "true");
        ADMIN SET FRONTEND CONFIG ("tablet_repair_delay_factor_second" = "120");
        ADMIN SET FRONTEND CONFIG ("colocate_group_relocate_delay_second" = "3600");
        ADMIN SET FRONTEND CONFIG ("enable_force_drop_redundant_replica" = "true");
        """

    } catch (Throwable t) {
        Assertions.fail("examples in docs/admin-manual/maint-monitor/tablet-repair-and-balance.md failed to exec, please fix it", t)
    }
}
