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

package org.apache.doris.qe;

import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.qe.Coordinator.FInstanceExecParam;
import org.apache.doris.qe.Coordinator.FragmentExecParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.SetMultimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

public class LocalShuffleTest extends TestWithFeService  {
    @Override
    protected int backendNum() {
        return 2;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        createTable("CREATE TABLE `oooo_oneid` (\n"
                + "  `identity_code` varchar(128) NULL COMMENT \"\",\n"
                + "  `identity_value` varchar(128) NULL COMMENT \"\",\n"
                + "  `oneid` bigint NULL COMMENT \"\",\n"
                + "  INDEX idx_oneid (`oneid`) USING INVERTED COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "UNIQUE KEY(`identity_code`, `identity_value`)\n"
                + "COMMENT 'oooo'\n"
                + "DISTRIBUTED BY HASH(`identity_code`, `identity_value`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 2\"\n"
                + ")");

        createTable("CREATE TABLE `aaaa_target` (\n"
                + "  `base_id` varchar(128) NULL COMMENT \"\",\n"
                + "  `hash_partition_id` bigint NOT NULL COMMENT \"\",\n"
                + "  `j_bbbb` text NULL COMMENT \"j_bbbb\"\n"
                + ") ENGINE=OLAP\n"
                + "UNIQUE KEY(`base_id`, `hash_partition_id`)\n"
                + "COMMENT 'aaaa'\n"
                + "PARTITION BY LIST (`hash_partition_id`)\n"
                + "(PARTITION p0 VALUES IN (\"0\"),\n"
                + "PARTITION p1 VALUES IN (\"1\"),\n"
                + "PARTITION p2 VALUES IN (\"2\"),\n"
                + "PARTITION p3 VALUES IN (\"3\"),\n"
                + "PARTITION p4 VALUES IN (\"4\"),\n"
                + "PARTITION p5 VALUES IN (\"5\"),\n"
                + "PARTITION p6 VALUES IN (\"6\"),\n"
                + "PARTITION p7 VALUES IN (\"7\"),\n"
                + "PARTITION p8 VALUES IN (\"8\"),\n"
                + "PARTITION p9 VALUES IN (\"9\"),\n"
                + "PARTITION p10 VALUES IN (\"10\"),\n"
                + "PARTITION p11 VALUES IN (\"11\"),\n"
                + "PARTITION p12 VALUES IN (\"12\"),\n"
                + "PARTITION p13 VALUES IN (\"13\"),\n"
                + "PARTITION p14 VALUES IN (\"14\"),\n"
                + "PARTITION p15 VALUES IN (\"15\"),\n"
                + "PARTITION p16 VALUES IN (\"16\"),\n"
                + "PARTITION p17 VALUES IN (\"17\"),\n"
                + "PARTITION p18 VALUES IN (\"18\"),\n"
                + "PARTITION p19 VALUES IN (\"19\"))\n"
                + "DISTRIBUTED BY HASH(`base_id`, `hash_partition_id`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 2\"\n"
                + ");");

        createTable("CREATE TABLE `bbbb_target` (\n"
                + "  `base_id` varchar(128) NULL COMMENT \"\",\n"
                + "  `hash_partition_id` bigint NOT NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "UNIQUE KEY(`base_id`, `hash_partition_id`)\n"
                + "COMMENT 'bbbb'\n"
                + "PARTITION BY LIST (`hash_partition_id`)\n"
                + "(PARTITION p0 VALUES IN (\"0\"),\n"
                + "PARTITION p1 VALUES IN (\"1\"),\n"
                + "PARTITION p2 VALUES IN (\"2\"),\n"
                + "PARTITION p3 VALUES IN (\"3\"),\n"
                + "PARTITION p4 VALUES IN (\"4\"),\n"
                + "PARTITION p5 VALUES IN (\"5\"),\n"
                + "PARTITION p6 VALUES IN (\"6\"),\n"
                + "PARTITION p7 VALUES IN (\"7\"),\n"
                + "PARTITION p8 VALUES IN (\"8\"),\n"
                + "PARTITION p9 VALUES IN (\"9\"),\n"
                + "PARTITION p10 VALUES IN (\"10\"),\n"
                + "PARTITION p11 VALUES IN (\"11\"),\n"
                + "PARTITION p12 VALUES IN (\"12\"),\n"
                + "PARTITION p13 VALUES IN (\"13\"),\n"
                + "PARTITION p14 VALUES IN (\"14\"),\n"
                + "PARTITION p15 VALUES IN (\"15\"),\n"
                + "PARTITION p16 VALUES IN (\"16\"),\n"
                + "PARTITION p17 VALUES IN (\"17\"),\n"
                + "PARTITION p18 VALUES IN (\"18\"),\n"
                + "PARTITION p19 VALUES IN (\"19\"))\n"
                + "DISTRIBUTED BY HASH(`base_id`, `hash_partition_id`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 2\"\n"
                + ");");

        createTable("CREATE TABLE `cccc_target` (\n"
                + "  `base_id` varchar(128) NULL COMMENT \"\",\n"
                + "  `hash_partition_id` bigint NOT NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "UNIQUE KEY(`base_id`, `hash_partition_id`)\n"
                + "COMMENT 'bbbb'\n"
                + "PARTITION BY LIST (`hash_partition_id`)\n"
                + "(PARTITION p0 VALUES IN (\"0\"),\n"
                + "PARTITION p1 VALUES IN (\"1\"),\n"
                + "PARTITION p2 VALUES IN (\"2\"),\n"
                + "PARTITION p3 VALUES IN (\"3\"),\n"
                + "PARTITION p4 VALUES IN (\"4\"),\n"
                + "PARTITION p5 VALUES IN (\"5\"),\n"
                + "PARTITION p6 VALUES IN (\"6\"),\n"
                + "PARTITION p7 VALUES IN (\"7\"),\n"
                + "PARTITION p8 VALUES IN (\"8\"),\n"
                + "PARTITION p9 VALUES IN (\"9\"),\n"
                + "PARTITION p10 VALUES IN (\"10\"),\n"
                + "PARTITION p11 VALUES IN (\"11\"),\n"
                + "PARTITION p12 VALUES IN (\"12\"),\n"
                + "PARTITION p13 VALUES IN (\"13\"),\n"
                + "PARTITION p14 VALUES IN (\"14\"),\n"
                + "PARTITION p15 VALUES IN (\"15\"),\n"
                + "PARTITION p16 VALUES IN (\"16\"),\n"
                + "PARTITION p17 VALUES IN (\"17\"),\n"
                + "PARTITION p18 VALUES IN (\"18\"),\n"
                + "PARTITION p19 VALUES IN (\"19\"))\n"
                + "DISTRIBUTED BY HASH(`base_id`, `hash_partition_id`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 2\"\n"
                + ");");
    }

    @Test
    public void testCteWithLocalShuffle() throws Exception {
        connectContext.getState().reset();
        connectContext.getSessionVariable().parallelPipelineTaskNum = 2;
        connectContext.getSessionVariable().setDisableNereidsRules(RuleType.PRUNE_EMPTY_PARTITION.name());
        connectContext.getSessionVariable().setQueryTimeoutS(10);
        connectContext.getSessionVariable().setEnableNereidsDistributePlanner(false);
        connectContext.getSessionVariable().setDisableJoinReorder(true);

        String sql = "with oooo_oneid as (select identity_value from test.oooo_oneid),\n"
                + "    aaaa_frm_$oooo as (select j_bbbb,base_id from test.aaaa_target),\n"
                + "    bbbb_frm_$aaaa_target as (select * from bbbb_target where base_id in (select 1 from aaaa_frm_$oooo)),\n"
                + "    bbbb_cdn_1602527468_lnkb_bbbb_target$aaaa_target as (select base_id from bbbb_frm_$aaaa_target),\n"
                + "    bbbb_frm_$oooo as (select *\n"
                + "                        from bbbb_target\n"
                + "                        where base_id in (select identity_value from oooo_oneid)\n"
                + "                      ),\n"
                + "    cccc_frm_$bbbb_target as (select * from cccc_target where base_id in (select 1 from bbbb_frm_$oooo))\n"
                + "select\n"
                + "   j_bbbb in (select base_id from bbbb_cdn_1602527468_lnkb_bbbb_target$aaaa_target),\n"
                + "   j_bbbb in (select base_id from bbbb_cdn_1602527468_lnkb_bbbb_target$aaaa_target)\n"
                + "from (select j_bbbb\n"
                + "     from oooo_oneid\n"
                + "     left outer join aaaa_frm_$oooo on (oooo_oneid.identity_value = aaaa_frm_$oooo.base_id)\n"
                + "     left outer join bbbb_frm_$oooo on (oooo_oneid.identity_value = bbbb_frm_$oooo.base_id)\n"
                + ")final_plain_table";
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        try {
            stmtExecutor.execute();
        } catch (Throwable t) {
            // ignore
        }
        Coordinator coord = stmtExecutor.getCoord();
        Map<PlanFragmentId, FragmentExecParams> fragmentExecParamsMap = coord.getFragmentExecParamsMap();

        for (FragmentExecParams fragmentExecParams : fragmentExecParamsMap.values()) {
            // skip check root fragment
            if (fragmentExecParams.fragment.getChildren().isEmpty()) {
                continue;
            }
            SetMultimap<TNetworkAddress, Integer> receiverIds = LinkedHashMultimap.create();
            ArrayListMultimap<TNetworkAddress, FInstanceExecParam> hostToInstances = ArrayListMultimap.create();
            boolean setRecvrId = false;
            for (FInstanceExecParam instanceExecParam : fragmentExecParams.instanceExecParams) {
                if (instanceExecParam.recvrId != -1) {
                    setRecvrId = true;
                }
                receiverIds.put(instanceExecParam.host, instanceExecParam.recvrId);
                hostToInstances.put(instanceExecParam.host, instanceExecParam);
            }
            if (!setRecvrId) {
                // skip check when share broadcast hash table
                continue;
            }
            for (Entry<TNetworkAddress, Collection<Integer>> hostToReceiverIds : receiverIds.asMap()
                    .entrySet()) {
                // if this host has 2 instances, it should contain 2 receiverId
                Assertions.assertEquals(
                        hostToInstances.get(hostToReceiverIds.getKey()).size(),
                        hostToReceiverIds.getValue().size()
                );
            }
        }
    }
}
