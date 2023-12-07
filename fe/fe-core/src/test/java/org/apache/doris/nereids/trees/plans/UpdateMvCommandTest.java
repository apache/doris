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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.nereids.trees.plans.commands.UpdateMvByPartitionCommand;
import org.apache.doris.nereids.util.PlanPatternMatchSupported;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

class UpdateMvCommandTest extends TestWithFeService implements PlanPatternMatchSupported {
    String dbName = "test";
    String mvName = "mv";

    @Override
    public void runBeforeAll() throws Exception {
        createDatabase(dbName);
        connectContext.setDatabase("default_cluster:test");
        createTable("create table t1 (\n"
                + "    k1 int not null,\n"
                + "    k2 int\n"
                + ")\n"
                + "unique key(k1, k2)\n"
                + "partition by range(k1) (PARTITION p1 values less than (1), PARTITION p2 values less than (2))\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");
        createTable("create table t2 (\n"
                + "    k1 int not null,\n"
                + "    k2 int\n"
                + ")\n"
                + "unique key(k1, k2)\n"
                + "partition by list(k1) (PARTITION p1 values in (1, 2, 3), PARTITION p2 values in (4, 5, 6))\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");

        createMvByNereids(String.format("create materialized view %s BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "        distributed by hash(k) buckets 4\n"
                + "        PROPERTIES ('replication_num' = '1')  as\n"
                + "  select t1.k1 as k from t1 join t2 on t1.k1 = t2.k1 group by k;", mvName));
    }

    @Test
    void test() throws Exception {
        MTMV mv = (MTMV) Objects.requireNonNull(Env.getCurrentEnv().getCurrentCatalog()
                        .getDbNullable("default_cluster:test"))
                        .getOlapTableOrDdlException(mvName);
        OlapTable t1 = Objects.requireNonNull(Env.getCurrentEnv().getCurrentCatalog()
                        .getDbNullable("default_cluster:test"))
                .getOlapTableOrDdlException("t1");
        OlapTable t2 = Objects.requireNonNull(Env.getCurrentEnv().getCurrentCatalog()
                        .getDbNullable("default_cluster:test"))
                .getOlapTableOrDdlException("t2");
        Set<PartitionItem> items = ImmutableSet.of();
        Map<OlapTable, String> cols = ImmutableMap.of(t1, "k1", t2, "k2");
        UpdateMvByPartitionCommand command = UpdateMvByPartitionCommand.from(mv, items, cols);
        Assertions.assertEquals("INSERT OVERWRITE TABLE mv select t1.k1 as k from t1[] join t2[] on t1.k1 = t2.k1 group by k",
                command.getLabel());

    }
}
