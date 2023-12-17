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

package org.apache.doris.cooldown;

import org.apache.doris.analysis.CreateUserStmt;
import org.apache.doris.analysis.GrantStmt;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AccessPrivilege;
import org.apache.doris.catalog.AccessPrivilegeWithCols;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.List;

public class CooldownConfHandlerTest extends TestWithFeService {
    private static final Logger LOG = LogManager.getLogger(CooldownConfHandlerTest.class);
    private long dbId = 100L;
    private long tableId = 101L;
    private long partitionId = 102L;
    private long indexId = 103L;
    private long tabletId = 104;

    private Tablet tablet = null;

    @Override
    protected void runBeforeAll() throws Exception {
        Config.enable_storage_policy = true;
        FeConstants.runningUnitTest = true;
        createDatabase("test");
        useDatabase("test");
        createTable("create table table1\n"
                + "(k1 int, k2 int) distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\" = \"1\");");
        // create user
        UserIdentity user = new UserIdentity("test_cooldown", "%");
        user.analyze();
        CreateUserStmt createUserStmt = new CreateUserStmt(new UserDesc(user));
        Env.getCurrentEnv().getAuth().createUser(createUserStmt);
        List<AccessPrivilegeWithCols> privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.ADMIN_PRIV));
        TablePattern tablePattern = new TablePattern("*", "*", "*");
        tablePattern.analyze();
        GrantStmt grantStmt = new GrantStmt(user, null, tablePattern, privileges);
        Env.getCurrentEnv().getAuth().grant(grantStmt);
        useUser("test_cooldown");
        Database db = Env.getCurrentInternalCatalog().getDb("test")
                .orElse(null);
        assert db != null;
        dbId = db.getId();
        Table tbl = db.getTable("table1").orElse(null);
        assert tbl != null;
        tableId = tbl.getId();
        assert tbl.getPartitionNames().size() == 1;
        Partition partition = tbl.getPartition("table1");
        partitionId = partition.getId();
        MaterializedIndex index = partition.getBaseIndex();
        indexId = index.getId();
        List<Tablet> tablets = index.getTablets();
        assert tablets.size() > 0;
        tablet = tablets.get(0);
        tablet.setCooldownConf(-1, 100);
        tabletId = tablet.getId();
        LOG.info("create table: db: {}, tbl: {}, partition: {}, index: {}, tablet: {}", dbId, tableId, partitionId,
                indexId, tabletId);
    }

    @Test
    public void updateCooldownConf() {
        List<CooldownConf> confToUpdate = new LinkedList<>();
        CooldownConf cooldownConf = new CooldownConf(dbId, tableId, partitionId, indexId, tabletId, 100);
        confToUpdate.add(cooldownConf);
        CooldownConfHandler cooldownConfHandler = new CooldownConfHandler();
        cooldownConfHandler.addCooldownConfToUpdate(confToUpdate);
        cooldownConfHandler.runAfterCatalogReady();
        Pair<Long, Long> conf = tablet.getCooldownConf();
        long cooldownReplicaId = conf.first;
        long cooldownTerm = conf.second;
        boolean matched = false;
        for (Replica replica : tablet.getReplicas()) {
            if (replica.getId() == cooldownReplicaId) {
                matched = true;
                break;
            }
        }
        Assert.assertTrue(matched);
        Assert.assertEquals(101, cooldownTerm);
    }
}
