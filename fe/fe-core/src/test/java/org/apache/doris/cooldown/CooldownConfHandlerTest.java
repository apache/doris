package org.apache.doris.cooldown;

import org.apache.doris.analysis.CreateUserStmt;
import org.apache.doris.analysis.GrantStmt;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AccessPrivilege;
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
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class CooldownConfHandlerTest extends TestWithFeService {

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
                + "properties(\"replication_num\" = \"3\");");
        // create user
        UserIdentity user = new UserIdentity("test_cooldown", "%");
        user.analyze(SystemInfoService.DEFAULT_CLUSTER);
        CreateUserStmt createUserStmt = new CreateUserStmt(new UserDesc(user));
        Env.getCurrentEnv().getAuth().createUser(createUserStmt);
        List<AccessPrivilege> privileges = Lists.newArrayList(AccessPrivilege.ADMIN_PRIV);
        TablePattern tablePattern = new TablePattern("*", "*", "*");
        tablePattern.analyze(SystemInfoService.DEFAULT_CLUSTER);
        GrantStmt grantStmt = new GrantStmt(user, null, tablePattern, privileges);
        Env.getCurrentEnv().getAuth().grant(grantStmt);
        useUser("test_cooldown");
        Database db = Env.getCurrentEnv().getInternalCatalog().getDb("test").orElse(null);
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
        tabletId = tablet.getId();
    }

    @Test
    public void updateCooldownConf() {
        List<CooldownConf> confToUpdate = new LinkedList<>();
        CooldownConf cooldownConf = new CooldownConf(dbId, tableId, partitionId, indexId, tabletId, 100);
        confToUpdate.add(cooldownConf);
        CooldownConfHandler cooldownConfHandler = new CooldownConfHandler();
        cooldownConfHandler.addCooldownConfToUpdate(confToUpdate);
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
