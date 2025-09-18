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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivObject.PrivObjectType;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;

public class AuthTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("CREATE TABLE test.`t1` (\n"
                + " `k1` bigint(20) NULL,\n"
                + " `k2` bigint(20) NULL,\n"
                + " `k3` bigint(20) not NULL,\n"
                + " `k4` bigint(20) not NULL,\n"
                + " `k5` bigint(20) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`k2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\"\n"
                + ");");
    }

    @Test
    public void testMergeRolePriv() throws Exception {
        addUser("u1", true);
        createRole("role1");
        createRole("role2");
        grantPriv("GRANT LOAD_PRIV ON internal.test.* TO ROLE 'role1';");
        grantPriv("GRANT GRANT_PRIV ON internal.test.* TO ROLE 'role2';");

        grantRole("GRANT 'role1','role2' TO 'u1'@'%'");
        Env.getCurrentEnv().getAuth().checkDbPriv(UserIdentity.createAnalyzedUserIdentWithIp("u1", "%"),
                InternalCatalog.INTERNAL_CATALOG_NAME, "test",
                PrivPredicate.of(PrivBitSet.of(Privilege.GRANT_PRIV, Privilege.LOAD_PRIV), Operator.AND));
    }

    @Test
    public void testGetUserPrivObjects() throws Exception {
        addUser("u2", true);
        // global
        grantPriv("GRANT GRANT_PRIV ON *.*.* TO 'u2';");
        // catalog
        grantPriv("GRANT SELECT_PRIV,LOAD_PRIV ON internal.*.* TO 'u2';");
        // db
        grantPriv("GRANT LOAD_PRIV ON internal.test.* TO 'u2';");
        grantPriv("GRANT SELECT_PRIV ON internal.test.* TO 'u2';");
        // table
        grantPriv("GRANT ALTER_PRIV ON internal.test.t1 TO 'u2';");
        // col
        grantPriv("GRANT SELECT_PRIV(k1,k2) ON internal.test.t1 TO 'u2';");
        grantPriv("GRANT SELECT_PRIV(k2,k3) ON internal.test.t1 TO 'u2';");
        // resource
        grantPriv("GRANT USAGE_PRIV ON RESOURCE 'r1' TO 'u2';");
        // COMPUTE GROUP
        grantPriv("GRANT USAGE_PRIV ON COMPUTE GROUP 'c1' TO 'u2';");
        // STORAGE VAULT
        grantPriv("GRANT USAGE_PRIV ON STORAGE VAULT 's1' TO 'u2';");
        // WORKLOAD GROUP
        grantPriv("GRANT USAGE_PRIV ON WORKLOAD GROUP 'normal' TO 'u2';");

        List<PrivObject> privObjectList = Env.getCurrentEnv().getAuth()
                .getUserPrivObjects(UserIdentity.createAnalyzedUserIdentWithIp("u2", "%"));
        checkRes(privObjectList);
    }

    @Test
    public void testGetRolePrivObjects() throws Exception {
        createRole("role3");
        // global
        grantPriv("GRANT GRANT_PRIV ON *.*.* TO ROLE 'role3';");
        // catalog
        grantPriv("GRANT SELECT_PRIV,LOAD_PRIV ON internal.*.* TO ROLE 'role3';");
        // db
        grantPriv("GRANT LOAD_PRIV ON internal.test.* TO ROLE 'role3';");
        grantPriv("GRANT SELECT_PRIV ON internal.test.* TO ROLE 'role3';");
        // table
        grantPriv("GRANT ALTER_PRIV ON internal.test.t1 TO ROLE 'role3';");
        // col
        grantPriv("GRANT SELECT_PRIV(k1,k2) ON internal.test.t1 TO ROLE 'role3';");
        grantPriv("GRANT SELECT_PRIV(k2,k3) ON internal.test.t1 TO ROLE 'role3';");
        // resource
        grantPriv("GRANT USAGE_PRIV ON RESOURCE 'r1' TO ROLE 'role3';");
        // COMPUTE GROUP
        grantPriv("GRANT USAGE_PRIV ON COMPUTE GROUP 'c1' TO ROLE 'role3';");
        // STORAGE VAULT
        grantPriv("GRANT USAGE_PRIV ON STORAGE VAULT 's1' TO ROLE 'role3';");
        // WORKLOAD GROUP
        grantPriv("GRANT USAGE_PRIV ON WORKLOAD GROUP 'normal' TO ROLE 'role3';");

        List<PrivObject> privObjectList = Env.getCurrentEnv().getAuth().getRoleByName("role3").getPrivObjects();
        checkRes(privObjectList);
    }

    private void checkRes(List<PrivObject> privObjectList) {
        HashSet<PrivObject> privObjects = Sets.newHashSet(privObjectList);
        Assert.assertTrue(privObjects.contains(new PrivObject(null, null, null, null, PrivObjectType.GLOBAL,
                Lists.newArrayList("Grant_priv"))));
        Assert.assertTrue(privObjects.contains(new PrivObject("internal", null, null, null, PrivObjectType.CATALOG,
                Lists.newArrayList("Select_priv", "Load_priv"))));
        Assert.assertTrue(privObjects.contains(new PrivObject("internal", "test", null, null, PrivObjectType.DATABASE,
                Lists.newArrayList("Select_priv", "Load_priv"))));
        Assert.assertTrue(privObjects.contains(new PrivObject("internal", "test", "t1", null, PrivObjectType.TABLE,
                Lists.newArrayList("Alter_priv"))));
        Assert.assertTrue(privObjects.contains(new PrivObject("internal", "test", "t1", "k1", PrivObjectType.COL,
                Lists.newArrayList("Select_priv"))));
        Assert.assertTrue(privObjects.contains(new PrivObject("internal", "test", "t1", "k2", PrivObjectType.COL,
                Lists.newArrayList("Select_priv"))));
        Assert.assertTrue(privObjects.contains(new PrivObject("internal", "test", "t1", "k3", PrivObjectType.COL,
                Lists.newArrayList("Select_priv"))));
        Assert.assertTrue(privObjects.contains(new PrivObject(null, null, null, "r1", PrivObjectType.RESOURCE,
                Lists.newArrayList("Usage_priv"))));
        Assert.assertTrue(privObjects.contains(new PrivObject(null, null, null, "c1", PrivObjectType.COMPUTE_GROUP,
                Lists.newArrayList("Cluster_usage_priv"))));
        Assert.assertTrue(privObjects.contains(new PrivObject(null, null, null, "s1", PrivObjectType.STORAGE_VAULT,
                Lists.newArrayList("Usage_priv"))));
        Assert.assertTrue(privObjects.contains(new PrivObject(null, null, null, "normal", PrivObjectType.WORKLOAD_GROUP,
                Lists.newArrayList("Usage_priv"))));
    }

}
