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

import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.Pair;
import org.apache.doris.mysql.MysqlCapability;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class ConnectContextTest {
    @Mocked
    private StmtExecutor executor;
    @Mocked
    private SocketChannel socketChannel;
    @Mocked
    private Env env;
    @Mocked
    private ConnectScheduler connectScheduler;
    @Mocked
    private Auth auth;
    @Mocked
    private String qualifiedUser;
    @Mocked
    private CloudSystemInfoService cloudSystemInfoService;
    @Mocked
    private AccessControllerManager accessManager;
    @Mocked
    private Backend backend;

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testNormal() {
        ConnectContext ctx = new ConnectContext();

        // State
        Assert.assertNotNull(ctx.getState());

        // Capability
        Assert.assertEquals(MysqlCapability.DEFAULT_CAPABILITY, ctx.getServerCapability());
        ctx.setCapability(new MysqlCapability(10));
        Assert.assertEquals(new MysqlCapability(10), ctx.getCapability());

        // Kill flag
        Assert.assertFalse(ctx.isKilled());
        ctx.setKilled();
        Assert.assertTrue(ctx.isKilled());

        // Current db
        Assert.assertEquals("", ctx.getDatabase());
        ctx.setDatabase("testDb");
        Assert.assertEquals("testDb", ctx.getDatabase());

        // User
        ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("testUser", "%"));
        Assert.assertEquals("testUser", ctx.getQualifiedUser());

        // Serializer
        Assert.assertNotNull(ctx.getMysqlChannel().getSerializer());

        // Session variable
        Assert.assertNotNull(ctx.getSessionVariable());

        // connect scheduler
        Assert.assertNull(ctx.getConnectScheduler());
        ctx.setConnectScheduler(connectScheduler);
        Assert.assertNotNull(ctx.getConnectScheduler());

        // connection id
        ctx.setConnectionId(101);
        Assert.assertEquals(101, ctx.getConnectionId());

        // command
        ctx.setCommand(MysqlCommand.COM_PING);
        Assert.assertEquals(MysqlCommand.COM_PING, ctx.getCommand());

        // LoginTime
        ctx.loginTime = 1694002396223L;

        // Thread info
        Assert.assertNotNull(ctx.toThreadInfo(false));
        List<String> row = ctx.toThreadInfo(false).toRow(101, 1000, Optional.of("+08:00"));
        Assert.assertEquals(15, row.size());
        Assert.assertEquals("Yes", row.get(0));
        Assert.assertEquals("101", row.get(1));
        Assert.assertEquals("testUser", row.get(2));
        Assert.assertEquals("", row.get(3));
        Assert.assertEquals("2023-09-06 20:13:16", row.get(4));
        Assert.assertEquals("internal", row.get(5));
        Assert.assertEquals("testDb", row.get(6));
        Assert.assertEquals("Ping", row.get(7));
        Assert.assertEquals("1", row.get(8));
        Assert.assertEquals("OK", row.get(9));
        Assert.assertEquals("", row.get(10));
        Assert.assertEquals("", row.get(11));

        // Start time
        Assert.assertEquals(0, ctx.getStartTime());
        ctx.setStartTime();
        Assert.assertNotSame(0, ctx.getStartTime());

        // query id
        ctx.setQueryId(new TUniqueId(100, 200));
        Assert.assertEquals(new TUniqueId(100, 200), ctx.queryId());

        // Catalog
        Assert.assertNull(ctx.getEnv());
        ctx.setEnv(env);
        Assert.assertNotNull(ctx.getEnv());

        // clean up
        ctx.cleanup();
    }

    @Test
    public void testSleepTimeout() {
        ConnectContext ctx = new ConnectContext();
        ctx.setCommand(MysqlCommand.COM_SLEEP);

        // sleep no time out
        ctx.setStartTime();
        Assert.assertFalse(ctx.isKilled());
        long now = ctx.getStartTime() + ctx.getSessionVariable().getWaitTimeoutS() * 1000L - 1;
        ctx.checkTimeout(now);
        Assert.assertFalse(ctx.isKilled());

        // Timeout
        ctx.setStartTime();
        now = ctx.getStartTime() + ctx.getSessionVariable().getWaitTimeoutS() * 1000L + 1;
        ctx.setExecutor(executor);
        ctx.checkTimeout(now);
        Assert.assertTrue(ctx.isKilled());

        // user query timeout
        ctx.setStartTime();
        now = ctx.getStartTime() + auth.getQueryTimeout(qualifiedUser) * 1000L + 1;
        ctx.setExecutor(executor);
        ctx.checkTimeout(now);
        Assert.assertTrue(ctx.isKilled());

        // Kill
        ctx.kill(true);
        Assert.assertTrue(ctx.isKilled());
        ctx.kill(false);
        Assert.assertTrue(ctx.isKilled());

        // clean up
        ctx.cleanup();
    }

    @Test
    public void testOtherTimeout() {
        ConnectContext ctx = new ConnectContext();
        ctx.setCommand(MysqlCommand.COM_QUERY);

        // sleep no time out
        Assert.assertFalse(ctx.isKilled());
        ctx.setExecutor(executor);
        long now = ctx.getExecTimeoutS() * 1000L - 1;
        ctx.checkTimeout(now);
        Assert.assertFalse(ctx.isKilled());

        // Timeout
        ctx.setExecutor(executor);
        now = ctx.getExecTimeoutS() * 1000L + 1;
        ctx.checkTimeout(now);
        Assert.assertFalse(ctx.isKilled());

        // Kill
        ctx.kill(true);
        Assert.assertTrue(ctx.isKilled());

        // clean up
        ctx.cleanup();
    }

    @Test
    public void testThreadLocal() {
        ConnectContext ctx = new ConnectContext();
        Assert.assertNull(ConnectContext.get());
        ctx.setThreadLocalInfo();
        Assert.assertNotNull(ConnectContext.get());
        Assert.assertEquals(ctx, ConnectContext.get());
    }

    @Test
    public void testGetMaxExecMemByte() {
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("a", "%"));
        context.setEnv(env);
        long sessionValue = 2097153L;
        long propertyValue = 2097154L;
        // only session
        context.getSessionVariable().setMaxExecMemByte(sessionValue);
        long result = context.getMaxExecMemByte();
        Assert.assertEquals(sessionValue, result);
        // has property
        new Expectations() {
            {
                auth.getExecMemLimit(anyString);
                minTimes = 0;
                result = propertyValue;
            }
        };
        result = context.getMaxExecMemByte();
        Assert.assertEquals(propertyValue, result);
    }

    @Test
    public void testGetQueryTimeoutS() {
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("a", "%"));
        context.setEnv(env);
        int sessionValue = 1;
        int propertyValue = 2;
        // only session
        context.getSessionVariable().setQueryTimeoutS(sessionValue);
        long result = context.getQueryTimeoutS();
        Assert.assertEquals(sessionValue, result);
        // has property
        new Expectations() {
            {
                auth.getQueryTimeout(anyString);
                minTimes = 0;
                result = propertyValue;
            }
        };
        result = context.getQueryTimeoutS();
        Assert.assertEquals(propertyValue, result);
    }

    @Test
    public void testInsertQueryTimeoutS() {
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("a", "%"));
        context.setEnv(env);
        int sessionValue = 1;
        int propertyValue = 2;
        // only session
        context.getSessionVariable().setInsertTimeoutS(sessionValue);
        long result = context.getInsertTimeoutS();
        Assert.assertEquals(sessionValue, result);
        // has property
        new Expectations() {
            {
                auth.getInsertTimeout(anyString);
                minTimes = 0;
                result = propertyValue;
            }
        };
        result = context.getInsertTimeoutS();
        Assert.assertEquals(propertyValue, result);
    }

    @Test
    public void testResetQueryId() {
        ConnectContext context = new ConnectContext();
        Assert.assertNull(context.queryId);
        Assert.assertNull(context.lastQueryId);

        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        context.setQueryId(queryId);
        Assert.assertEquals(queryId, context.queryId);
        Assert.assertNull(context.lastQueryId);

        context.resetQueryId();
        Assert.assertNull(context.queryId);
        Assert.assertEquals(queryId, context.lastQueryId);

        UUID uuid2 = UUID.randomUUID();
        TUniqueId queryId2 = new TUniqueId(uuid2.getMostSignificantBits(), uuid2.getLeastSignificantBits());
        context.setQueryId(queryId2);
        Assert.assertEquals(queryId2, context.queryId);
        Assert.assertEquals(queryId, context.lastQueryId);
    }

    @Test
    public void testInitCatalogAndDbSinglePart() throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(env);

        new Expectations() {
            {
                env.changeDb(ctx, "testDb");
                minTimes = 0;
            }
        };

        Optional<Pair<ErrorCode, String>> result = ConnectContextUtil.initCatalogAndDb(ctx, "testDb");
        Assert.assertFalse(result.isPresent());
    }

    @Test
    public void testInitCatalogAndDbTwoParts() throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(env);

        new Expectations() {
            {
                env.changeCatalog(ctx, "catalog1");
                minTimes = 0;
                env.changeDb(ctx, "testDb");
                minTimes = 0;
            }
        };

        Optional<Pair<ErrorCode, String>> result = ConnectContextUtil.initCatalogAndDb(ctx, "catalog1.testDb");
        Assert.assertFalse(result.isPresent());
    }

    @Test
    public void testInitCatalogAndDbMultiplePartsWithNestedNamespaceEnabled() throws Exception {
        // Temporarily set the field value
        boolean originalValue = GlobalVariable.enableNestedNamespace;
        GlobalVariable.enableNestedNamespace = true;

        try {
            ConnectContext ctx = new ConnectContext();
            ctx.setEnv(env);

            new Expectations() {
                {
                    env.changeCatalog(ctx, "catalog1");
                    minTimes = 0;
                    env.changeDb(ctx, "ns1.ns2.testDb");
                    minTimes = 0;
                }
            };

            Optional<Pair<ErrorCode, String>> result = ConnectContextUtil.initCatalogAndDb(ctx,
                    "catalog1.ns1.ns2.testDb");
            Assert.assertFalse(result.isPresent());
        } finally {
            GlobalVariable.enableNestedNamespace = originalValue;
        }
    }

    @Test
    public void testInitCatalogAndDbMultiplePartsWithNestedNamespaceDisabled() throws Exception {
        // Ensure GlobalVariable.enableNestedNamespace is false (default)
        boolean originalValue = GlobalVariable.enableNestedNamespace;
        GlobalVariable.enableNestedNamespace = false;

        try {
            ConnectContext ctx = new ConnectContext();
            ctx.setEnv(env);

            Optional<Pair<ErrorCode, String>> result = ConnectContextUtil.initCatalogAndDb(ctx,
                    "catalog1.ns1.ns2.testDb");
            Assert.assertTrue(result.isPresent());
            Assert.assertEquals(ErrorCode.ERR_BAD_DB_ERROR, result.get().first);
            Assert.assertTrue(result.get().second.contains("Only one dot can be in the name"));
        } finally {
            GlobalVariable.enableNestedNamespace = originalValue;
        }
    }

    @Test
    public void testInitCatalogAndDbWithFourPartsNestedNamespaceEnabled() throws Exception {
        // Temporarily set GlobalVariable.enableNestedNamespace to be true
        boolean originalValue = GlobalVariable.enableNestedNamespace;
        GlobalVariable.enableNestedNamespace = true;

        try {
            ConnectContext ctx = new ConnectContext();
            ctx.setEnv(env);

            new Expectations() {
                {
                    env.changeCatalog(ctx, "catalog1");
                    minTimes = 0;
                    env.changeDb(ctx, "ns1.ns2.ns3.testDb");
                    minTimes = 0;
                }
            };

            Optional<Pair<ErrorCode, String>> result = ConnectContextUtil.initCatalogAndDb(ctx,
                    "catalog1.ns1.ns2.ns3.testDb");
            Assert.assertFalse(result.isPresent());
        } finally {
            GlobalVariable.enableNestedNamespace = originalValue;
        }
    }

    @Test
    public void testInitCatalogAndDbWithChangeCatalogException() throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(env);

        new Expectations() {
            {
                env.changeCatalog(ctx, "invalidCatalog");
                result = new DdlException("Catalog not found");
                minTimes = 0;
            }
        };

        Optional<Pair<ErrorCode, String>> result = ConnectContextUtil.initCatalogAndDb(ctx, "invalidCatalog.testDb");
        Assert.assertTrue(result.isPresent());
        Assert.assertTrue(result.get().second.contains("Catalog not found"));
    }

    @Test
    public void testInitCatalogAndDbWithChangeDbException() throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(env);

        new Expectations() {
            {
                env.changeDb(ctx, "invalidDb");
                result = new DdlException("Database not found");
                minTimes = 0;
            }
        };

        Optional<Pair<ErrorCode, String>> result = ConnectContextUtil.initCatalogAndDb(ctx, "invalidDb");
        Assert.assertTrue(result.isPresent());
        Assert.assertTrue(result.get().second.contains("Database not found"));
    }

    @Test
    public void testInitCatalogAndDbEmptyString() throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(env);

        new Expectations() {
            {
                env.changeDb(ctx, "");
                minTimes = 0;
            }
        };

        Optional<Pair<ErrorCode, String>> result = ConnectContextUtil.initCatalogAndDb(ctx, "");
        Assert.assertFalse(result.isPresent());
    }

    @Test
    public void testInitCatalogAndDbNullString() {
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(env);

        // This should cause a NullPointerException when calling split on null
        try {
            ConnectContextUtil.initCatalogAndDb(ctx, null);
            Assert.fail("Expected NullPointerException");
        } catch (NullPointerException e) {
            // Expected behavior
        }
    }

    @Test
    public void testGetCloudCluster() throws Exception {
        // Setup: enable cloud mode by setting cloud_unique_id
        String originalCloudUniqueId = Config.cloud_unique_id;
        try {
            Config.cloud_unique_id = "test-cloud-id";

            ConnectContext ctx = new ConnectContext();
            ctx.setEnv(env);
            ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("testUser", "%"));

            // Test 1: Cluster from session variable (step 1)
            // This tests: "Get cluster from session variable (set by `use @` command or setCloudCluster())"
            ctx.setCloudCluster("session_cluster");
            // Verify that setCloudCluster sets session variable
            Assert.assertEquals("session_cluster", ctx.getSessionVariable().getCloudCluster());
            new Expectations() {
                {
                    env.getCurrentSystemInfo();
                    result = cloudSystemInfoService;
                }
            };
            String cluster = ctx.getCloudCluster(false);
            Assert.assertEquals("session_cluster", cluster);

            // Test 2: Cluster from user default (step 2)
            // This tests: "Get cluster from user's default cluster property if set"
            ctx.setCloudCluster(null); // Clear session cluster
            ctx.cloudCluster = null; // Clear cached cluster
            new Expectations() {
                {
                    env.getAuth();
                    result = auth;
                    auth.getDefaultCloudCluster("testUser");
                    result = "user_default_cluster";
                    env.getCurrentSystemInfo();
                    result = cloudSystemInfoService;
                    cloudSystemInfoService.getCloudClusterNames();
                    result = Lists.newArrayList("user_default_cluster", "other_cluster");
                }
            };
            cluster = ctx.getCloudCluster(false);
            Assert.assertEquals("user_default_cluster", cluster);

            // Test 3: Cluster from this.cloudCluster cache (step 3)
            // This tests: "Get cluster from cached variable (this.cloudCluster) if available"
            ctx.setCloudCluster(null); // Clear session cluster
            ctx.cloudCluster = "cached_cluster"; // Set cached cluster (from previous policy selection)
            new Expectations() {
                {
                    env.getAuth();
                    result = auth;
                    auth.getDefaultCloudCluster("testUser");
                    result = null; // No user default
                    env.getCurrentSystemInfo();
                    result = cloudSystemInfoService;
                }
            };
            cluster = ctx.getCloudCluster(false);
            Assert.assertEquals("cached_cluster", cluster);

            // Test 4: Cluster from policy (step 4)
            // This tests: "Choose an authorized cluster by policy if all preceding conditions failed"
            ctx.setCloudCluster(null); // Clear session cluster
            ctx.cloudCluster = null; // Clear cached cluster
            new Expectations() {
                {
                    env.getAuth();
                    result = auth;
                    auth.getDefaultCloudCluster("testUser");
                    result = null; // No user default
                    env.getCurrentSystemInfo();
                    result = cloudSystemInfoService;
                    cloudSystemInfoService.getCloudClusterNames();
                    result = Lists.newArrayList("policy_cluster1", "policy_cluster2");
                    env.getAccessManager();
                    result = accessManager;
                    accessManager.checkCloudPriv((UserIdentity) any, "policy_cluster2",
                            PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER);
                    result = true;
                    cloudSystemInfoService.isStandByComputeGroup("policy_cluster2");
                    result = false;
                    cloudSystemInfoService.getBackendsByClusterName("policy_cluster2");
                    result = Lists.newArrayList(backend);
                    backend.isAlive();
                    result = true;
                }
            };
            cluster = ctx.getCloudCluster(false);
            Assert.assertEquals("policy_cluster2", cluster);
            // Verify cache is set for subsequent calls
            Assert.assertEquals("policy_cluster2", ctx.cloudCluster);

            // Test 5: Priority order - session variable takes precedence over this.cloudCluster
            ctx.setCloudCluster("session_cluster2");
            ctx.cloudCluster = "cached_cluster2"; // This should be ignored
            new Expectations() {
                {
                    env.getCurrentSystemInfo();
                    result = cloudSystemInfoService;
                }
            };
            cluster = ctx.getCloudCluster(false);
            Assert.assertEquals("session_cluster2", cluster); // Session variable wins

            // Test 6: Priority order - user this.cloudCluster over default takes precedence
            ctx.setCloudCluster(null); // Clear session cluster
            ctx.cloudCluster = "cached_cluster3"; // This should be ignored
            new Expectations() {
                {
                    env.getAuth();
                    result = auth;
                    auth.getDefaultCloudCluster("testUser");
                    result = "user_default_cluster2";
                    env.getCurrentSystemInfo();
                    result = cloudSystemInfoService;
                    cloudSystemInfoService.getCloudClusterNames();
                    result = Lists.newArrayList("user_default_cluster2", "other_cluster");
                }
            };
            cluster = ctx.getCloudCluster(false);
            Assert.assertEquals("cached_cluster3", cluster); // User this.cloudCluster wins

            // Test 7: No cluster available - should throw exception
            ctx.setCloudCluster(null);
            ctx.cloudCluster = null;
            new Expectations() {
                {
                    env.getAuth();
                    result = auth;
                    auth.getDefaultCloudCluster("testUser");
                    result = null;
                    env.getCurrentSystemInfo();
                    result = cloudSystemInfoService;
                    cloudSystemInfoService.getCloudClusterNames();
                    result = Lists.newArrayList("unauthorized_cluster");
                    env.getAccessManager();
                    result = accessManager;
                    accessManager.checkCloudPriv((UserIdentity) any, anyString,
                            PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER);
                    result = false; // No authorized cluster
                }
            };
            try {
                ctx.getCloudCluster(true);
                Assert.fail("Expected ComputeGroupException");
            } catch (ComputeGroupException e) {
                Assert.assertEquals(ComputeGroupException.FailedTypeEnum.CURRENT_USER_NO_AUTH_TO_USE_ANY_COMPUTE_GROUP,
                        e.getFailedType());
            }
        } finally {
            Config.cloud_unique_id = originalCloudUniqueId;
        }
    }
}
