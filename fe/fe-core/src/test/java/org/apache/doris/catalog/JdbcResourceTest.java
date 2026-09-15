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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.connector.ConnectorFactory;
import org.apache.doris.connector.ConnectorPluginManager;
import org.apache.doris.connector.jdbc.JdbcConnectorProvider;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.CreateResourceCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateResourceInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

public class JdbcResourceTest {

    private final ResourceMgr resourceMgr = new ResourceMgr();

    private Map<String, String> jdbcProperties;

    @BeforeEach
    public void setUp() {
        FeConstants.runningUnitTest = true;
        // A JDBC resource is validated by the jdbc connector plugin, the way a JDBC catalog is; the plugin
        // manager is a static singleton shared across the fork, so start from one holding exactly that provider.
        ConnectorPluginManager manager = new ConnectorPluginManager();
        manager.registerProvider(new JdbcConnectorProvider());
        ConnectorFactory.initPluginManager(manager);
        jdbcProperties = Maps.newHashMap();
        jdbcProperties.put("type", "jdbc");
        jdbcProperties.put("user", "postgres");
        jdbcProperties.put("password", "");
        jdbcProperties.put("jdbc_url", "jdbc:postgresql://127.0.0.1:5432/postgres?currentSchema=doris_test");
        jdbcProperties.put("driver_url", "postgresql-42.5.0.jar");
        jdbcProperties.put("driver_class", "org.postgresql.Driver");
        jdbcProperties.put("checksum", "20c8228267b6c9ce620fddb39467d3eb");
    }

    @AfterEach
    public void tearDown() {
        ConnectorFactory.initPluginManager(new ConnectorPluginManager());
    }

    private void createResource(String name, Map<String, String> properties) throws UserException {
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            EditLog editLog = Mockito.mock(EditLog.class);
            AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            Mockito.when(env.getAccessManager()).thenReturn(accessManager);
            Mockito.when(accessManager.checkGlobalPriv(Mockito.nullable(ConnectContext.class),
                    Mockito.eq(PrivPredicate.ADMIN))).thenReturn(true);
            CreateResourceCommand createResourceCommand = new CreateResourceCommand(
                    new CreateResourceInfo(true, false, name, ImmutableMap.copyOf(properties)));
            createResourceCommand.getInfo().validate();
            resourceMgr.createResource(createResourceCommand);
        }
    }

    @Test
    public void testJdbcResourceCreateWithDefaultProperties() throws UserException {
        jdbcProperties.remove("checksum");
        createResource("jdbc_resource_pg_14", jdbcProperties);

        JdbcResource jdbcResource = (JdbcResource) resourceMgr.getResource("jdbc_resource_pg_14");

        // Verify the default properties were applied on creation
        Map<String, String> properties = jdbcResource.getCopiedProperties();
        Assertions.assertEquals("1", properties.get("connection_pool_min_size"));
        Assertions.assertEquals("30", properties.get("connection_pool_max_size"));
        Assertions.assertEquals("1800000", properties.get("connection_pool_max_life_time"));
        Assertions.assertEquals("5000", properties.get("connection_pool_max_wait_time"));
        Assertions.assertEquals("false", properties.get("connection_pool_keep_alive"));
    }

    @Test
    public void testJdbcResourceReplayWithDefaultProperties() {

        JdbcResource jdbcResource = new JdbcResource("jdbc_resource_pg_14", jdbcProperties);

        // Replay the resource creation to simulate the edit log replay
        resourceMgr.replayCreateResource(jdbcResource);

        // Retrieve the replayed resource
        Resource replayedResource = resourceMgr.getResource("jdbc_resource_pg_14");

        Assertions.assertNotNull(replayedResource);
        Assertions.assertTrue(replayedResource instanceof JdbcResource);

        // Verify the default properties were applied during the replay
        Map<String, String> properties = replayedResource.getCopiedProperties();
        Assertions.assertEquals("1", properties.get("connection_pool_min_size"));
        Assertions.assertEquals("30", properties.get("connection_pool_max_size"));
        Assertions.assertEquals("1800000", properties.get("connection_pool_max_life_time"));
        Assertions.assertEquals("5000", properties.get("connection_pool_max_wait_time"));
        Assertions.assertEquals("false", properties.get("connection_pool_keep_alive"));
    }

    @Test
    public void testJdbcResourceReplayWithSetProperties() {

        // Add some properties to the JDBC properties
        jdbcProperties.put("connection_pool_min_size", "2");
        jdbcProperties.put("connection_pool_max_size", "20");
        jdbcProperties.put("connection_pool_max_life_time", "3600000");
        jdbcProperties.put("connection_pool_max_wait_time", "10000");
        jdbcProperties.put("connection_pool_keep_alive", "true");

        JdbcResource jdbcResource = new JdbcResource("jdbc_resource_pg_14", jdbcProperties);

        // Replay the resource creation to simulate the edit log replay
        resourceMgr.replayCreateResource(jdbcResource);

        // Retrieve the replayed resource
        Resource replayedResource = resourceMgr.getResource("jdbc_resource_pg_14");

        Assertions.assertNotNull(replayedResource);
        Assertions.assertTrue(replayedResource instanceof JdbcResource);

        // Verify the modified properties were applied during the replay
        Map<String, String> properties = replayedResource.getCopiedProperties();
        Assertions.assertEquals("2", properties.get("connection_pool_min_size"));
        Assertions.assertEquals("20", properties.get("connection_pool_max_size"));
        Assertions.assertEquals("3600000", properties.get("connection_pool_max_life_time"));
        Assertions.assertEquals("10000", properties.get("connection_pool_max_wait_time"));
        Assertions.assertEquals("true", properties.get("connection_pool_keep_alive"));
    }

    @Test
    public void testJdbcResourceReplayWithModifiedAfterSetDefaultProperties() throws DdlException {
        JdbcResource jdbcResource = new JdbcResource("jdbc_resource_pg_14", jdbcProperties);

        // Replay the resource creation to simulate the edit log replay
        resourceMgr.replayCreateResource(jdbcResource);

        // Retrieve the replayed resource
        Resource replayedResource = resourceMgr.getResource("jdbc_resource_pg_14");
        Map<String, String> newProperties = Maps.newHashMap();
        newProperties.put(JdbcResource.CONNECTION_POOL_MIN_SIZE, "2");
        replayedResource.modifyProperties(newProperties);
        Map<String, String> properties = replayedResource.getCopiedProperties();
        Assertions.assertEquals("2", properties.get("connection_pool_min_size"));
        resourceMgr.replayCreateResource(replayedResource);
        Resource replayedResource2 = resourceMgr.getResource("jdbc_resource_pg_14");
        Map<String, String> properties2 = replayedResource2.getCopiedProperties();
        Assertions.assertEquals("2", properties2.get("connection_pool_min_size"));
    }

    @Test
    public void testCreateShowsTheSameRowsAsBefore() throws UserException {
        // 21 persisted keys plus the checksum the connector records (blank under unit tests, where the
        // driver jar does not exist): the row count SHOW RESOURCES has always shown for a JDBC resource.
        jdbcProperties.remove("checksum");
        createResource("jdbc_resource_rows", jdbcProperties);
        JdbcResource resource = (JdbcResource) resourceMgr.getResource("jdbc_resource_rows");
        Map<String, String> properties = resource.getCopiedProperties();
        Assertions.assertEquals(22, properties.size(), properties.toString());
        Assertions.assertTrue(properties.containsKey(JdbcResource.CHECK_SUM));
        Assertions.assertNotNull(properties.get(JdbcResource.CREATE_TIME));
        // The url is stored as the user wrote it; nothing in the engine rewrites JDBC urls any more.
        Assertions.assertEquals("jdbc:postgresql://127.0.0.1:5432/postgres?currentSchema=doris_test",
                properties.get(JdbcResource.JDBC_URL));
    }

    @Test
    public void testCreateIsValidatedByTheJdbcConnector() {
        // The connector's rules apply to a resource exactly as to a catalog: a required key, a pool bound,
        // the driver_url grammar. The engine holds no copy of these rules.
        jdbcProperties.remove("checksum");
        jdbcProperties.remove("driver_class");
        DdlException missing = Assertions.assertThrows(DdlException.class,
                () -> createResource("jdbc_resource_bad", jdbcProperties));
        Assertions.assertTrue(missing.getMessage().contains("driver_class"), missing.getMessage());

        jdbcProperties.put("driver_class", "org.postgresql.Driver");
        jdbcProperties.put("connection_pool_max_size", "0");
        DdlException pool = Assertions.assertThrows(DdlException.class,
                () -> createResource("jdbc_resource_bad", jdbcProperties));
        Assertions.assertTrue(pool.getMessage().contains("connection_pool_max_size"), pool.getMessage());

        jdbcProperties.put("connection_pool_max_size", "10");
        jdbcProperties.put("driver_url", "../escape.jar");
        DdlException traversal = Assertions.assertThrows(DdlException.class,
                () -> createResource("jdbc_resource_bad", jdbcProperties));
        Assertions.assertTrue(traversal.getMessage().contains("driver_url"), traversal.getMessage());
    }

    @Test
    public void testUnknownPropertyIsRejected() {
        jdbcProperties.remove("checksum");
        jdbcProperties.put("no_such_property", "x");
        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> createResource("jdbc_resource_bad", jdbcProperties));
        Assertions.assertTrue(e.getMessage().contains("no_such_property"), e.getMessage());
    }

    @Test
    public void testCreateWithoutTheJdbcPluginFailsLoud() {
        jdbcProperties.remove("checksum");
        ConnectorFactory.initPluginManager(new ConnectorPluginManager());
        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> createResource("jdbc_resource_noplugin", jdbcProperties));
        Assertions.assertTrue(e.getMessage().contains("connector plugin"), e.getMessage());
    }

    @Test
    public void testProcNodeDataMasksThePassword() throws UserException {
        jdbcProperties.remove("checksum");
        jdbcProperties.put("password", "secret");
        createResource("jdbc_resource_masked", jdbcProperties);
        JdbcResource resource = (JdbcResource) resourceMgr.getResource("jdbc_resource_masked");
        org.apache.doris.common.proc.BaseProcResult result = new org.apache.doris.common.proc.BaseProcResult();
        resource.getProcNodeData(result);
        boolean sawPassword = false;
        for (List<String> row : result.getRows()) {
            if (row.get(2).equals(JdbcResource.PASSWORD)) {
                sawPassword = true;
                Assertions.assertEquals("", row.get(3), "the password row must be blanked");
            }
            Assertions.assertEquals("jdbc", row.get(1));
        }
        Assertions.assertTrue(sawPassword);
        Assertions.assertEquals(22, result.getRows().size());
    }
}
