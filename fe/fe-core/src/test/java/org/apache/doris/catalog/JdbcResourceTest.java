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

import org.apache.doris.analysis.AccessTestUtil;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.CreateResourceStmt;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class JdbcResourceTest {

    private final ResourceMgr resourceMgr = new ResourceMgr();

    private Map<String, String> jdbcProperties;

    private Analyzer analyzer;

    @Before
    public void setUp() {
        FeConstants.runningUnitTest = true;
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        jdbcProperties = Maps.newHashMap();
        jdbcProperties.put("type", "jdbc");
        jdbcProperties.put("user", "postgres");
        jdbcProperties.put("password", "");
        jdbcProperties.put("jdbc_url", "jdbc:postgresql://127.0.0.1:5432/postgres?currentSchema=doris_test");
        jdbcProperties.put("driver_url", "postgresql-42.5.0.jar");
        jdbcProperties.put("driver_class", "org.postgresql.Driver");
        jdbcProperties.put("checksum", "20c8228267b6c9ce620fddb39467d3eb");
    }

    @Test
    public void testJdbcResourceCreateWithDefaultProperties(@Mocked Env env,
            @Injectable AccessControllerManager accessManager)
            throws UserException {
        new Expectations() {
            {
                env.getAccessManager();
                result = accessManager;
                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };

        jdbcProperties.remove("checksum");

        CreateResourceStmt stmt = new CreateResourceStmt(true, false, "jdbc_resource_pg_14", jdbcProperties);

        stmt.analyze(analyzer);

        resourceMgr.createResource(stmt);

        JdbcResource jdbcResource = (JdbcResource) resourceMgr.getResource("jdbc_resource_pg_14");


        // Verify the default properties were applied during the replay
        Map<String, String> properties = jdbcResource.getCopiedProperties();
        Assert.assertEquals("1", properties.get("connection_pool_min_size"));
        Assert.assertEquals("30", properties.get("connection_pool_max_size"));
        Assert.assertEquals("1800000", properties.get("connection_pool_max_life_time"));
        Assert.assertEquals("5000", properties.get("connection_pool_max_wait_time"));
        Assert.assertEquals("false", properties.get("connection_pool_keep_alive"));
    }

    @Test
    public void testJdbcResourceReplayWithDefaultProperties() {

        JdbcResource jdbcResource = new JdbcResource("jdbc_resource_pg_14", jdbcProperties);

        // Replay the resource creation to simulate the edit log replay
        resourceMgr.replayCreateResource(jdbcResource);

        // Retrieve the replayed resource
        Resource replayedResource = resourceMgr.getResource("jdbc_resource_pg_14");

        Assert.assertNotNull(replayedResource);
        Assert.assertTrue(replayedResource instanceof JdbcResource);

        // Verify the default properties were applied during the replay
        Map<String, String> properties = replayedResource.getCopiedProperties();
        Assert.assertEquals("1", properties.get("connection_pool_min_size"));
        Assert.assertEquals("30", properties.get("connection_pool_max_size"));
        Assert.assertEquals("1800000", properties.get("connection_pool_max_life_time"));
        Assert.assertEquals("5000", properties.get("connection_pool_max_wait_time"));
        Assert.assertEquals("false", properties.get("connection_pool_keep_alive"));
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

        Assert.assertNotNull(replayedResource);
        Assert.assertTrue(replayedResource instanceof JdbcResource);

        // Verify the modified properties were applied during the replay
        Map<String, String> properties = replayedResource.getCopiedProperties();
        Assert.assertEquals("2", properties.get("connection_pool_min_size"));
        Assert.assertEquals("20", properties.get("connection_pool_max_size"));
        Assert.assertEquals("3600000", properties.get("connection_pool_max_life_time"));
        Assert.assertEquals("10000", properties.get("connection_pool_max_wait_time"));
        Assert.assertEquals("true", properties.get("connection_pool_keep_alive"));
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
        Assert.assertEquals("2", properties.get("connection_pool_min_size"));
        resourceMgr.replayCreateResource(replayedResource);
        Resource replayedResource2 = resourceMgr.getResource("jdbc_resource_pg_14");
        Map<String, String> properties2 = replayedResource2.getCopiedProperties();
        Assert.assertEquals("2", properties2.get("connection_pool_min_size"));
    }

    @Test
    public void testHandleJdbcUrlForMySql() throws DdlException {
        String inputUrl = "jdbc:mysql://127.0.0.1:3306/test";
        String resultUrl = JdbcResource.handleJdbcUrl(inputUrl);

        // Check if the result URL contains the necessary delimiters for MySQL
        Assert.assertTrue(resultUrl.contains("?"));
        Assert.assertTrue(resultUrl.contains("&"));
    }

    @Test
    public void testHandleJdbcUrlForSqlServerWithoutParams() throws DdlException {
        String inputUrl = "jdbc:sqlserver://127.0.0.1:1433;databaseName=doris_test";
        String resultUrl = JdbcResource.handleJdbcUrl(inputUrl);

        // Ensure that the result URL for SQL Server doesn't have '?' or '&'
        Assert.assertFalse(resultUrl.contains("?"));
        Assert.assertFalse(resultUrl.contains("&"));

        // Ensure the result URL still contains ';'
        Assert.assertTrue(resultUrl.contains(";"));
    }

    @Test
    public void testHandleJdbcUrlForSqlServerWithParams() throws DdlException {
        String inputUrl
                = "jdbc:sqlserver://127.0.0.1:1433;encrypt=false;databaseName=doris_test;trustServerCertificate=false";
        String resultUrl = JdbcResource.handleJdbcUrl(inputUrl);

        // Ensure that the result URL for SQL Server doesn't have '?' or '&'
        Assert.assertFalse(resultUrl.contains("?"));
        Assert.assertFalse(resultUrl.contains("&"));

        // Ensure the result URL still contains ';'
        Assert.assertTrue(resultUrl.contains(";"));
    }

    @Test
    public void testJdbcDriverPtah() {
        String driverPath = "postgresql-42.5.0.jar";
        Config.jdbc_driver_secure_path = "";
        String fullPath = JdbcResource.getFullDriverUrl(driverPath);
        Assert.assertEquals(fullPath, "file://" + Config.jdbc_drivers_dir + "/" + driverPath);
        Config.jdbc_driver_secure_path = "file:///jdbc/;http://jdbc";
        String driverPath2 = "file:///postgresql-42.5.0.jar";
        Exception exception = Assert.assertThrows(IllegalArgumentException.class, () -> {
            JdbcResource.getFullDriverUrl(driverPath2);
        });
        Assert.assertEquals("Driver URL does not match any allowed paths: file:///postgresql-42.5.0.jar", exception.getMessage());
    }
}
