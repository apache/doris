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

import org.apache.doris.blockrule.SqlBlockRuleMgr;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.load.DppConfig;
import org.apache.doris.mysql.privilege.UserProperty;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class UserPropertyTest {
    private FakeEnv fakeEnv;
    @Mocked
    private Env env;
    @Mocked
    private SqlBlockRuleMgr sqlBlockRuleMgr;

    @Before
    public void setUp() {
        new Expectations(env) {
            {
                env.getSqlBlockRuleMgr();
                minTimes = 0;
                result = sqlBlockRuleMgr;

                sqlBlockRuleMgr.existRule("rule1");
                minTimes = 0;
                result = true;

                sqlBlockRuleMgr.existRule("rule2");
                minTimes = 0;
                result = true;

                sqlBlockRuleMgr.existRule("test1");
                minTimes = 0;
                result = true;

                sqlBlockRuleMgr.existRule("test2");
                minTimes = 0;
                result = true;

                sqlBlockRuleMgr.existRule("test3");
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testNormal() throws IOException, DdlException {
        // mock catalog
        fakeEnv = new FakeEnv();
        FakeEnv.setMetaVersion(FeConstants.meta_version);

        UserProperty property = new UserProperty("root");
        property.getResource().updateGroupShare("low", 991);
        // To image
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(byteStream);
        property.write(outputStream);
        outputStream.flush();
        DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(byteStream.toByteArray()));
        UserProperty newProperty = UserProperty.read(inputStream);

        Assert.assertEquals(991, newProperty.getResource().getShareByGroup().get("low").intValue());
    }

    @Test
    public void testUpdate() throws UserException {
        List<Pair<String, String>> properties = Lists.newArrayList();
        properties.add(Pair.of("MAX_USER_CONNECTIONS", "100"));
        properties.add(Pair.of("resource.cpu_share", "101"));
        properties.add(Pair.of("quota.normal", "102"));
        properties.add(Pair.of("load_cluster.dpp-cluster.hadoop_palo_path", "/user/palo2"));
        properties.add(Pair.of("default_load_cluster", "dpp-cluster"));
        properties.add(Pair.of("max_qUERY_instances", "3000"));
        properties.add(Pair.of("sql_block_rules", "rule1,rule2"));
        properties.add(Pair.of("cpu_resource_limit", "2"));
        properties.add(Pair.of("query_timeout", "500"));

        UserProperty userProperty = new UserProperty();
        userProperty.update(properties);
        Assert.assertEquals(100, userProperty.getMaxConn());
        Assert.assertEquals(101, userProperty.getResource().getResource().getByDesc("cpu_share"));
        Assert.assertEquals(102, userProperty.getResource().getShareByGroup().get("normal").intValue());
        Assert.assertEquals("/user/palo2", userProperty.getLoadClusterInfo("dpp-cluster").second.getPaloPath());
        Assert.assertEquals("dpp-cluster", userProperty.getDefaultLoadCluster());
        Assert.assertEquals(3000, userProperty.getMaxQueryInstances());
        Assert.assertEquals(new String[]{"rule1", "rule2"}, userProperty.getSqlBlockRules());
        Assert.assertEquals(2, userProperty.getCpuResourceLimit());
        Assert.assertEquals(500, userProperty.getQueryTimeout());

        // fetch property
        List<List<String>> rows = userProperty.fetchProperty();
        for (List<String> row : rows) {
            String key = row.get(0);
            String value = row.get(1);

            if (key.equalsIgnoreCase("max_user_connections")) {
                Assert.assertEquals("100", value);
            } else if (key.equalsIgnoreCase("resource.cpu_share")) {
                Assert.assertEquals("101", value);
            } else if (key.equalsIgnoreCase("quota.normal")) {
                Assert.assertEquals("102", value);
            } else if (key.equalsIgnoreCase("load_cluster.dpp-cluster.hadoop_palo_path")) {
                Assert.assertEquals("/user/palo2", value);
            } else if (key.equalsIgnoreCase("default_load_cluster")) {
                Assert.assertEquals("dpp-cluster", value);
            } else if (key.equalsIgnoreCase("max_query_instances")) {
                Assert.assertEquals("3000", value);
            } else if (key.equalsIgnoreCase("sql_block_rules")) {
                Assert.assertEquals("rule1,rule2", value);
            } else if (key.equalsIgnoreCase("cpu_resource_limit")) {
                Assert.assertEquals("2", value);
            } else if (key.equalsIgnoreCase("query_timeout")) {
                Assert.assertEquals("500", value);
            }
        }

        // get cluster info
        DppConfig dppConfig = userProperty.getLoadClusterInfo("dpp-cluster").second;
        Assert.assertEquals(8070, dppConfig.getHttpPort());

        // set palo path null
        properties.clear();
        properties.add(Pair.of("load_cluster.dpp-cluster.hadoop_palo_path", null));
        userProperty.update(properties);
        Assert.assertEquals(null, userProperty.getLoadClusterInfo("dpp-cluster").second.getPaloPath());

        // remove dpp-cluster
        properties.clear();
        properties.add(Pair.of("load_cluster.dpp-cluster", null));
        Assert.assertEquals("dpp-cluster", userProperty.getDefaultLoadCluster());
        userProperty.update(properties);
        Assert.assertEquals(null, userProperty.getLoadClusterInfo("dpp-cluster").second);
        Assert.assertEquals(null, userProperty.getDefaultLoadCluster());

        // sql block rule
        properties.clear();
        properties.add(Pair.of("sql_block_rules", ""));
        userProperty.update(properties);
        Assert.assertEquals(1, userProperty.getSqlBlockRules().length);
        properties.clear();
        properties.add(Pair.of("sql_block_rules", "test1, test2,test3"));
        userProperty.update(properties);
        Assert.assertEquals(3, userProperty.getSqlBlockRules().length);
    }

    @Test
    public void testValidation() throws UserException {
        List<Pair<String, String>> properties = Lists.newArrayList();
        properties.add(Pair.of("cpu_resource_limit", "-1"));
        UserProperty userProperty = new UserProperty();
        userProperty.update(properties);
        Assert.assertEquals(-1, userProperty.getCpuResourceLimit());

        properties = Lists.newArrayList();
        properties.add(Pair.of("cpu_resource_limit", "-2"));
        userProperty = new UserProperty();
        try {
            userProperty.update(properties);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("is not valid"));
        }
        Assert.assertEquals(-1, userProperty.getCpuResourceLimit());
    }
}
