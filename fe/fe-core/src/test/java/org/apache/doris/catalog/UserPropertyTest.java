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
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.UserProperty;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
    public void testUpdate() throws UserException {
        List<Pair<String, String>> properties = Lists.newArrayList();
        properties.add(Pair.of("MAX_USER_CONNECTIONS", "100"));
        properties.add(Pair.of("max_qUERY_instances", "3000"));
        properties.add(Pair.of("parallel_fragment_exec_instance_num", "2000"));
        properties.add(Pair.of("sql_block_rules", "rule1,rule2"));
        properties.add(Pair.of("cpu_resource_limit", "2"));
        properties.add(Pair.of("query_timeout", "500"));

        UserProperty userProperty = new UserProperty();
        userProperty.update(properties);
        Assert.assertEquals(100, userProperty.getMaxConn());
        Assert.assertEquals(3000, userProperty.getMaxQueryInstances());
        Assert.assertEquals(2000, userProperty.getParallelFragmentExecInstanceNum());
        Assert.assertEquals(new String[]{"rule1", "rule2"}, userProperty.getSqlBlockRules());
        Assert.assertEquals(2, userProperty.getCpuResourceLimit());
        Assert.assertEquals(500, userProperty.getQueryTimeout());
        Assert.assertEquals(Sets.newHashSet(), userProperty.getCopiedResourceTags());

        // fetch property
        List<List<String>> rows = userProperty.fetchProperty();
        for (List<String> row : rows) {
            String key = row.get(0);
            String value = row.get(1);

            if (key.equalsIgnoreCase("max_user_connections")) {
                Assert.assertEquals("100", value);
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
        // we should allow query_timeout  < 0, otherwise, not have command reset query_timeout of user
        properties = Lists.newArrayList();
        properties.add(Pair.of("query_timeout", "-2"));
        userProperty = new UserProperty();
        userProperty.update(properties);
        Assert.assertEquals(-2, userProperty.getQueryTimeout());
        // we should allow insert_timeout  < 0, otherwise, not have command reset insert_timeout of user
        properties = Lists.newArrayList();
        properties.add(Pair.of("insert_timeout", "-2"));
        userProperty = new UserProperty();
        userProperty.update(properties);
        Assert.assertEquals(-2, userProperty.getInsertTimeout());
    }

    @Test
    public void testUpdateInitCatalog(@Mocked Env env, @Mocked CatalogMgr catalogMgr) throws UserException {
        CatalogIf internalCatalog = new InternalCatalog();
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 1;
                result = env;

                env.getCatalogMgr();
                minTimes = 1;
                result = catalogMgr;

                catalogMgr.getCatalog(anyString);
                minTimes = 2;
                result = null;
                result = internalCatalog;
            }
        };

        // for non exist catalog, use internal
        List<Pair<String, String>> properties = Lists.newArrayList();
        properties.add(Pair.of("default_init_catalog", "non_exist_catalog"));
        UserProperty userProperty = new UserProperty();
        try {
            userProperty.update(properties);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("not exists"));
        }
        Assert.assertEquals("internal",  userProperty.getInitCatalog());

        // for exist catalog, use it directly
        properties = Lists.newArrayList();
        properties.add(Pair.of("default_init_catalog", "exist_catalog"));
        userProperty = new UserProperty();
        userProperty.update(properties);
        Assert.assertEquals("exist_catalog",  userProperty.getInitCatalog());
    }
}
