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

package org.apache.doris.blockrule;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.persist.gson.GsonUtils;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SqlBlockRuleMgrTest {
    @BeforeClass
    public static void setUp() {
        MetricRepo.init();
    }

    @Test
    public void testToInfoString() {
        SqlBlockRuleMgr mgr = new SqlBlockRuleMgr();
        Assert.assertTrue(mgr.getNameToSqlBlockRuleMap() instanceof ConcurrentHashMap);
        SqlBlockRule rule = new SqlBlockRule();
        mgr.getNameToSqlBlockRuleMap().put("r1", rule);
        String mgrJson = GsonUtils.GSON.toJson(mgr);
        SqlBlockRuleMgr mgrNew = GsonUtils.GSON.fromJson(mgrJson, SqlBlockRuleMgr.class);
        Map<String, SqlBlockRule> nameToSqlBlockRuleMap = mgrNew.getNameToSqlBlockRuleMap();
        Assert.assertTrue(nameToSqlBlockRuleMap instanceof ConcurrentHashMap);
        Assert.assertTrue(nameToSqlBlockRuleMap.containsKey("r1"));
    }

    @Test
    public void testRuleSerializeRequirePartitionFilter() {
        SqlBlockRule rule = new SqlBlockRule("r1", "NULL", "NULL", 0L, 0L, 0L,
                true, true, true);
        String json = GsonUtils.GSON.toJson(rule);
        SqlBlockRule roundTrip = GsonUtils.GSON.fromJson(json, SqlBlockRule.class);
        Assert.assertTrue(roundTrip.getRequirePartitionFilter());
    }

    @Test
    public void testShowInfoUseNumericBooleanForRequirePartitionFilter() {
        SqlBlockRule enabledRule = new SqlBlockRule("r1", "NULL", "NULL", 0L, 0L, 0L,
                true, true, true);
        List<String> enabledShowInfo = enabledRule.getShowInfo();
        Assert.assertEquals(9, enabledShowInfo.size());
        Assert.assertEquals("1", enabledShowInfo.get(8));

        SqlBlockRule disabledRule = new SqlBlockRule("r2", "NULL", "NULL", 0L, 0L, 0L,
                false, true, true);
        List<String> disabledShowInfo = disabledRule.getShowInfo();
        Assert.assertEquals("0", disabledShowInfo.get(8));
    }

    @Test
    public void testConstructorPlaceRequirePartitionFilterBeforeGlobal() {
        SqlBlockRule rule = new SqlBlockRule("r1", "NULL", "NULL", 0L, 0L, 0L,
                true, false, true);
        Assert.assertTrue(rule.getRequirePartitionFilter());
        Assert.assertFalse(rule.getGlobal());
        Assert.assertTrue(rule.getEnable());
    }

    @Test
    public void testRequirePartitionFilterBlocksPartitionedScanWithoutFilter() {
        SqlBlockRuleMgr mgr = new SqlBlockRuleMgr();
        SqlBlockRule rule = new SqlBlockRule("r1", "NULL", "NULL", 0L, 0L, 0L,
                true, true, true);

        AnalysisException exception = Assert.assertThrows(AnalysisException.class,
                () -> mgr.checkLimitations(rule, 2L, 3L, 4L, true, false));
        Assert.assertTrue(exception.getMessage().contains("sql hits sql block rule: r1, missing partition filter"));
    }

    @Test
    public void testRequirePartitionFilterAllowsPartitionedScanWithFilter() throws AnalysisException {
        SqlBlockRuleMgr mgr = new SqlBlockRuleMgr();
        SqlBlockRule rule = new SqlBlockRule("r1", "NULL", "NULL", 0L, 0L, 0L,
                true, true, true);

        mgr.checkLimitations(rule, 2L, 3L, 4L, true, true);
    }

    @Test
    public void testRequirePartitionFilterAllowsUnpartitionedScan() throws AnalysisException {
        SqlBlockRuleMgr mgr = new SqlBlockRuleMgr();
        SqlBlockRule rule = new SqlBlockRule("r1", "NULL", "NULL", 0L, 0L, 0L,
                true, true, true);

        mgr.checkLimitations(rule, 0L, 3L, 4L, false, false);
    }
}
