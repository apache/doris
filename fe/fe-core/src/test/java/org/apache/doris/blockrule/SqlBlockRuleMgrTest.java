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

import org.apache.doris.persist.gson.GsonUtils;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SqlBlockRuleMgrTest {
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

}
