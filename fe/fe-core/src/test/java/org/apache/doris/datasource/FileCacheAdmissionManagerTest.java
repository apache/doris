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

package org.apache.doris.datasource;

import org.apache.doris.common.Config;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class FileCacheAdmissionManagerTest {

    private FileCacheAdmissionManager manager;

    @Before
    public void setUp() {
        Config.file_cache_admission_control_default_allow = false;
        manager = new FileCacheAdmissionManager();

        List<FileCacheAdmissionManager.AdmissionRule> rules = new ArrayList<>();
        long updatedTime = 0;
        manager.initialize(rules, updatedTime);
    }

    @Test
    public void testEmptyUserIdentity() {
        AtomicReference<String> reason = new AtomicReference<>();
        boolean result = manager.isAllowed("", "catalog", "database", "table", reason);
        Assert.assertFalse(result);
        Assert.assertEquals("empty user_identity", reason.get());
    }

    @Test
    public void testInvalidUserIdentity() {
        AtomicReference<String> reason = new AtomicReference<>();
        boolean result = manager.isAllowed("123user", "catalog", "database", "table", reason);
        Assert.assertFalse(result);
        Assert.assertEquals("invalid user_identity", reason.get());
    }

    @Test
    public void testDefaultAllow() {
        Config.file_cache_admission_control_default_allow = true;
        AtomicReference<String> reason1 = new AtomicReference<>();
        boolean result1 = manager.isAllowed("user", "catalog", "database", "table", reason1);
        Assert.assertTrue(result1);
        Assert.assertEquals("default rule", reason1.get());

        Config.file_cache_admission_control_default_allow = false;
        AtomicReference<String> reason2 = new AtomicReference<>();
        boolean result2 = manager.isAllowed("user", "catalog", "database", "table", reason2);
        Assert.assertFalse(result2);
        Assert.assertEquals("default rule", reason2.get());
    }

    @Test
    public void testCommonRule() {
        List<FileCacheAdmissionManager.AdmissionRule> rules = new ArrayList<>();
        long updatedTime = 1000;

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                1L,  "", "catalog_1", "", "", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                2L, "", "catalog_2", "database_1", "", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                3L, "", "catalog_3", "database_2", "table_1", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));

        manager.update(rules, updatedTime);

        AtomicReference<String> reason1 = new AtomicReference<>();
        boolean result1 = manager.isAllowed("user", "catalog_1", "database", "table", reason1);
        Assert.assertTrue(result1);
        Assert.assertEquals("common catalog-level whitelist rule", reason1.get());

        AtomicReference<String> reason2 = new AtomicReference<>();
        boolean result2 = manager.isAllowed("user", "catalog_2", "database_1", "table", reason2);
        Assert.assertTrue(result2);
        Assert.assertEquals("common database-level whitelist rule", reason2.get());

        AtomicReference<String> reason3 = new AtomicReference<>();
        boolean result3 = manager.isAllowed("user", "catalog_3", "database_2", "table_1", reason3);
        Assert.assertTrue(result3);
        Assert.assertEquals("common table-level whitelist rule", reason3.get());
    }

    @Test
    public void testRuleEnabled() {
        List<FileCacheAdmissionManager.AdmissionRule> rules = new ArrayList<>();
        long updatedTime = 2000;

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                1L, "", "catalog_1", "", "", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, false, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                2L, "", "catalog_2", "database_1", "", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, false, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                3L, "", "catalog_3", "database_2", "table_1", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, false, updatedTime
        ));

        manager.update(rules, updatedTime);

        AtomicReference<String> reason1 = new AtomicReference<>();
        boolean result1 = manager.isAllowed("user", "catalog_1", "database", "table", reason1);
        Assert.assertFalse(result1);
        Assert.assertEquals("default rule", reason1.get());

        AtomicReference<String> reason2 = new AtomicReference<>();
        boolean result2 = manager.isAllowed("user", "catalog_2", "database_1", "table", reason2);
        Assert.assertFalse(result2);
        Assert.assertEquals("default rule", reason2.get());

        AtomicReference<String> reason3 = new AtomicReference<>();
        boolean result3 = manager.isAllowed("user", "catalog_3", "database_2", "table_1", reason3);
        Assert.assertFalse(result3);
        Assert.assertEquals("default rule", reason3.get());
    }

    @Test
    public void testUserRule() {
        List<FileCacheAdmissionManager.AdmissionRule> rules = new ArrayList<>();
        long updatedTime = 3000;

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                4L, "user_1", "catalog_4", "", "", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                5L, "user_1", "catalog_5", "database_4", "", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                6L, "user_1", "catalog_6", "database_5", "table_4", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));

        manager.update(rules, updatedTime);

        AtomicReference<String> reason1 = new AtomicReference<>();
        boolean result1 = manager.isAllowed("user_1", "catalog_4", "database", "table", reason1);
        Assert.assertTrue(result1);
        Assert.assertEquals("user catalog-level whitelist rule", reason1.get());
        AtomicReference<String> reason2 = new AtomicReference<>();
        boolean result2 = manager.isAllowed("user_2", "catalog_4", "database", "table", reason2);
        Assert.assertFalse(result2);
        Assert.assertEquals("default rule", reason2.get());

        AtomicReference<String> reason3 = new AtomicReference<>();
        boolean result3 = manager.isAllowed("user_1", "catalog_5", "database_4", "table", reason3);
        Assert.assertTrue(result3);
        Assert.assertEquals("user database-level whitelist rule", reason3.get());
        AtomicReference<String> reason4 = new AtomicReference<>();
        boolean result4 = manager.isAllowed("user_2", "catalog_5", "database_4", "table", reason4);
        Assert.assertFalse(result4);
        Assert.assertEquals("default rule", reason4.get());

        AtomicReference<String> reason5 = new AtomicReference<>();
        boolean result5 = manager.isAllowed("user_1", "catalog_6", "database_5", "table_4", reason5);
        Assert.assertTrue(result5);
        Assert.assertEquals("user table-level whitelist rule", reason5.get());
        AtomicReference<String> reason6 = new AtomicReference<>();
        boolean result6 = manager.isAllowed("user_2", "catalog_6", "database_5", "table_4", reason6);
        Assert.assertFalse(result6);
        Assert.assertEquals("default rule", reason6.get());
    }

    @Test
    public void testRuleUpdate() {
        List<FileCacheAdmissionManager.AdmissionRule> rules = new ArrayList<>();
        long updatedTime = 4000;

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                4L, "user_1", "catalog_44", "", "", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                5L, "user_1", "catalog_55", "database_44", "", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                6L, "user_1", "catalog_66", "database_55", "table_44", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));

        manager.update(rules, updatedTime);

        AtomicReference<String> reason1 = new AtomicReference<>();
        boolean result1 = manager.isAllowed("user_1", "catalog_4", "database", "table", reason1);
        Assert.assertFalse(result1);
        Assert.assertEquals("default rule", reason1.get());
        AtomicReference<String> reason2 = new AtomicReference<>();
        boolean result2 = manager.isAllowed("user_1", "catalog_44", "database", "table", reason2);
        Assert.assertTrue(result2);
        Assert.assertEquals("user catalog-level whitelist rule", reason2.get());

        AtomicReference<String> reason3 = new AtomicReference<>();
        boolean result3 = manager.isAllowed("user_1", "catalog_5", "database_4", "table", reason3);
        Assert.assertFalse(result3);
        Assert.assertEquals("default rule", reason3.get());
        AtomicReference<String> reason4 = new AtomicReference<>();
        boolean result4 = manager.isAllowed("user_1", "catalog_55", "database_44", "table", reason4);
        Assert.assertTrue(result4);
        Assert.assertEquals("user database-level whitelist rule", reason4.get());

        AtomicReference<String> reason5 = new AtomicReference<>();
        boolean result5 = manager.isAllowed("user_1", "catalog_6", "database_5", "table_4", reason5);
        Assert.assertFalse(result5);
        Assert.assertEquals("default rule", reason5.get());
        AtomicReference<String> reason6 = new AtomicReference<>();
        boolean result6 = manager.isAllowed("user_1", "catalog_66", "database_55", "table_44", reason6);
        Assert.assertTrue(result6);
        Assert.assertEquals("user table-level whitelist rule", reason6.get());


        rules.clear();

        ++updatedTime;
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                4L, "user_2", "catalog_44", "", "", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                5L, "user_2", "catalog_55", "database_44", "", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                6L, "user_2", "catalog_66", "database_55", "table_44", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));

        manager.update(rules, updatedTime);

        AtomicReference<String> reason7 = new AtomicReference<>();
        boolean result7 = manager.isAllowed("user_1", "catalog_44", "database", "table", reason7);
        Assert.assertFalse(result7);
        Assert.assertEquals("default rule", reason7.get());
        AtomicReference<String> reason8 = new AtomicReference<>();
        boolean result8 = manager.isAllowed("user_2", "catalog_44", "database", "table", reason8);
        Assert.assertTrue(result8);
        Assert.assertEquals("user catalog-level whitelist rule", reason8.get());

        AtomicReference<String> reason9 = new AtomicReference<>();
        boolean result9 = manager.isAllowed("user_1", "catalog_55", "database_44", "table", reason9);
        Assert.assertFalse(result9);
        Assert.assertEquals("default rule", reason9.get());
        AtomicReference<String> reason10 = new AtomicReference<>();
        boolean result10 = manager.isAllowed("user_2", "catalog_55", "database_44", "table", reason10);
        Assert.assertTrue(result10);
        Assert.assertEquals("user database-level whitelist rule", reason10.get());

        AtomicReference<String> reason11 = new AtomicReference<>();
        boolean result11 = manager.isAllowed("user_1", "catalog_66", "database_55", "table_44", reason11);
        Assert.assertFalse(result11);
        Assert.assertEquals("default rule", reason11.get());
        AtomicReference<String> reason12 = new AtomicReference<>();
        boolean result12 = manager.isAllowed("user_2", "catalog_66", "database_55", "table_44", reason12);
        Assert.assertTrue(result12);
        Assert.assertEquals("user table-level whitelist rule", reason12.get());


        rules.clear();

        ++updatedTime;
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                4L, "", "catalog_44", "", "", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                5L, "", "catalog_55", "database_44", "", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                6L, "", "catalog_66", "database_55", "table_44", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));

        manager.update(rules, updatedTime);

        AtomicReference<String> reason13 = new AtomicReference<>();
        boolean result13 = manager.isAllowed("user_1", "catalog_44", "database", "table", reason13);
        Assert.assertTrue(result13);
        Assert.assertEquals("common catalog-level whitelist rule", reason13.get());
        AtomicReference<String> reason14 = new AtomicReference<>();
        boolean result14 = manager.isAllowed("user_2", "catalog_44", "database", "table", reason14);
        Assert.assertTrue(result14);
        Assert.assertEquals("common catalog-level whitelist rule", reason14.get());

        AtomicReference<String> reason15 = new AtomicReference<>();
        boolean result15 = manager.isAllowed("user_1", "catalog_55", "database_44", "table", reason15);
        Assert.assertTrue(result15);
        Assert.assertEquals("common database-level whitelist rule", reason15.get());
        AtomicReference<String> reason16 = new AtomicReference<>();
        boolean result16 = manager.isAllowed("user_2", "catalog_55", "database_44", "table", reason16);
        Assert.assertTrue(result16);
        Assert.assertEquals("common database-level whitelist rule", reason16.get());

        AtomicReference<String> reason17 = new AtomicReference<>();
        boolean result17 = manager.isAllowed("user_1", "catalog_66", "database_55", "table_44", reason17);
        Assert.assertTrue(result17);
        Assert.assertEquals("common table-level whitelist rule", reason17.get());
        AtomicReference<String> reason18 = new AtomicReference<>();
        boolean result18 = manager.isAllowed("user_2", "catalog_66", "database_55", "table_44", reason18);
        Assert.assertTrue(result18);
        Assert.assertEquals("common table-level whitelist rule", reason18.get());
    }

    @Test
    public void testRuleLevelPriority() {
        List<FileCacheAdmissionManager.AdmissionRule> rules = new ArrayList<>();
        long updatedTime = 5000;

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                7L, "user_3", "", "", "", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));

        manager.update(rules, updatedTime);

        AtomicReference<String> reason1 = new AtomicReference<>();
        boolean result1 = manager.isAllowed("user_3", "catalog", "database", "table", reason1);
        Assert.assertTrue(result1);
        Assert.assertEquals("user global-level whitelist rule", reason1.get());

        ++updatedTime;
        rules.clear();
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                8L, "user_3", "catalog", "", "", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));

        manager.update(rules, updatedTime);

        AtomicReference<String> reason2 = new AtomicReference<>();
        boolean result2 = manager.isAllowed("user_3", "catalog", "database", "table", reason2);
        Assert.assertTrue(result2);
        Assert.assertEquals("user catalog-level whitelist rule", reason2.get());

        ++updatedTime;
        rules.clear();
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                9L, "user_3", "catalog", "database", "", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));

        manager.update(rules, updatedTime);

        AtomicReference<String> reason3 = new AtomicReference<>();
        boolean result3 = manager.isAllowed("user_3", "catalog", "database", "table", reason3);
        Assert.assertTrue(result3);
        Assert.assertEquals("user database-level whitelist rule", reason3.get());

        ++updatedTime;
        rules.clear();
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                10L, "user_3", "catalog", "database", "table", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));

        manager.update(rules, updatedTime);

        AtomicReference<String> reason4 = new AtomicReference<>();
        boolean result4 = manager.isAllowed("user_3", "catalog", "database", "table", reason4);
        Assert.assertTrue(result4);
        Assert.assertEquals("user table-level whitelist rule", reason4.get());
    }

    @Test
    public void testRuleTypePriority() {
        List<FileCacheAdmissionManager.AdmissionRule> rules = new ArrayList<>();
        long updatedTime = 6000;

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                11L, "user_4", "", "", "", "",
                FileCacheAdmissionManager.RuleType.EXCLUDE, true, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                12L, "user_4", "", "", "", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));

        manager.update(rules, updatedTime);

        AtomicReference<String> reason1 = new AtomicReference<>();
        boolean result1 = manager.isAllowed("user_4", "catalog", "database", "table", reason1);
        Assert.assertFalse(result1);
        Assert.assertEquals("user global-level blacklist rule", reason1.get());

        ++updatedTime;
        rules.clear();
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                13L, "user_4", "catalog", "", "", "",
                FileCacheAdmissionManager.RuleType.EXCLUDE, true, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                14L, "user_4", "catalog", "", "", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));

        manager.update(rules, updatedTime);

        AtomicReference<String> reason2 = new AtomicReference<>();
        boolean result2 = manager.isAllowed("user_4", "catalog", "database", "table", reason2);
        Assert.assertFalse(result2);
        Assert.assertEquals("user catalog-level blacklist rule", reason2.get());

        ++updatedTime;
        rules.clear();
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                15L, "user_4", "catalog", "database", "", "",
                FileCacheAdmissionManager.RuleType.EXCLUDE, true, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                16L, "user_4", "catalog", "database", "", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));

        manager.update(rules, updatedTime);

        AtomicReference<String> reason3 = new AtomicReference<>();
        boolean result3 = manager.isAllowed("user_4", "catalog", "database", "table", reason3);
        Assert.assertFalse(result3);
        Assert.assertEquals("user database-level blacklist rule", reason3.get());

        ++updatedTime;
        rules.clear();
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                17L, "user_4", "catalog", "database", "table", "",
                FileCacheAdmissionManager.RuleType.EXCLUDE, true, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                18L, "user_4", "catalog", "database", "table", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));

        manager.update(rules, updatedTime);

        AtomicReference<String> reason4 = new AtomicReference<>();
        boolean result4 = manager.isAllowed("user_4", "catalog", "database", "table", reason4);
        Assert.assertFalse(result4);
        Assert.assertEquals("user table-level blacklist rule", reason4.get());
    }

    @Test
    public void testNestedRulePriorities() {
        List<FileCacheAdmissionManager.AdmissionRule> rules = new ArrayList<>();
        long updatedTime = 7000;

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                19L, "user_5", "catalog", "", "", "",
                FileCacheAdmissionManager.RuleType.EXCLUDE, true, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                20L, "user_5", "catalog", "database", "", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                21L, "user_6", "catalog", "", "", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                22L, "user_6", "catalog", "database", "", "",
                FileCacheAdmissionManager.RuleType.EXCLUDE, true, updatedTime
        ));

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                23L, "user_7", "catalog", "database", "", "",
                FileCacheAdmissionManager.RuleType.EXCLUDE, true, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                24L, "user_7", "catalog", "database", "table", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                25L, "user_8", "catalog", "database", "", "",
                FileCacheAdmissionManager.RuleType.INCLUDE, true, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                26L, "user_8", "catalog", "database", "table", "",
                FileCacheAdmissionManager.RuleType.EXCLUDE, true, updatedTime
        ));

        manager.update(rules, updatedTime);

        AtomicReference<String> reason1 = new AtomicReference<>();
        boolean result1 = manager.isAllowed("user_5", "catalog", "database", "table", reason1);
        Assert.assertTrue(result1);
        Assert.assertEquals("user database-level whitelist rule", reason1.get());
        AtomicReference<String> reason2 = new AtomicReference<>();
        boolean result2 = manager.isAllowed("user_5", "catalog", "otherDatabase", "table", reason2);
        Assert.assertFalse(result2);
        Assert.assertEquals("user catalog-level blacklist rule", reason2.get());

        AtomicReference<String> reason3 = new AtomicReference<>();
        boolean result3 = manager.isAllowed("user_6", "catalog", "database", "table", reason3);
        Assert.assertFalse(result3);
        Assert.assertEquals("user database-level blacklist rule", reason3.get());
        AtomicReference<String> reason4 = new AtomicReference<>();
        boolean result4 = manager.isAllowed("user_6", "catalog", "otherDatabase", "table", reason4);
        Assert.assertTrue(result4);
        Assert.assertEquals("user catalog-level whitelist rule", reason4.get());

        AtomicReference<String> reason5 = new AtomicReference<>();
        boolean result5 = manager.isAllowed("user_7", "catalog", "database", "table", reason5);
        Assert.assertTrue(result5);
        Assert.assertEquals("user table-level whitelist rule", reason5.get());
        AtomicReference<String> reason6 = new AtomicReference<>();
        boolean result6 = manager.isAllowed("user_7", "catalog", "database", "otherTable", reason6);
        Assert.assertFalse(result6);
        Assert.assertEquals("user database-level blacklist rule", reason6.get());

        AtomicReference<String> reason7 = new AtomicReference<>();
        boolean result7 = manager.isAllowed("user_8", "catalog", "database", "table", reason7);
        Assert.assertFalse(result7);
        Assert.assertEquals("user table-level blacklist rule", reason7.get());
        AtomicReference<String> reason8 = new AtomicReference<>();
        boolean result8 = manager.isAllowed("user_8", "catalog", "database", "otherTable", reason8);
        Assert.assertTrue(result8);
        Assert.assertEquals("user database-level whitelist rule", reason8.get());
    }

}
