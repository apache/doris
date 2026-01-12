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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class FileCacheAdmissionManagerTest {

    private FileCacheAdmissionManager manager;

    @Before
    public void setUp() {
        Config.file_cache_admission_control_default_allow = false;
        manager = new FileCacheAdmissionManager();
    }

    @Test
    public void testEmptyUserIdentity() {
        AtomicReference<String> reason = new AtomicReference<>();
        boolean result = manager.isAdmittedAtTableLevel("", "catalog", "database", "table", reason);
        Assert.assertFalse(result);
        Assert.assertEquals("empty user_identity", reason.get());
    }

    @Test
    public void testInvalidUserIdentity() {
        AtomicReference<String> reason = new AtomicReference<>();
        boolean result = manager.isAdmittedAtTableLevel("123user", "catalog", "database", "table", reason);
        Assert.assertFalse(result);
        Assert.assertEquals("invalid user_identity", reason.get());
    }

    @Test
    public void testDefaultAllow() {
        Config.file_cache_admission_control_default_allow = true;
        AtomicReference<String> reason1 = new AtomicReference<>();
        boolean result1 = manager.isAdmittedAtTableLevel("user", "catalog", "database", "table", reason1);
        Assert.assertTrue(result1);
        Assert.assertEquals("default rule", reason1.get());

        Config.file_cache_admission_control_default_allow = false;
        AtomicReference<String> reason2 = new AtomicReference<>();
        boolean result2 = manager.isAdmittedAtTableLevel("user", "catalog", "database", "table", reason2);
        Assert.assertFalse(result2);
        Assert.assertEquals("default rule", reason2.get());
    }

    @Test
    public void testCommonRule() throws Exception {
        List<FileCacheAdmissionManager.AdmissionRule> rules = new ArrayList<>();
        long createdTime = 0;
        long updatedTime = 0;

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                1L,  "", "catalog_1", "", "", "",
                1, true, createdTime, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                2L, "", "catalog_2", "database_1", "", "",
                1, true, createdTime, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                3L, "", "catalog_3", "database_2", "table_1", "",
                1, true, createdTime, updatedTime
        ));

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        File jsonFile = new File("rules.json");
        objectMapper.writeValue(jsonFile, rules);

        manager.loadRules("rules.json");

        AtomicReference<String> reason1 = new AtomicReference<>();
        boolean result1 = manager.isAdmittedAtTableLevel("user", "catalog_1", "database", "table", reason1);
        Assert.assertTrue(result1);
        Assert.assertEquals("common catalog-level whitelist rule", reason1.get());

        AtomicReference<String> reason2 = new AtomicReference<>();
        boolean result2 = manager.isAdmittedAtTableLevel("user", "catalog_2", "database_1", "table", reason2);
        Assert.assertTrue(result2);
        Assert.assertEquals("common database-level whitelist rule", reason2.get());

        AtomicReference<String> reason3 = new AtomicReference<>();
        boolean result3 = manager.isAdmittedAtTableLevel("user", "catalog_3", "database_2", "table_1", reason3);
        Assert.assertTrue(result3);
        Assert.assertEquals("common table-level whitelist rule", reason3.get());
    }

    @Test
    public void testRuleEnabled() throws Exception  {
        List<FileCacheAdmissionManager.AdmissionRule> rules = new ArrayList<>();
        long createdTime = 0;
        long updatedTime = 0;

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                1L, "", "catalog_1", "", "", "",
                1, false, createdTime, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                2L, "", "catalog_2", "database_1", "", "",
                1, false, createdTime, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                3L, "", "catalog_3", "database_2", "table_1", "",
                1, false, createdTime, updatedTime
        ));

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        File jsonFile = new File("rules.json");
        objectMapper.writeValue(jsonFile, rules);

        manager.loadRules("rules.json");

        AtomicReference<String> reason1 = new AtomicReference<>();
        boolean result1 = manager.isAdmittedAtTableLevel("user", "catalog_1", "database", "table", reason1);
        Assert.assertFalse(result1);
        Assert.assertEquals("default rule", reason1.get());

        AtomicReference<String> reason2 = new AtomicReference<>();
        boolean result2 = manager.isAdmittedAtTableLevel("user", "catalog_2", "database_1", "table", reason2);
        Assert.assertFalse(result2);
        Assert.assertEquals("default rule", reason2.get());

        AtomicReference<String> reason3 = new AtomicReference<>();
        boolean result3 = manager.isAdmittedAtTableLevel("user", "catalog_3", "database_2", "table_1", reason3);
        Assert.assertFalse(result3);
        Assert.assertEquals("default rule", reason3.get());
    }

    @Test
    public void testUserRule() throws Exception {
        List<FileCacheAdmissionManager.AdmissionRule> rules = new ArrayList<>();
        long createdTime = 0;
        long updatedTime = 0;

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                4L, "user_1", "catalog_4", "", "", "",
                1, true, createdTime, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                5L, "user_1", "catalog_5", "database_4", "", "",
                1, true, createdTime, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                6L, "user_1", "catalog_6", "database_5", "table_4", "",
                1, true, createdTime, updatedTime
        ));

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        File jsonFile = new File("rules.json");
        objectMapper.writeValue(jsonFile, rules);

        manager.loadRules("rules.json");

        AtomicReference<String> reason1 = new AtomicReference<>();
        boolean result1 = manager.isAdmittedAtTableLevel("user_1", "catalog_4", "database", "table", reason1);
        Assert.assertTrue(result1);
        Assert.assertEquals("user catalog-level whitelist rule", reason1.get());
        AtomicReference<String> reason2 = new AtomicReference<>();
        boolean result2 = manager.isAdmittedAtTableLevel("user_2", "catalog_4", "database", "table", reason2);
        Assert.assertFalse(result2);
        Assert.assertEquals("default rule", reason2.get());

        AtomicReference<String> reason3 = new AtomicReference<>();
        boolean result3 = manager.isAdmittedAtTableLevel("user_1", "catalog_5", "database_4", "table", reason3);
        Assert.assertTrue(result3);
        Assert.assertEquals("user database-level whitelist rule", reason3.get());
        AtomicReference<String> reason4 = new AtomicReference<>();
        boolean result4 = manager.isAdmittedAtTableLevel("user_2", "catalog_5", "database_4", "table", reason4);
        Assert.assertFalse(result4);
        Assert.assertEquals("default rule", reason4.get());

        AtomicReference<String> reason5 = new AtomicReference<>();
        boolean result5 = manager.isAdmittedAtTableLevel("user_1", "catalog_6", "database_5", "table_4", reason5);
        Assert.assertTrue(result5);
        Assert.assertEquals("user table-level whitelist rule", reason5.get());
        AtomicReference<String> reason6 = new AtomicReference<>();
        boolean result6 = manager.isAdmittedAtTableLevel("user_2", "catalog_6", "database_5", "table_4", reason6);
        Assert.assertFalse(result6);
        Assert.assertEquals("default rule", reason6.get());
    }

    @Test
    public void testRuleLevelPriority() throws Exception {
        List<FileCacheAdmissionManager.AdmissionRule> rules = new ArrayList<>();
        long createdTime = 0;
        long updatedTime = 0;

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                7L, "user_3", "", "", "", "",
                1, true, createdTime, updatedTime
        ));

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        File jsonFile = new File("rules.json");
        objectMapper.writeValue(jsonFile, rules);

        manager.loadRules("rules.json");

        AtomicReference<String> reason1 = new AtomicReference<>();
        boolean result1 = manager.isAdmittedAtTableLevel("user_3", "catalog", "database", "table", reason1);
        Assert.assertTrue(result1);
        Assert.assertEquals("user global-level whitelist rule", reason1.get());

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                8L, "user_3", "catalog", "", "", "",
                1, true, createdTime, updatedTime
        ));

        objectMapper.writeValue(jsonFile, rules);

        manager.loadRules("rules.json");

        AtomicReference<String> reason2 = new AtomicReference<>();
        boolean result2 = manager.isAdmittedAtTableLevel("user_3", "catalog", "database", "table", reason2);
        Assert.assertTrue(result2);
        Assert.assertEquals("user catalog-level whitelist rule", reason2.get());

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                9L, "user_3", "catalog", "database", "", "",
                1, true, createdTime, updatedTime
        ));

        objectMapper.writeValue(jsonFile, rules);

        manager.loadRules("rules.json");

        AtomicReference<String> reason3 = new AtomicReference<>();
        boolean result3 = manager.isAdmittedAtTableLevel("user_3", "catalog", "database", "table", reason3);
        Assert.assertTrue(result3);
        Assert.assertEquals("user database-level whitelist rule", reason3.get());

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                10L, "user_3", "catalog", "database", "table", "",
                1, true, createdTime, updatedTime
        ));

        objectMapper.writeValue(jsonFile, rules);

        manager.loadRules("rules.json");

        AtomicReference<String> reason4 = new AtomicReference<>();
        boolean result4 = manager.isAdmittedAtTableLevel("user_3", "catalog", "database", "table", reason4);
        Assert.assertTrue(result4);
        Assert.assertEquals("user table-level whitelist rule", reason4.get());
    }

    @Test
    public void testRuleTypePriority() throws Exception {
        List<FileCacheAdmissionManager.AdmissionRule> rules = new ArrayList<>();
        long createdTime = 0;
        long updatedTime = 0;

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                11L, "user_4", "", "", "", "",
                0, true, createdTime, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                12L, "user_4", "", "", "", "",
                1, true, createdTime, updatedTime
        ));

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        File jsonFile = new File("rules.json");
        objectMapper.writeValue(jsonFile, rules);

        manager.loadRules("rules.json");

        AtomicReference<String> reason1 = new AtomicReference<>();
        boolean result1 = manager.isAdmittedAtTableLevel("user_4", "catalog", "database", "table", reason1);
        Assert.assertFalse(result1);
        Assert.assertEquals("user global-level blacklist rule", reason1.get());

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                13L, "user_4", "catalog", "", "", "",
                0, true, createdTime, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                14L, "user_4", "catalog", "", "", "",
                1, true, createdTime, updatedTime
        ));

        objectMapper.writeValue(jsonFile, rules);

        manager.loadRules("rules.json");

        AtomicReference<String> reason2 = new AtomicReference<>();
        boolean result2 = manager.isAdmittedAtTableLevel("user_4", "catalog", "database", "table", reason2);
        Assert.assertFalse(result2);
        Assert.assertEquals("user catalog-level blacklist rule", reason2.get());

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                15L, "user_4", "catalog", "database", "", "",
                0, true, createdTime, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                16L, "user_4", "catalog", "database", "", "",
                1, true, createdTime, updatedTime
        ));

        objectMapper.writeValue(jsonFile, rules);

        manager.loadRules("rules.json");

        AtomicReference<String> reason3 = new AtomicReference<>();
        boolean result3 = manager.isAdmittedAtTableLevel("user_4", "catalog", "database", "table", reason3);
        Assert.assertFalse(result3);
        Assert.assertEquals("user database-level blacklist rule", reason3.get());

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                17L, "user_4", "catalog", "database", "table", "",
                0, true, createdTime, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                18L, "user_4", "catalog", "database", "table", "",
                1, true, createdTime, updatedTime
        ));

        objectMapper.writeValue(jsonFile, rules);

        manager.loadRules("rules.json");

        AtomicReference<String> reason4 = new AtomicReference<>();
        boolean result4 = manager.isAdmittedAtTableLevel("user_4", "catalog", "database", "table", reason4);
        Assert.assertFalse(result4);
        Assert.assertEquals("user table-level blacklist rule", reason4.get());
    }

    @Test
    public void testNestedRulePriorities() throws Exception {
        List<FileCacheAdmissionManager.AdmissionRule> rules = new ArrayList<>();
        long createdTime = 0;
        long updatedTime = 0;

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                19L, "user_5", "catalog", "", "", "",
                0, true, createdTime, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                20L, "user_5", "catalog", "database", "", "",
                1, true, createdTime, updatedTime
        ));

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                21L, "user_6", "catalog", "", "", "",
                1, true, createdTime, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                22L, "user_6", "catalog", "database", "", "",
                0, true, createdTime, updatedTime
        ));

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                23L, "user_7", "catalog", "database", "", "",
                0, true, createdTime, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                24L, "user_7", "catalog", "database", "table", "",
                1, true, createdTime, updatedTime
        ));

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                25L, "user_8", "catalog", "database", "", "",
                1, true, createdTime, updatedTime
        ));
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                26L, "user_8", "catalog", "database", "table", "",
                0, true, createdTime, updatedTime
        ));

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        File jsonFile = new File("rules.json");
        objectMapper.writeValue(jsonFile, rules);

        manager.loadRules("rules.json");

        AtomicReference<String> reason1 = new AtomicReference<>();
        boolean result1 = manager.isAdmittedAtTableLevel("user_5", "catalog", "database", "table", reason1);
        Assert.assertTrue(result1);
        Assert.assertEquals("user database-level whitelist rule", reason1.get());
        AtomicReference<String> reason2 = new AtomicReference<>();
        boolean result2 = manager.isAdmittedAtTableLevel("user_5", "catalog", "otherDatabase", "table", reason2);
        Assert.assertFalse(result2);
        Assert.assertEquals("user catalog-level blacklist rule", reason2.get());

        AtomicReference<String> reason3 = new AtomicReference<>();
        boolean result3 = manager.isAdmittedAtTableLevel("user_6", "catalog", "database", "table", reason3);
        Assert.assertFalse(result3);
        Assert.assertEquals("user database-level blacklist rule", reason3.get());
        AtomicReference<String> reason4 = new AtomicReference<>();
        boolean result4 = manager.isAdmittedAtTableLevel("user_6", "catalog", "otherDatabase", "table", reason4);
        Assert.assertTrue(result4);
        Assert.assertEquals("user catalog-level whitelist rule", reason4.get());

        AtomicReference<String> reason5 = new AtomicReference<>();
        boolean result5 = manager.isAdmittedAtTableLevel("user_7", "catalog", "database", "table", reason5);
        Assert.assertTrue(result5);
        Assert.assertEquals("user table-level whitelist rule", reason5.get());
        AtomicReference<String> reason6 = new AtomicReference<>();
        boolean result6 = manager.isAdmittedAtTableLevel("user_7", "catalog", "database", "otherTable", reason6);
        Assert.assertFalse(result6);
        Assert.assertEquals("user database-level blacklist rule", reason6.get());

        AtomicReference<String> reason7 = new AtomicReference<>();
        boolean result7 = manager.isAdmittedAtTableLevel("user_8", "catalog", "database", "table", reason7);
        Assert.assertFalse(result7);
        Assert.assertEquals("user table-level blacklist rule", reason7.get());
        AtomicReference<String> reason8 = new AtomicReference<>();
        boolean result8 = manager.isAdmittedAtTableLevel("user_8", "catalog", "database", "otherTable", reason8);
        Assert.assertTrue(result8);
        Assert.assertEquals("user database-level whitelist rule", reason8.get());
    }

    @AfterClass
    public static void deleteJsonFile() throws Exception {
        File file = new File("rules.json");
        Assert.assertTrue(file.exists());
        if (file.exists()) {
            Assert.assertTrue(file.delete());
        }
    }
}
