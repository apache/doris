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
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class FileCacheAdmissionRuleRefresherTest {

    private static FileCacheAdmissionManager manager;

    @BeforeClass
    public static void setUpClass() throws Exception {
        Path currentDir = Paths.get("").toAbsolutePath();
        Path jsonFileDir = currentDir.resolve("jsonFileDir-test");

        if (!Files.exists(jsonFileDir)) {
            Files.createDirectories(jsonFileDir);
            System.out.println("Directory created successfully: " + jsonFileDir.toAbsolutePath());
        }

        Config.file_cache_admission_control_json_dir = jsonFileDir.toString();

        manager = new FileCacheAdmissionManager();
        manager.loadOnStartup();
    }

    @Test
    public void testJsonFileCreated() throws Exception {
        AtomicReference<String> reason1 = new AtomicReference<>();
        boolean result1 = manager.isAdmittedAtTableLevel("user_1", "catalog_1", "database_1", "table_1", reason1);
        Assert.assertFalse(result1);
        Assert.assertEquals("default rule", reason1.get());

        AtomicReference<String> reason2 = new AtomicReference<>();
        boolean result2 = manager.isAdmittedAtTableLevel("user_2", "catalog_2", "database_2", "table_2", reason2);
        Assert.assertFalse(result2);
        Assert.assertEquals("default rule", reason2.get());

        List<FileCacheAdmissionManager.AdmissionRule> rules = new ArrayList<>();
        long createdTime = 0;
        long updatedTime = 0;

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                1L, "user_1", "catalog_1", "database_1", "table_1", "",
                1, true, createdTime, updatedTime
        ));

        File jsonFile1 = new File(Config.file_cache_admission_control_json_dir, "rules_1.json");
        objectMapper.writeValue(jsonFile1, rules);

        rules.clear();
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                2L, "user_2", "catalog_2", "database_2", "table_2", "",
                1, true, createdTime, updatedTime
        ));

        File jsonFile2 = new File(Config.file_cache_admission_control_json_dir, "rules_2.json");
        objectMapper.writeValue(jsonFile2, rules);

        Thread.sleep(1000);

        AtomicReference<String> reason3 = new AtomicReference<>();
        boolean result3 = manager.isAdmittedAtTableLevel("user_1", "catalog_1", "database_1", "table_1", reason3);
        Assert.assertTrue(result3);
        Assert.assertEquals("user table-level whitelist rule", reason3.get());

        AtomicReference<String> reason4 = new AtomicReference<>();
        boolean result4 = manager.isAdmittedAtTableLevel("user_2", "catalog_2", "database_2", "table_2", reason4);
        Assert.assertTrue(result4);
        Assert.assertEquals("user table-level whitelist rule", reason4.get());
    }

    @Test
    public void testJsonFileDeleted() throws Exception {
        List<FileCacheAdmissionManager.AdmissionRule> rules = new ArrayList<>();
        long createdTime = 0;
        long updatedTime = 0;

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                3L, "user_3", "catalog_3", "database_3", "table_3", "",
                1, true, createdTime, updatedTime
        ));

        File jsonFile3 = new File(Config.file_cache_admission_control_json_dir, "rules_3.json");
        objectMapper.writeValue(jsonFile3, rules);

        rules.clear();
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                4L, "user_4", "catalog_4", "database_4", "table_4", "",
                1, true, createdTime, updatedTime
        ));

        File jsonFile4 = new File(Config.file_cache_admission_control_json_dir, "rules_4.json");
        objectMapper.writeValue(jsonFile4, rules);

        Thread.sleep(1000);

        AtomicReference<String> reason1 = new AtomicReference<>();
        boolean result1 = manager.isAdmittedAtTableLevel("user_3", "catalog_3", "database_3", "table_3", reason1);
        Assert.assertTrue(result1);
        Assert.assertEquals("user table-level whitelist rule", reason1.get());

        AtomicReference<String> reason2 = new AtomicReference<>();
        boolean result2 = manager.isAdmittedAtTableLevel("user_4", "catalog_4", "database_4", "table_4", reason2);
        Assert.assertTrue(result2);
        Assert.assertEquals("user table-level whitelist rule", reason2.get());

        Assert.assertTrue(jsonFile4.delete());

        Thread.sleep(1000);

        AtomicReference<String> reason3 = new AtomicReference<>();
        boolean result3 = manager.isAdmittedAtTableLevel("user_3", "catalog_3", "database_3", "table_3", reason3);
        Assert.assertTrue(result3);
        Assert.assertEquals("user table-level whitelist rule", reason3.get());

        AtomicReference<String> reason4 = new AtomicReference<>();
        boolean result4 = manager.isAdmittedAtTableLevel("user_4", "catalog_4", "database_4", "table_4", reason4);
        Assert.assertFalse(result4);
        Assert.assertEquals("default rule", reason4.get());
    }

    @Test
    public void testJsonFileModified() throws Exception {
        List<FileCacheAdmissionManager.AdmissionRule> rules = new ArrayList<>();
        long createdTime = 0;
        long updatedTime = 0;

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                5L, "user_5", "catalog_5", "database_5", "table_5", "",
                1, true, createdTime, updatedTime
        ));

        File jsonFile5 = new File(Config.file_cache_admission_control_json_dir, "rules_5.json");
        objectMapper.writeValue(jsonFile5, rules);

        Thread.sleep(1000);

        AtomicReference<String> reason1 = new AtomicReference<>();
        boolean result1 = manager.isAdmittedAtTableLevel("user_5", "catalog_5", "database_5", "table_5", reason1);
        Assert.assertTrue(result1);
        Assert.assertEquals("user table-level whitelist rule", reason1.get());

        AtomicReference<String> reason2 = new AtomicReference<>();
        boolean result2 = manager.isAdmittedAtTableLevel("user_6", "catalog_6", "database_6", "table_6", reason2);
        Assert.assertFalse(result2);
        Assert.assertEquals("default rule", reason2.get());

        rules.clear();
        rules.add(new FileCacheAdmissionManager.AdmissionRule(
                6L, "user_6", "catalog_6", "database_6", "table_6", "",
                1, true, createdTime, updatedTime
        ));

        objectMapper.writeValue(jsonFile5, rules);

        Thread.sleep(1000);

        AtomicReference<String> reason3 = new AtomicReference<>();
        boolean result3 = manager.isAdmittedAtTableLevel("user_5", "catalog_5", "database_5", "table_5", reason3);
        Assert.assertFalse(result3);
        Assert.assertEquals("default rule", reason3.get());

        AtomicReference<String> reason4 = new AtomicReference<>();
        boolean result4 = manager.isAdmittedAtTableLevel("user_6", "catalog_6", "database_6", "table_6", reason4);
        Assert.assertTrue(result4);
        Assert.assertEquals("user table-level whitelist rule", reason4.get());
    }

    private static void deleteDirectoryRecursively(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }

        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if (exc != null) {
                    throw exc;
                }
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    @AfterClass
    public static void deleteJsonFile() throws Exception {
        Path currentDir = Paths.get("").toAbsolutePath();
        Path jsonFileDir = currentDir.resolve("jsonFileDir-test");

        if (Files.exists(Paths.get(Config.file_cache_admission_control_json_dir))) {
            deleteDirectoryRecursively(jsonFileDir);
        }
    }
}
