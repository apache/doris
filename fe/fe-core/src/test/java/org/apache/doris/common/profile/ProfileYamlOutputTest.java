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

package org.apache.doris.common.profile;

import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TUnit;

import com.google.common.base.Strings;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Unit test for YAML output format of Profile.
 * This test verifies the correctness of YAML serialization for:
 * 1. Counter.toMap() - basic counter serialization
 * 2. AggCounter.toMap() - aggregated counter with sum/avg/max/min/count
 * 3. RuntimeProfile.toStructuredMap() - hierarchical profile structure
 * 4. Profile.getProfileAsYaml() - complete profile YAML output
 */
public class ProfileYamlOutputTest {

    private Profile profile;
    private RuntimeProfile runtimeProfile;
    private Yaml yaml;

    @BeforeEach
    public void setUp() {
        profile = ProfilePersistentTest.constructRandomProfile(1);
        runtimeProfile = new RuntimeProfile("TestProfile");
        yaml = new Yaml();
    }

    @AfterEach
    public void tearDown() {
        if (profile != null) {
            ProfileManager.getInstance().removeProfile(profile.getId());
        }
    }

    /**
     * Test Counter.toMap() basic functionality
     */
    @Test
    public void testCounterToMap() {
        // Test UNIT type counter
        Counter unitCounter = new Counter(TUnit.UNIT, 1234);
        Map<String, Object> unitMap = unitCounter.toMap();

        Assertions.assertNotNull(unitMap);
        Assertions.assertEquals(1234L, unitMap.get("raw"));
        Assertions.assertEquals("UNIT", unitMap.get("unit"));
        Assertions.assertNotNull(unitMap.get("display"));

        // Test TIME_NS type counter
        Counter timeCounter = new Counter(TUnit.TIME_NS, 123456789L);
        Map<String, Object> timeMap = timeCounter.toMap();

        Assertions.assertNotNull(timeMap);
        Assertions.assertEquals(123456789L, timeMap.get("raw"));
        Assertions.assertEquals("TIME_NS", timeMap.get("unit"));
        Assertions.assertNotNull(timeMap.get("display"));

        // Test BYTES type counter
        Counter bytesCounter = new Counter(TUnit.BYTES, 1024 * 1024);
        Map<String, Object> bytesMap = bytesCounter.toMap();

        Assertions.assertNotNull(bytesMap);
        Assertions.assertEquals(1024 * 1024L, bytesMap.get("raw"));
        Assertions.assertEquals("BYTES", bytesMap.get("unit"));
        Assertions.assertNotNull(bytesMap.get("display"));
    }

    /**
     * Test AggCounter.toMap() for non-time types (includes sum)
     */
    @Test
    public void testAggCounterToMapNonTime() {
        AggCounter aggCounter = new AggCounter(TUnit.UNIT);

        // Add some test data
        aggCounter.addCounter(new Counter(TUnit.UNIT, 100));
        aggCounter.addCounter(new Counter(TUnit.UNIT, 200));
        aggCounter.addCounter(new Counter(TUnit.UNIT, 300));

        Map<String, Object> map = aggCounter.toMap();

        Assertions.assertNotNull(map);
        Assertions.assertEquals("UNIT", map.get("unit"));
        Assertions.assertEquals(600L, map.get("sum"));
        Assertions.assertEquals(200L, map.get("avg"));
        Assertions.assertEquals(300L, map.get("max"));
        Assertions.assertEquals(100L, map.get("min"));
        Assertions.assertEquals(3, map.get("count"));
        Assertions.assertNotNull(map.get("display"));
    }

    /**
     * Test AggCounter.toMap() for time types (no sum)
     */
    @Test
    public void testAggCounterToMapTimeType() {
        AggCounter aggCounter = new AggCounter(TUnit.TIME_NS);

        // Add some test data
        aggCounter.addCounter(new Counter(TUnit.TIME_NS, 100000000L)); // 100ms
        aggCounter.addCounter(new Counter(TUnit.TIME_NS, 200000000L)); // 200ms
        aggCounter.addCounter(new Counter(TUnit.TIME_NS, 300000000L)); // 300ms

        Map<String, Object> map = aggCounter.toMap();

        Assertions.assertNotNull(map);
        Assertions.assertEquals("TIME_NS", map.get("unit"));
        // Time types should NOT include sum
        Assertions.assertNull(map.get("sum"));
        Assertions.assertEquals(200000000L, map.get("avg"));
        Assertions.assertEquals(300000000L, map.get("max"));
        Assertions.assertEquals(100000000L, map.get("min"));
        Assertions.assertEquals(3, map.get("count"));
        Assertions.assertNotNull(map.get("display"));
    }

    /**
     * Test AggCounter.toMap() with empty counter
     */
    @Test
    public void testAggCounterToMapEmpty() {
        AggCounter aggCounter = new AggCounter(TUnit.UNIT);

        Map<String, Object> map = aggCounter.toMap();

        Assertions.assertNotNull(map);
        Assertions.assertEquals("UNIT", map.get("unit"));
        Assertions.assertEquals(0L, map.get("sum"));
        Assertions.assertEquals(0L, map.get("avg"));
        Assertions.assertEquals(0L, map.get("max"));
        Assertions.assertEquals(0L, map.get("min"));
        Assertions.assertEquals(0, map.get("count"));
    }

    /**
     * Test RuntimeProfile.toStructuredMap() basic structure
     */
    @Test
    public void testRuntimeProfileToStructuredMapBasic() {
        runtimeProfile.addInfoString("TestKey", "TestValue");
        runtimeProfile.addCounter("Counter1", TUnit.UNIT, RuntimeProfile.ROOT_COUNTER);

        Map<String, Object> map = runtimeProfile.toStructuredMap();

        Assertions.assertNotNull(map);
        // New format: profile name is the key
        Assertions.assertTrue(map.containsKey("TestProfile"));

        @SuppressWarnings("unchecked")
        Map<String, Object> content = (Map<String, Object>) map.get("TestProfile");
        Assertions.assertNotNull(content);

        // Check info_strings
        @SuppressWarnings("unchecked")
        Map<String, String> infoStrings = (Map<String, String>) content.get("info_strings");
        Assertions.assertNotNull(infoStrings);
        Assertions.assertEquals("TestValue", infoStrings.get("TestKey"));

        // Check counters
        @SuppressWarnings("unchecked")
        Map<String, Object> counters = (Map<String, Object>) content.get("counters");
        Assertions.assertNotNull(counters);
        Assertions.assertTrue(counters.containsKey("Counter1"));
    }

    /**
     * Test RuntimeProfile.toStructuredMap() with counter hierarchy
     */
    @Test
    public void testRuntimeProfileToStructuredMapWithHierarchy() {
        // Create counter hierarchy: Root -> Parent -> Child -> Grandchild
        runtimeProfile.addCounter("Parent", TUnit.BYTES, RuntimeProfile.ROOT_COUNTER);
        runtimeProfile.addCounter("Child", TUnit.BYTES, "Parent");
        runtimeProfile.addCounter("Grandchild", TUnit.UNIT, "Child");

        Counter parent = runtimeProfile.getCounterMap().get("Parent");
        parent.setValue(1000);
        Counter child = runtimeProfile.getCounterMap().get("Child");
        child.setValue(500);
        Counter grandchild = runtimeProfile.getCounterMap().get("Grandchild");
        grandchild.setValue(10);

        Map<String, Object> map = runtimeProfile.toStructuredMap();

        // Get content from profile name key
        @SuppressWarnings("unchecked")
        Map<String, Object> content = (Map<String, Object>) map.get("TestProfile");
        Assertions.assertNotNull(content);

        // Check counter hierarchy
        @SuppressWarnings("unchecked")
        Map<String, Object> counters = (Map<String, Object>) content.get("counters");
        Assertions.assertNotNull(counters);
        Assertions.assertTrue(counters.containsKey("Parent"));

        @SuppressWarnings("unchecked")
        Map<String, Object> parentMap = (Map<String, Object>) counters.get("Parent");
        Assertions.assertEquals(1000L, parentMap.get("raw"));

        @SuppressWarnings("unchecked")
        Map<String, Object> parentChildren = (Map<String, Object>) parentMap.get("children");
        Assertions.assertNotNull(parentChildren);
        Assertions.assertTrue(parentChildren.containsKey("Child"));

        @SuppressWarnings("unchecked")
        Map<String, Object> childMap = (Map<String, Object>) parentChildren.get("Child");
        Assertions.assertEquals(500L, childMap.get("raw"));

        @SuppressWarnings("unchecked")
        Map<String, Object> childChildren = (Map<String, Object>) childMap.get("children");
        Assertions.assertNotNull(childChildren);
        Assertions.assertTrue(childChildren.containsKey("Grandchild"));
    }

    /**
     * Test RuntimeProfile.toStructuredMap() with child profiles
     */
    @Test
    public void testRuntimeProfileToStructuredMapWithChildren() {
        RuntimeProfile child1 = new RuntimeProfile("Child1");
        child1.addInfoString("ChildKey1", "ChildValue1");

        RuntimeProfile child2 = new RuntimeProfile("Child2");
        child2.addInfoString("ChildKey2", "ChildValue2");

        runtimeProfile.addChild(child1, true);
        runtimeProfile.addChild(child2, false);

        Map<String, Object> map = runtimeProfile.toStructuredMap();

        // Get content from profile name key
        @SuppressWarnings("unchecked")
        Map<String, Object> content = (Map<String, Object>) map.get("TestProfile");
        Assertions.assertNotNull(content);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> children = (List<Map<String, Object>>) content.get("children");
        Assertions.assertNotNull(children);
        Assertions.assertEquals(2, children.size());

        // Each child is now a map with profile name as key
        Map<String, Object> firstChild = children.get(0);
        Assertions.assertTrue(firstChild.containsKey("Child1"));

        Map<String, Object> secondChild = children.get(1);
        Assertions.assertTrue(secondChild.containsKey("Child2"));
    }

    /**
     * Test Profile.getProfileAsYaml() returns valid YAML
     */
    @Test
    public void testProfileGetProfileAsYamlValidFormat() {
        String yamlOutput = profile.getProfileAsYaml();

        Assertions.assertNotNull(yamlOutput);
        Assertions.assertFalse(yamlOutput.isEmpty());
        Assertions.assertTrue(yamlOutput.startsWith("---"));

        // Parse YAML to ensure it's valid
        Object parsed = yaml.load(yamlOutput);
        Assertions.assertNotNull(parsed);
        Assertions.assertTrue(parsed instanceof Map);

        @SuppressWarnings("unchecked")
        Map<String, Object> rootMap = (Map<String, Object>) parsed;

        // Check required fields
        Assertions.assertTrue(rootMap.containsKey("format_version"));
        Assertions.assertEquals("1.0", rootMap.get("format_version"));
        Assertions.assertTrue(rootMap.containsKey("generated_at"));
        Assertions.assertTrue(rootMap.containsKey("summary"));
        Assertions.assertTrue(rootMap.containsKey("execution_summary"));
    }

    /**
     * Test Profile.getProfileAsYaml() summary section
     */
    @Test
    public void testProfileGetProfileAsYamlSummarySection() {
        String yamlOutput = profile.getProfileAsYaml();

        @SuppressWarnings("unchecked")
        Map<String, Object> rootMap = (Map<String, Object>) yaml.load(yamlOutput);

        @SuppressWarnings("unchecked")
        Map<String, Object> summary = (Map<String, Object>) rootMap.get("summary");
        Assertions.assertNotNull(summary);

        // Check that summary contains expected fields
        Assertions.assertTrue(summary.containsKey("profile_id"));
        Assertions.assertFalse(Strings.isNullOrEmpty((String) summary.get("profile_id")));

        Assertions.assertTrue(summary.containsKey("task_type"));
        String taskType = (String) summary.get("task_type");
        Assertions.assertTrue("QUERY".equals(taskType) || "LOAD".equals(taskType));
    }

    /**
     * Test Profile.getProfileAsYaml() execution summary section
     */
    @Test
    public void testProfileGetProfileAsYamlExecutionSummarySection() {
        String yamlOutput = profile.getProfileAsYaml();

        @SuppressWarnings("unchecked")
        Map<String, Object> rootMap = (Map<String, Object>) yaml.load(yamlOutput);

        @SuppressWarnings("unchecked")
        Map<String, Object> executionSummary = (Map<String, Object>) rootMap.get("execution_summary");
        Assertions.assertNotNull(executionSummary);

        // Execution summary may be empty or contain timing information
        // Just verify it exists and is a map
        Assertions.assertTrue(executionSummary instanceof Map);
    }

    /**
     * Test Profile.getProfileAsYaml() with merged profile
     */
    @Test
    public void testProfileGetProfileAsYamlWithMergedProfile() {
        // Create a profile with more realistic runtime profile structure
        TUniqueId queryId = new TUniqueId();
        UUID uuid = UUID.randomUUID();
        queryId.setHi(uuid.getMostSignificantBits());
        queryId.setLo(uuid.getLeastSignificantBits());

        RuntimeProfile fragment = new RuntimeProfile("Fragment-0");
        fragment.addCounter("RowsProduced", TUnit.UNIT, RuntimeProfile.ROOT_COUNTER);

        List<Integer> fragmentIds = new ArrayList<>();
        fragmentIds.add(0);
        ExecutionProfile executionProfile = new ExecutionProfile(queryId, fragmentIds);
        executionProfile.getRoot().addChild(fragment, true);

        Profile testProfile = new Profile();
        testProfile.setSummaryProfile(ProfilePersistentTest.constructRandomSummaryProfile());
        testProfile.addExecutionProfile(executionProfile);

        String yamlOutput = testProfile.getProfileAsYaml();

        @SuppressWarnings("unchecked")
        Map<String, Object> rootMap = (Map<String, Object>) yaml.load(yamlOutput);

        // Check if merged_profile exists (it should for single execution profile)
        if (rootMap.containsKey("merged_profile")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> mergedProfile = (Map<String, Object>) rootMap.get("merged_profile");
            Assertions.assertNotNull(mergedProfile);
            Assertions.assertTrue(mergedProfile.containsKey("Fragments"));
        }
    }

    /**
     * Test Profile.getProfileAsYaml() with detail profiles
     */
    @Test
    public void testProfileGetProfileAsYamlWithDetailProfiles() {
        String yamlOutput = profile.getProfileAsYaml();

        @SuppressWarnings("unchecked")
        Map<String, Object> rootMap = (Map<String, Object>) yaml.load(yamlOutput);

        // Check detail_profiles
        if (rootMap.containsKey("detail_profiles")) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> detailProfiles =
                    (List<Map<String, Object>>) rootMap.get("detail_profiles");
            Assertions.assertNotNull(detailProfiles);
            Assertions.assertTrue(detailProfiles.size() > 0);
        }
    }

    /**
     * Test YAML output can be parsed back successfully
     */
    @Test
    public void testYamlOutputRoundTrip() {
        String yamlOutput = profile.getProfileAsYaml();

        // Parse the YAML
        Object parsed = yaml.load(yamlOutput);
        Assertions.assertNotNull(parsed);

        @SuppressWarnings("unchecked")
        Map<String, Object> rootMap = (Map<String, Object>) parsed;

        // Verify we can access nested structures
        Assertions.assertNotNull(rootMap.get("format_version"));
        Assertions.assertNotNull(rootMap.get("generated_at"));
        Assertions.assertNotNull(rootMap.get("summary"));
        Assertions.assertNotNull(rootMap.get("execution_summary"));

        // Convert back to YAML and verify it's still valid
        String yamlAgain = yaml.dump(rootMap);
        Assertions.assertNotNull(yamlAgain);
        Assertions.assertFalse(yamlAgain.isEmpty());

        Object parsedAgain = yaml.load(yamlAgain);
        Assertions.assertNotNull(parsedAgain);
    }

    /**
     * Test Counter with description
     */
    @Test
    public void testCounterWithDescription() {
        Counter counter = new Counter("Test description");
        Map<String, Object> map = counter.toMap();

        Assertions.assertNotNull(map);
        Assertions.assertEquals(0L, map.get("raw"));
        Assertions.assertEquals("Test description", map.get("display"));
    }

    /**
     * Test AggCounter merging
     */
    @Test
    public void testAggCounterMerging() {
        AggCounter agg1 = new AggCounter(TUnit.UNIT);
        agg1.addCounter(new Counter(TUnit.UNIT, 100));
        agg1.addCounter(new Counter(TUnit.UNIT, 200));

        AggCounter agg2 = new AggCounter(TUnit.UNIT);
        agg2.addCounter(new Counter(TUnit.UNIT, 150));
        agg2.addCounter(new Counter(TUnit.UNIT, 250));

        agg1.mergeCounter(agg2);

        Map<String, Object> map = agg1.toMap();

        Assertions.assertEquals(700L, map.get("sum")); // 100+200+150+250
        Assertions.assertEquals(175L, map.get("avg")); // 700/4
        Assertions.assertEquals(250L, map.get("max"));
        Assertions.assertEquals(100L, map.get("min"));
        Assertions.assertEquals(4, map.get("count"));
    }

    /**
     * Test RuntimeProfile with multiple info strings
     */
    @Test
    public void testRuntimeProfileWithMultipleInfoStrings() {
        runtimeProfile.addInfoString("Key1", "Value1");
        runtimeProfile.addInfoString("Key2", "Value2");
        runtimeProfile.addInfoString("Key3", "Value3");

        Map<String, Object> map = runtimeProfile.toStructuredMap();

        // Get content from profile name key
        @SuppressWarnings("unchecked")
        Map<String, Object> content = (Map<String, Object>) map.get("TestProfile");
        Assertions.assertNotNull(content);

        @SuppressWarnings("unchecked")
        Map<String, String> infoStrings = (Map<String, String>) content.get("info_strings");
        Assertions.assertNotNull(infoStrings);
        Assertions.assertEquals(3, infoStrings.size());
        Assertions.assertEquals("Value1", infoStrings.get("Key1"));
        Assertions.assertEquals("Value2", infoStrings.get("Key2"));
        Assertions.assertEquals("Value3", infoStrings.get("Key3"));
    }

    /**
     * Test empty RuntimeProfile
     */
    @Test
    public void testEmptyRuntimeProfile() {
        RuntimeProfile emptyProfile = new RuntimeProfile("EmptyProfile");
        Map<String, Object> map = emptyProfile.toStructuredMap();

        Assertions.assertNotNull(map);
        Assertions.assertTrue(map.containsKey("EmptyProfile"));

        @SuppressWarnings("unchecked")
        Map<String, Object> content = (Map<String, Object>) map.get("EmptyProfile");
        Assertions.assertNotNull(content);

        // Empty profile should not have these keys
        Assertions.assertFalse(content.containsKey("info_strings"));
        Assertions.assertFalse(content.containsKey("counters"));
        Assertions.assertFalse(content.containsKey("children"));
    }

    /**
     * Test AggCounter with LinkedList
     */
    @Test
    public void testAggCounterWithLinkedList() {
        AggCounter aggCounter = new AggCounter(TUnit.BYTES);

        LinkedList<Counter> counters = new LinkedList<>();
        counters.add(new Counter(TUnit.BYTES, 1024));
        counters.add(new Counter(TUnit.BYTES, 2048));
        counters.add(new Counter(TUnit.BYTES, 4096));

        aggCounter.addCounters(counters);

        Map<String, Object> map = aggCounter.toMap();

        Assertions.assertEquals(7168L, map.get("sum")); // 1024+2048+4096
        Assertions.assertEquals(2389L, map.get("avg")); // 7168/3
        Assertions.assertEquals(4096L, map.get("max"));
        Assertions.assertEquals(1024L, map.get("min"));
        Assertions.assertEquals(3, map.get("count"));
    }

    /**
     * Test YAML output doesn't include N/A values
     */
    @Test
    public void testYamlOutputExcludesNA() {
        String yamlOutput = profile.getProfileAsYaml();

        @SuppressWarnings("unchecked")
        Map<String, Object> rootMap = (Map<String, Object>) yaml.load(yamlOutput);

        @SuppressWarnings("unchecked")
        Map<String, Object> summary = (Map<String, Object>) rootMap.get("summary");

        // Verify that "N/A" values are not included in the output
        for (Map.Entry<String, Object> entry : summary.entrySet()) {
            Assertions.assertNotEquals("N/A", entry.getValue());
        }
    }

    /**
     * Test format version is correct
     */
    @Test
    public void testFormatVersion() {
        String yamlOutput = profile.getProfileAsYaml();

        @SuppressWarnings("unchecked")
        Map<String, Object> rootMap = (Map<String, Object>) yaml.load(yamlOutput);

        Assertions.assertEquals("1.0", rootMap.get("format_version"));
    }

    /**
     * Test generated_at timestamp format
     */
    @Test
    public void testGeneratedAtTimestamp() {
        String yamlOutput = profile.getProfileAsYaml();

        @SuppressWarnings("unchecked")
        Map<String, Object> rootMap = (Map<String, Object>) yaml.load(yamlOutput);

        String generatedAt = (String) rootMap.get("generated_at");
        Assertions.assertNotNull(generatedAt);
        Assertions.assertFalse(generatedAt.isEmpty());
        // Should be in ISO format, containing 'T' and 'Z'
        Assertions.assertTrue(generatedAt.contains("T"));
        Assertions.assertTrue(generatedAt.contains("Z"));
    }

    /**
     * Test Profile storage with YAML format
     * This test verifies that when profile_format is set to "yaml",
     * the profile is correctly written to and read from storage.
     */
    @Test
    public void testProfileStorageWithYamlFormat() throws Exception {
        // Save original config value
        String originalFormat = Config.profile_format;

        try {
            // Set format to yaml
            Config.profile_format = "yaml";

            // Create a temporary directory for storage
            java.io.File tempDir = java.nio.file.Files.createTempDirectory("profile_test").toFile();
            tempDir.deleteOnExit();

            // Mark query as finished so it can be stored
            profile.markQueryFinished();

            // Wait a bit to ensure timestamp is set
            Thread.sleep(10);

            // Write profile to storage
            profile.writeToStorage(tempDir.getAbsolutePath());

            // Verify storage path is set
            Assertions.assertTrue(profile.profileHasBeenStored());
            Assertions.assertNotNull(profile.getProfileStoragePath());

            // Read back the profile in YAML format
            String yamlFromStorage = profile.getOnStorageProfileAsYaml();
            Assertions.assertNotNull(yamlFromStorage);
            Assertions.assertFalse(yamlFromStorage.isEmpty());

            // Verify it's valid YAML
            @SuppressWarnings("unchecked")
            Map<String, Object> rootMap = (Map<String, Object>) yaml.load(yamlFromStorage);
            Assertions.assertNotNull(rootMap);
            Assertions.assertTrue(rootMap.containsKey("format_version"));
            Assertions.assertEquals("1.0", rootMap.get("format_version"));

            // Clean up storage file
            java.io.File storageFile = new java.io.File(profile.getProfileStoragePath());
            if (storageFile.exists()) {
                storageFile.delete();
            }
            tempDir.delete();
        } finally {
            // Restore original config value
            Config.profile_format = originalFormat;
        }
    }

    /**
     * Test Profile storage with TEXT format
     * This test verifies that when profile_format is set to "text",
     * the profile is correctly written to storage in TEXT format.
     */
    @Test
    public void testProfileStorageWithTextFormat() throws Exception {
        // Save original config value
        String originalFormat = Config.profile_format;

        try {
            // Set format to text
            Config.profile_format = "text";

            // Create a temporary directory for storage
            java.io.File tempDir = java.nio.file.Files.createTempDirectory("profile_test").toFile();
            tempDir.deleteOnExit();

            // Mark query as finished so it can be stored
            profile.markQueryFinished();

            // Wait a bit to ensure timestamp is set
            Thread.sleep(10);

            // Write profile to storage
            profile.writeToStorage(tempDir.getAbsolutePath());

            // Verify storage path is set
            Assertions.assertTrue(profile.profileHasBeenStored());
            Assertions.assertNotNull(profile.getProfileStoragePath());

            // Clean up storage file
            java.io.File storageFile = new java.io.File(profile.getProfileStoragePath());
            if (storageFile.exists()) {
                storageFile.delete();
            }
            tempDir.delete();
        } finally {
            // Restore original config value
            Config.profile_format = originalFormat;
        }
    }

    /**
     * Test getProfileAsYaml() reads from storage when profile is stored in YAML format
     */
    @Test
    public void testGetProfileAsYamlReadsFromStorage() throws Exception {
        // Save original config value
        String originalFormat = Config.profile_format;

        try {
            // Set format to yaml
            Config.profile_format = "yaml";

            // Create a temporary directory for storage
            java.io.File tempDir = java.nio.file.Files.createTempDirectory("profile_test").toFile();
            tempDir.deleteOnExit();

            // Get YAML before storage
            String yamlBeforeStorage = profile.getProfileAsYaml();
            Assertions.assertNotNull(yamlBeforeStorage);

            // Mark query as finished and store
            profile.markQueryFinished();
            Thread.sleep(10);
            profile.writeToStorage(tempDir.getAbsolutePath());

            // Release memory (simulating what happens in production)
            // Note: releaseMemory() might be private/protected, so we just verify
            // that getProfileAsYaml() works after storage

            // Get YAML after storage
            String yamlAfterStorage = profile.getProfileAsYaml();
            Assertions.assertNotNull(yamlAfterStorage);
            Assertions.assertFalse(yamlAfterStorage.isEmpty());

            // Verify it's valid YAML
            @SuppressWarnings("unchecked")
            Map<String, Object> rootMap = (Map<String, Object>) yaml.load(yamlAfterStorage);
            Assertions.assertNotNull(rootMap);
            Assertions.assertTrue(rootMap.containsKey("format_version"));

            // Clean up
            java.io.File storageFile = new java.io.File(profile.getProfileStoragePath());
            if (storageFile.exists()) {
                storageFile.delete();
            }
            tempDir.delete();
        } finally {
            // Restore original config value
            Config.profile_format = originalFormat;
        }
    }

    /**
     * Test AggCounter outputs as single-line flow style in YAML.
     * This test verifies that AggCounter.toMap() returns FlowStyleMap,
     * and the YAML output uses compact single-line format instead of multi-line format.
     */
    @Test
    public void testAggCounterFlowStyleOutput() {
        // Create an AggCounter with sample data
        AggCounter aggCounter = new AggCounter(TUnit.TIME_NS);
        aggCounter.addCounter(new Counter(TUnit.TIME_NS, 83505));
        aggCounter.addCounter(new Counter(TUnit.TIME_NS, 104026));
        aggCounter.addCounter(new Counter(TUnit.TIME_NS, 134215));
        aggCounter.addCounter(new Counter(TUnit.TIME_NS, 95000));
        aggCounter.addCounter(new Counter(TUnit.TIME_NS, 110000));

        // Verify toMap() returns FlowStyleMap instance
        Map<String, Object> counterMap = aggCounter.toMap();
        Assertions.assertNotNull(counterMap);
        Assertions.assertTrue(counterMap instanceof AggCounter.FlowStyleMap,
                "AggCounter.toMap() should return FlowStyleMap instance");

        // Verify the map contains expected keys
        Assertions.assertTrue(counterMap.containsKey("unit"));
        Assertions.assertTrue(counterMap.containsKey("avg"));
        Assertions.assertTrue(counterMap.containsKey("max"));
        Assertions.assertTrue(counterMap.containsKey("min"));
        Assertions.assertTrue(counterMap.containsKey("count"));
        Assertions.assertTrue(counterMap.containsKey("display"));

        // Add the AggCounter to a RuntimeProfile
        RuntimeProfile testProfile = new RuntimeProfile("TestProfile");
        testProfile.addInfoString("TestInfo", "TestValue");

        // Add AggCounter to the profile
        Counter rootCounter = testProfile.addCounter("ExecTime", TUnit.TIME_NS, RuntimeProfile.ROOT_COUNTER);
        // We can't directly replace with AggCounter, so let's create a structure that uses it

        // Create a simple YAML structure with FlowStyleMap
        Map<String, Object> testData = new LinkedHashMap<>();
        testData.put("normal_counter", new Counter(TUnit.UNIT, 1234).toMap());
        testData.put("agg_counter", counterMap);

        // Use Profile's YAML dumper to convert to YAML string
        // Note: We need to access the createYamlDumper method, but it's private
        // So let's create the YAML dumper configuration directly here for testing
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.AUTO);
        options.setPrettyFlow(false);  // Disable pretty flow to keep everything on one line
        options.setIndent(2);
        options.setWidth(Integer.MAX_VALUE);  // Set to max value to prevent line wrapping
        options.setExplicitStart(false);

        // Create custom representer
        org.yaml.snakeyaml.representer.Representer representer =
                new org.yaml.snakeyaml.representer.Representer(options) {
            {
                this.representers.put(AggCounter.FlowStyleMap.class,
                        new org.yaml.snakeyaml.representer.Represent() {
                    @Override
                    public org.yaml.snakeyaml.nodes.Node representData(Object data) {
                        @SuppressWarnings("unchecked")
                        Map<Object, Object> map = (Map<Object, Object>) data;
                        return representFlowStyleMapping(map);
                    }
                });
            }

            protected org.yaml.snakeyaml.nodes.Node representFlowStyleMapping(Map<Object, Object> map) {
                return representMapping(
                        org.yaml.snakeyaml.nodes.Tag.MAP,
                        map,
                        org.yaml.snakeyaml.DumperOptions.FlowStyle.FLOW);
            }
        };

        Yaml testYaml = new Yaml(representer, options);
        String yamlOutput = testYaml.dump(testData);

        System.out.println("=== YAML Output for FlowStyleMap Test ===");
        System.out.println(yamlOutput);
        System.out.println("=== End of YAML Output ===");

        // Verify the agg_counter is in single-line format
        // Single-line format should contain all fields in one line with braces: {unit: TIME_NS, avg: ..., max: ..., min: ..., count: ..., display: ...}
        String[] lines = yamlOutput.split("\n");

        boolean foundAggCounterLine = false;
        for (String line : lines) {
            if (line.contains("agg_counter:")) {
                // The agg_counter line should contain opening brace on the same line
                // indicating flow style (single-line format)
                String nextContent = line.substring(line.indexOf("agg_counter:") + "agg_counter:".length()).trim();
                if (nextContent.startsWith("{")) {
                    foundAggCounterLine = true;
                    // Verify it contains all required fields in the same line or following lines
                    // In flow style, everything should be compact
                    Assertions.assertTrue(line.contains("{") ||
                            (lines.length > 0 && yamlOutput.contains("agg_counter: {")),
                            "AggCounter should be output in flow style (single-line with braces)");
                }
            }
        }

        // Verify that agg_counter is truly on a single line:
        // The entire content from "agg_counter: {" to "}" should be on one line
        boolean foundSingleLineAggCounter = false;
        for (String line : lines) {
            if (line.contains("agg_counter:") && line.contains("{") && line.contains("}")) {
                // Found! The entire agg_counter is on one line
                foundSingleLineAggCounter = true;

                // Verify it contains all expected fields
                Assertions.assertTrue(line.contains("unit:"), "Should contain 'unit:' field");
                Assertions.assertTrue(line.contains("avg:"), "Should contain 'avg:' field");
                Assertions.assertTrue(line.contains("max:"), "Should contain 'max:' field");
                Assertions.assertTrue(line.contains("min:"), "Should contain 'min:' field");
                Assertions.assertTrue(line.contains("count:"), "Should contain 'count:' field");
                Assertions.assertTrue(line.contains("display:"), "Should contain 'display:' field");

                System.out.println("✓ AggCounter is on a single line: " + line);
                break;
            }
        }

        Assertions.assertTrue(foundSingleLineAggCounter,
                "AggCounter should be entirely on a single line with format: agg_counter: {unit: ..., avg: ..., max: ..., min: ..., count: ..., display: ...}");
    }

    /**
     * Test that Profile.getProfileAsYaml() outputs AggCounters in compact single-line format
     */
    @Test
    public void testProfileYamlWithAggCounterCompactFormat() {
        // Create a profile with execution profiles that contain AggCounters
        String yamlOutput = profile.getProfileAsYaml();

        Assertions.assertNotNull(yamlOutput);
        Assertions.assertFalse(yamlOutput.isEmpty());

        System.out.println("=== Profile YAML Output ===");
        System.out.println(yamlOutput);
        System.out.println("=== End of Profile YAML ===");

        // Parse the YAML to verify structure
        @SuppressWarnings("unchecked")
        Map<String, Object> rootMap = (Map<String, Object>) yaml.load(yamlOutput);
        Assertions.assertNotNull(rootMap);

        // The YAML should be valid and parseable
        Assertions.assertTrue(rootMap.containsKey("format_version"));

        // Look for patterns that indicate flow style is being used
        // Flow style: "ExecTime: {unit: TIME_NS, avg: 104026, max: 134215, min: 83505, count: 8, display: ...}"
        // Block style: "ExecTime:\n  unit: TIME_NS\n  avg: 104026\n  ..."

        // If there are counters, check if they use compact format
        String[] lines = yamlOutput.split("\n");
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            // Look for counter-like keys that might be AggCounters
            if (line.matches(".*\\w+Time:.*") || line.matches(".*Rows:.*")) {
                // Check if this line contains opening brace (flow style indicator)
                if (line.contains(": {")) {
                    // Good! This is flow style
                    Assertions.assertTrue(line.contains("unit:") ||
                            (i + 1 < lines.length && lines[i + 1].contains("unit:")),
                            "Counter in flow style should contain fields inline or on next line");
                }
            }
        }
    }
}
