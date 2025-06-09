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

package org.apache.doris.analysis;

import org.apache.doris.analysis.AbstractBackupTableRefClause;
import org.apache.doris.analysis.LabelName;
import org.apache.doris.analysis.RestoreStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class RestoreStmtMediumSyncSimpleTest {

    private LabelName labelName;
    private String repoName = "test_repo";
    private AbstractBackupTableRefClause tableRefClause;

    @Before
    public void setUp() {
        labelName = new LabelName("test_db", "test_snapshot");
        
        List<TableRef> tableRefs = Lists.newArrayList();
        tableRefs.add(new TableRef(new TableName(null, null, "test_table"), null));
        tableRefClause = new AbstractBackupTableRefClause(false, tableRefs);
    }

    @Test
    public void testMediumSyncPolicyHDD() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("backup_timestamp", "2023-12-01-10-00-00");
        properties.put(RestoreStmt.PROP_MEDIUM_SYNC_POLICY, RestoreStmt.MEDIUM_SYNC_POLICY_HDD);

        RestoreStmt stmt = new RestoreStmt(labelName, repoName, tableRefClause, properties);
        stmt.analyzeProperties(); // Need to analyze properties to process medium_sync_policy

        Assert.assertEquals("Medium sync policy should be hdd", 
                          RestoreStmt.MEDIUM_SYNC_POLICY_HDD, stmt.getMediumSyncPolicy());
    }

    @Test
    public void testMediumSyncPolicySameWithUpstream() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("backup_timestamp", "2023-12-01-10-00-00");
        properties.put(RestoreStmt.PROP_MEDIUM_SYNC_POLICY, RestoreStmt.MEDIUM_SYNC_POLICY_SAME_WITH_UPSTREAM);

        RestoreStmt stmt = new RestoreStmt(labelName, repoName, tableRefClause, properties);
        stmt.analyzeProperties(); // Need to analyze properties to process medium_sync_policy

        Assert.assertEquals("Medium sync policy should be same_with_upstream", 
                          RestoreStmt.MEDIUM_SYNC_POLICY_SAME_WITH_UPSTREAM, stmt.getMediumSyncPolicy());
    }

    @Test
    public void testMediumSyncPolicyDefault() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("backup_timestamp", "2023-12-01-10-00-00");
        // No medium_sync_policy specified

        RestoreStmt stmt = new RestoreStmt(labelName, repoName, tableRefClause, properties);
        stmt.analyzeProperties(); // Need to analyze properties

        Assert.assertEquals("Default medium sync policy should be hdd", 
                          RestoreStmt.MEDIUM_SYNC_POLICY_HDD, stmt.getMediumSyncPolicy());
    }

    @Test
    public void testMediumSyncPolicyCaseInsensitive() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("backup_timestamp", "2023-12-01-10-00-00");
        properties.put(RestoreStmt.PROP_MEDIUM_SYNC_POLICY, "HDD"); // uppercase

        RestoreStmt stmt = new RestoreStmt(labelName, repoName, tableRefClause, properties);
        stmt.analyzeProperties(); // Need to analyze properties

        Assert.assertEquals("Medium sync policy should be normalized to lowercase", 
                          RestoreStmt.MEDIUM_SYNC_POLICY_HDD, stmt.getMediumSyncPolicy());

        // Test mixed case
        properties.remove(RestoreStmt.PROP_MEDIUM_SYNC_POLICY);
        properties.put(RestoreStmt.PROP_MEDIUM_SYNC_POLICY, "Same_With_Upstream");
        stmt = new RestoreStmt(labelName, repoName, tableRefClause, properties);
        stmt.analyzeProperties(); // Need to analyze properties

        Assert.assertEquals("Medium sync policy should be normalized to lowercase", 
                          RestoreStmt.MEDIUM_SYNC_POLICY_SAME_WITH_UPSTREAM, stmt.getMediumSyncPolicy());
    }

    @Test
    public void testInvalidMediumSyncPolicy() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("backup_timestamp", "2023-12-01-10-00-00");
        properties.put(RestoreStmt.PROP_MEDIUM_SYNC_POLICY, "invalid_policy");

        RestoreStmt stmt = new RestoreStmt(labelName, repoName, tableRefClause, properties);
        
        try {
            // Trigger validation by calling analyzeProperties
            stmt.analyzeProperties();
            Assert.fail("Should throw AnalysisException for invalid medium sync policy");
        } catch (AnalysisException e) {
            Assert.assertTrue("Error message should mention invalid medium sync policy", 
                            e.getMessage().contains("Invalid medium sync policy value"));
            Assert.assertTrue("Error message should list valid values", 
                            e.getMessage().contains("hdd") && e.getMessage().contains("same_with_upstream"));
        }
    }

    @Test
    public void testMediumSyncPolicyWithOtherProperties() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("backup_timestamp", "2023-12-01-10-00-00");
        properties.put("reserve_replica", "true");
        properties.put("atomic_restore", "true");
        properties.put(RestoreStmt.PROP_MEDIUM_SYNC_POLICY, RestoreStmt.MEDIUM_SYNC_POLICY_SAME_WITH_UPSTREAM);

        RestoreStmt stmt = new RestoreStmt(labelName, repoName, tableRefClause, properties);
        stmt.analyzeProperties(); // Need to analyze properties

        Assert.assertEquals("Medium sync policy should be preserved with other properties", 
                          RestoreStmt.MEDIUM_SYNC_POLICY_SAME_WITH_UPSTREAM, stmt.getMediumSyncPolicy());
        
        // Verify other properties don't interfere
        Assert.assertTrue("Reserve replica should be true", stmt.reserveReplica());
        Assert.assertTrue("Atomic restore should be true", stmt.isAtomicRestore());
    }

    @Test
    public void testEmptyMediumSyncPolicy() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("backup_timestamp", "2023-12-01-10-00-00");
        properties.put(RestoreStmt.PROP_MEDIUM_SYNC_POLICY, "");

        RestoreStmt stmt = new RestoreStmt(labelName, repoName, tableRefClause, properties);
        
        try {
            stmt.analyzeProperties();
            Assert.fail("Should throw AnalysisException for empty medium sync policy");
        } catch (AnalysisException e) {
            Assert.assertTrue("Error message should mention invalid medium sync policy", 
                            e.getMessage().contains("Invalid medium sync policy value"));
        }
    }

    @Test 
    public void testWhitespaceMediumSyncPolicy() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("backup_timestamp", "2023-12-01-10-00-00");
        properties.put(RestoreStmt.PROP_MEDIUM_SYNC_POLICY, "   ");

        RestoreStmt stmt = new RestoreStmt(labelName, repoName, tableRefClause, properties);

        try {
            stmt.analyzeProperties();
            Assert.fail("Should throw AnalysisException for whitespace medium sync policy");
        } catch (AnalysisException e) {
            Assert.assertTrue("Error message should mention invalid medium sync policy", 
                            e.getMessage().contains("Invalid medium sync policy value"));
        }
    }

    @Test
    public void testMediumSyncPolicyToSql() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("backup_timestamp", "2023-12-01-10-00-00");
        properties.put(RestoreStmt.PROP_MEDIUM_SYNC_POLICY, RestoreStmt.MEDIUM_SYNC_POLICY_SAME_WITH_UPSTREAM);

        RestoreStmt stmt = new RestoreStmt(labelName, repoName, tableRefClause, properties);
        stmt.analyzeProperties(); // Need to analyze properties
        String sql = stmt.toSql();

        Assert.assertTrue("SQL should contain medium sync policy", 
                         sql.toLowerCase().contains("medium_sync_policy"));
        Assert.assertTrue("SQL should contain same_with_upstream", 
                         sql.toLowerCase().contains("same_with_upstream"));
    }

    @Test
    public void testMediumSyncPolicyValidValues() throws Exception {
        // Test all valid values
        String[] validValues = {
            RestoreStmt.MEDIUM_SYNC_POLICY_HDD,
            RestoreStmt.MEDIUM_SYNC_POLICY_SAME_WITH_UPSTREAM,
            "HDD",
            "SAME_WITH_UPSTREAM",
            "hdd",
            "same_with_upstream"
        };

        for (String value : validValues) {
            Map<String, String> properties = Maps.newHashMap();
            properties.put("backup_timestamp", "2023-12-01-10-00-00");
            properties.put(RestoreStmt.PROP_MEDIUM_SYNC_POLICY, value);

            RestoreStmt stmt = new RestoreStmt(labelName, repoName, tableRefClause, properties);
            stmt.analyzeProperties(); // Need to analyze properties
            
            // Should not throw exception
            String normalizedValue = stmt.getMediumSyncPolicy();
            Assert.assertTrue("Should be one of the valid normalized values", 
                            normalizedValue.equals(RestoreStmt.MEDIUM_SYNC_POLICY_HDD) || 
                            normalizedValue.equals(RestoreStmt.MEDIUM_SYNC_POLICY_SAME_WITH_UPSTREAM));
        }
    }

    @Test
    public void testMediumSyncPolicyInvalidValues() throws Exception {
        String[] invalidValues = {
            "ssd", "invalid", "null", "auto", "mixed", "123", "same_upstream"
        };

        for (String value : invalidValues) {
            Map<String, String> properties = Maps.newHashMap();
            properties.put("backup_timestamp", "2023-12-01-10-00-00");
            properties.put(RestoreStmt.PROP_MEDIUM_SYNC_POLICY, value);

            RestoreStmt stmt = new RestoreStmt(labelName, repoName, tableRefClause, properties);
            
            try {
                stmt.analyzeProperties();
                Assert.fail("Should throw AnalysisException for invalid value: " + value);
            } catch (AnalysisException e) {
                Assert.assertTrue("Error message should mention invalid value", 
                                e.getMessage().contains("Invalid medium sync policy value"));
            }
        }
    }
} 