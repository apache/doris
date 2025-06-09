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

package org.apache.doris.backup;

import org.apache.doris.analysis.RestoreStmt;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Env;

import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class RestoreJobMediumSyncTest {

    @Mocked
    private Env env;

    @Test
    public void testMediumSyncPolicyHDD() {
        // Test case 1: medium_sync_policy = "hdd" should not preserve upstream storage medium
        RestoreJob restoreJob = createRestoreJobWithMediumSyncPolicy(RestoreStmt.MEDIUM_SYNC_POLICY_HDD);
        
        // Verify that preserveStorageMedium() returns false for HDD policy
        Assert.assertFalse("HDD policy should not preserve upstream storage medium", 
                          restoreJob.preserveStorageMedium());
        
        // Verify medium sync policy is correctly set
        Assert.assertEquals("Medium sync policy should be HDD", 
                          RestoreStmt.MEDIUM_SYNC_POLICY_HDD, restoreJob.getMediumSyncPolicy());
    }

    @Test
    public void testMediumSyncPolicySameWithUpstream() {
        // Test case 2: medium_sync_policy = "same_with_upstream" should preserve upstream storage medium
        RestoreJob restoreJob = createRestoreJobWithMediumSyncPolicy(RestoreStmt.MEDIUM_SYNC_POLICY_SAME_WITH_UPSTREAM);
        
        // Verify that preserveStorageMedium() returns true for same_with_upstream policy
        Assert.assertTrue("Same with upstream policy should preserve upstream storage medium", 
                         restoreJob.preserveStorageMedium());
        
        // Verify medium sync policy is correctly set
        Assert.assertEquals("Medium sync policy should be same_with_upstream", 
                          RestoreStmt.MEDIUM_SYNC_POLICY_SAME_WITH_UPSTREAM, restoreJob.getMediumSyncPolicy());
    }

    @Test
    public void testDefaultMediumSyncPolicy() {
        // Test case 3: Test default behavior when no medium sync policy is specified
        RestoreJob restoreJob = createRestoreJobWithMediumSyncPolicy(null);
        
        // Verify default behavior (should be same as HDD policy)
        Assert.assertFalse("Default policy should not preserve upstream storage medium", 
                          restoreJob.preserveStorageMedium());
        
        // Verify medium sync policy is null
        Assert.assertNull("Medium sync policy should be null when not specified", 
                         restoreJob.getMediumSyncPolicy());
    }
    
    @Test 
    public void testMediumPolicyArchitectureImprovement() {
        // Test that the new architecture passes policy instead of pre-determined medium
        RestoreJob sameWithUpstreamJob = createRestoreJobWithMediumSyncPolicy(RestoreStmt.MEDIUM_SYNC_POLICY_SAME_WITH_UPSTREAM);
        RestoreJob hddJob = createRestoreJobWithMediumSyncPolicy(RestoreStmt.MEDIUM_SYNC_POLICY_HDD);
        
        // Verify that resetIdsForRestore will receive the policy, not a pre-computed medium
        // This allows per-partition medium decision making inside resetIdsForRestore
        Assert.assertEquals("Should preserve policy for downstream processing", 
                          RestoreStmt.MEDIUM_SYNC_POLICY_SAME_WITH_UPSTREAM, sameWithUpstreamJob.getMediumSyncPolicy());
        Assert.assertEquals("Should preserve policy for downstream processing", 
                          RestoreStmt.MEDIUM_SYNC_POLICY_HDD, hddJob.getMediumSyncPolicy());
        
        // The decision logic should be delegated to OlapTable.resetIdsForRestore method
        // which can handle per-partition medium selection based on the policy
        Assert.assertTrue("Same with upstream should trigger preserve logic", sameWithUpstreamJob.preserveStorageMedium());
        Assert.assertFalse("HDD policy should not trigger preserve logic", hddJob.preserveStorageMedium());
    }

    @Test
    public void testInvalidMediumSyncPolicy() {
        // Test case 4: Test behavior with invalid medium sync policy
        RestoreJob restoreJob = createRestoreJobWithMediumSyncPolicy("invalid_policy");
        
        // Verify that invalid policy is treated as default (like HDD policy)
        Assert.assertFalse("Invalid policy should not preserve upstream storage medium", 
                          restoreJob.preserveStorageMedium());
        
        // Verify medium sync policy is set to the invalid value
        Assert.assertEquals("Medium sync policy should be the invalid value", 
                          "invalid_policy", restoreJob.getMediumSyncPolicy());
    }

    private RestoreJob createRestoreJobWithMediumSyncPolicy(String mediumSyncPolicy) {
        BackupJobInfo jobInfo = new BackupJobInfo();
        jobInfo.dbName = "test_db";
        
        return new RestoreJob("test_restore", "20231201000000", 1, "test_db", 
                            jobInfo, false, ReplicaAllocation.DEFAULT_ALLOCATION, 
                            3600000, -1, false, false, false, false, 
                            false, false, false, false, mediumSyncPolicy, env, 1);
    }
} 