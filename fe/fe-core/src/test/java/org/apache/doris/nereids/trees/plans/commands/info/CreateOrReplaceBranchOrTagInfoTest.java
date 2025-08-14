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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class CreateOrReplaceBranchOrTagInfoTest {

    @Test
    public void testCreateBranchToSql() {
        // Test CREATE BRANCH with no options
        CreateOrReplaceBranchInfo branchInfo = new CreateOrReplaceBranchInfo(
                "test_branch", true, false, false, null);
        String expected = "CREATE BRANCH test_branch";
        Assertions.assertEquals(expected, branchInfo.toSql());
    }

    @Test
    public void testReplaceBranchToSql() {
        // Test REPLACE BRANCH with no options
        CreateOrReplaceBranchInfo branchInfo = new CreateOrReplaceBranchInfo(
                "test_branch", false, true, false, null);
        String expected = "REPLACE BRANCH test_branch";
        Assertions.assertEquals(expected, branchInfo.toSql());
    }

    @Test
    public void testCreateOrReplaceBranchToSql() {
        // Test CREATE OR REPLACE BRANCH with no options
        CreateOrReplaceBranchInfo branchInfo = new CreateOrReplaceBranchInfo(
                "test_branch", true, true, false, null);
        String expected = "CREATE OR REPLACE BRANCH test_branch";
        Assertions.assertEquals(expected, branchInfo.toSql());
    }

    @Test
    public void testCreateBranchIfNotExistsToSql() {
        // Test CREATE BRANCH IF NOT EXISTS
        CreateOrReplaceBranchInfo branchInfo = new CreateOrReplaceBranchInfo(
                "test_branch", true, false, true, null);
        String expected = "CREATE BRANCH IF NOT EXISTS test_branch";
        Assertions.assertEquals(expected, branchInfo.toSql());
    }

    @Test
    public void testCreateOrReplaceBranchIfNotExistsToSql() {
        // Test CREATE OR REPLACE BRANCH IF NOT EXISTS
        CreateOrReplaceBranchInfo branchInfo = new CreateOrReplaceBranchInfo(
                "test_branch", true, true, true, null);
        String expected = "CREATE OR REPLACE BRANCH IF NOT EXISTS test_branch";
        Assertions.assertEquals(expected, branchInfo.toSql());
    }

    @Test
    public void testCreateBranchWithSnapshotIdToSql() {
        // Test CREATE BRANCH with snapshot ID
        BranchOptions options = new BranchOptions(
                Optional.of(123456L), Optional.empty(), Optional.empty(), Optional.empty());
        CreateOrReplaceBranchInfo branchInfo = new CreateOrReplaceBranchInfo(
                "test_branch", true, false, false, options);
        String expected = "CREATE BRANCH test_branch AS OF VERSION 123456";
        Assertions.assertEquals(expected, branchInfo.toSql());
    }

    @Test
    public void testCreateBranchWithRetainToSql() {
        // Test CREATE BRANCH with retain time (1 hour = 3600000ms)
        BranchOptions options = new BranchOptions(
                Optional.empty(), Optional.of(3600000L), Optional.empty(), Optional.empty());
        CreateOrReplaceBranchInfo branchInfo = new CreateOrReplaceBranchInfo(
                "test_branch", true, false, false, options);
        String expected = "CREATE BRANCH test_branch RETAIN 60 MINUTES";
        Assertions.assertEquals(expected, branchInfo.toSql());
    }

    @Test
    public void testCreateBranchWithNumSnapshotsToSql() {
        // Test CREATE BRANCH with number of snapshots
        BranchOptions options = new BranchOptions(
                Optional.empty(), Optional.empty(), Optional.of(5), Optional.empty());
        CreateOrReplaceBranchInfo branchInfo = new CreateOrReplaceBranchInfo(
                "test_branch", true, false, false, options);
        String expected = "CREATE BRANCH test_branch WITH SNAPSHOT RETENTION 5 SNAPSHOTS";
        Assertions.assertEquals(expected, branchInfo.toSql());
    }

    @Test
    public void testCreateBranchWithRetentionToSql() {
        // Test CREATE BRANCH with retention time (1 day = 86400000ms)
        BranchOptions options = new BranchOptions(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(86400000L));
        CreateOrReplaceBranchInfo branchInfo = new CreateOrReplaceBranchInfo(
                "test_branch", true, false, false, options);
        String expected = "CREATE BRANCH test_branch WITH SNAPSHOT RETENTION 1440 MINUTES";
        Assertions.assertEquals(expected, branchInfo.toSql());
    }

    @Test
    public void testCreateBranchWithNumSnapshotsAndRetentionToSql() {
        // Test CREATE BRANCH with both num snapshots and retention
        BranchOptions options = new BranchOptions(
                Optional.empty(), Optional.empty(), Optional.of(10), Optional.of(86400000L));
        CreateOrReplaceBranchInfo branchInfo = new CreateOrReplaceBranchInfo(
                "test_branch", true, false, false, options);
        String expected = "CREATE BRANCH test_branch WITH SNAPSHOT RETENTION 10 SNAPSHOTS 1440 MINUTES";
        Assertions.assertEquals(expected, branchInfo.toSql());
    }

    @Test
    public void testCreateBranchWithAllOptionsToSql() {
        // Test CREATE BRANCH with all options
        BranchOptions options = new BranchOptions(
                Optional.of(123456L), Optional.of(3600000L), Optional.of(5), Optional.of(86400000L));
        CreateOrReplaceBranchInfo branchInfo = new CreateOrReplaceBranchInfo(
                "test_branch", true, false, false, options);
        String expected
                = "CREATE BRANCH test_branch AS OF VERSION 123456 RETAIN 60 MINUTES WITH SNAPSHOT RETENTION 5 SNAPSHOTS 1440 MINUTES";
        Assertions.assertEquals(expected, branchInfo.toSql());
    }

    @Test
    public void testEmptyOptionsToSql() {
        // Test with BranchOptions.EMPTY
        CreateOrReplaceBranchInfo branchInfo = new CreateOrReplaceBranchInfo(
                "test_branch", true, false, false, BranchOptions.EMPTY);
        String expected = "CREATE BRANCH test_branch";
        Assertions.assertEquals(expected, branchInfo.toSql());
    }

    @Test
    public void testBranchNameWithSpecialCharacters() {
        // Test branch name with underscores and numbers
        CreateOrReplaceBranchInfo branchInfo = new CreateOrReplaceBranchInfo(
                "feature_branch_v2_0", true, false, false, null);
        String expected = "CREATE BRANCH feature_branch_v2_0";
        Assertions.assertEquals(expected, branchInfo.toSql());
    }

    // ========================== Tag Tests ==========================

    @Test
    public void testCreateTagToSql() {
        // Test CREATE TAG with no options
        CreateOrReplaceTagInfo tagInfo = new CreateOrReplaceTagInfo(
                "test_tag", true, false, false, null);
        String expected = "CREATE TAG test_tag";
        Assertions.assertEquals(expected, tagInfo.toSql());
    }

    @Test
    public void testReplaceTagToSql() {
        // Test REPLACE TAG with no options
        CreateOrReplaceTagInfo tagInfo = new CreateOrReplaceTagInfo(
                "test_tag", false, true, false, null);
        String expected = "REPLACE TAG test_tag";
        Assertions.assertEquals(expected, tagInfo.toSql());
    }

    @Test
    public void testCreateOrReplaceTagToSql() {
        // Test CREATE OR REPLACE TAG with no options
        CreateOrReplaceTagInfo tagInfo = new CreateOrReplaceTagInfo(
                "test_tag", true, true, false, null);
        String expected = "CREATE OR REPLACE TAG test_tag";
        Assertions.assertEquals(expected, tagInfo.toSql());
    }

    @Test
    public void testCreateTagIfNotExistsToSql() {
        // Test CREATE TAG IF NOT EXISTS
        CreateOrReplaceTagInfo tagInfo = new CreateOrReplaceTagInfo(
                "test_tag", true, false, true, null);
        String expected = "CREATE TAG IF NOT EXISTS test_tag";
        Assertions.assertEquals(expected, tagInfo.toSql());
    }

    @Test
    public void testCreateOrReplaceTagIfNotExistsToSql() {
        // Test CREATE OR REPLACE TAG IF NOT EXISTS
        CreateOrReplaceTagInfo tagInfo = new CreateOrReplaceTagInfo(
                "test_tag", true, true, true, null);
        String expected = "CREATE OR REPLACE TAG IF NOT EXISTS test_tag";
        Assertions.assertEquals(expected, tagInfo.toSql());
    }

    @Test
    public void testCreateTagWithSnapshotIdToSql() {
        // Test CREATE TAG with snapshot ID
        TagOptions options = new TagOptions(Optional.of(123456L), Optional.empty());
        CreateOrReplaceTagInfo tagInfo = new CreateOrReplaceTagInfo(
                "test_tag", true, false, false, options);
        String expected = "CREATE TAG test_tag AS OF VERSION 123456";
        Assertions.assertEquals(expected, tagInfo.toSql());
    }

    @Test
    public void testCreateTagWithRetainToSql() {
        // Test CREATE TAG with retain time (1 hour = 3600000ms)
        TagOptions options = new TagOptions(Optional.empty(), Optional.of(3600000L));
        CreateOrReplaceTagInfo tagInfo = new CreateOrReplaceTagInfo(
                "test_tag", true, false, false, options);
        String expected = "CREATE TAG test_tag RETAIN 60 MINUTES";
        Assertions.assertEquals(expected, tagInfo.toSql());
    }

    @Test
    public void testCreateTagWithAllOptionsToSql() {
        // Test CREATE TAG with all options (snapshot ID and retain)
        TagOptions options = new TagOptions(Optional.of(123456L), Optional.of(3600000L));
        CreateOrReplaceTagInfo tagInfo = new CreateOrReplaceTagInfo(
                "test_tag", true, false, false, options);
        String expected = "CREATE TAG test_tag AS OF VERSION 123456 RETAIN 60 MINUTES";
        Assertions.assertEquals(expected, tagInfo.toSql());
    }

    @Test
    public void testEmptyTagOptionsToSql() {
        // Test with TagOptions.EMPTY
        CreateOrReplaceTagInfo tagInfo = new CreateOrReplaceTagInfo(
                "test_tag", true, false, false, TagOptions.EMPTY);
        String expected = "CREATE TAG test_tag";
        Assertions.assertEquals(expected, tagInfo.toSql());
    }

    @Test
    public void testTagNameWithSpecialCharacters() {
        // Test tag name with underscores and numbers
        CreateOrReplaceTagInfo tagInfo = new CreateOrReplaceTagInfo(
                "release_tag_v1_0", true, false, false, null);
        String expected = "CREATE TAG release_tag_v1_0";
        Assertions.assertEquals(expected, tagInfo.toSql());
    }

    @Test
    public void testReplaceTagWithOptionsToSql() {
        // Test REPLACE TAG with snapshot ID and retain
        TagOptions options = new TagOptions(Optional.of(789012L), Optional.of(7200000L));
        CreateOrReplaceTagInfo tagInfo = new CreateOrReplaceTagInfo(
                "production_tag", false, true, false, options);
        String expected = "REPLACE TAG production_tag AS OF VERSION 789012 RETAIN 120 MINUTES";
        Assertions.assertEquals(expected, tagInfo.toSql());
    }

    @Test
    public void testCreateOrReplaceTagWithSnapshotOnlyToSql() {
        // Test CREATE OR REPLACE TAG with only snapshot ID
        TagOptions options = new TagOptions(Optional.of(555666L), Optional.empty());
        CreateOrReplaceTagInfo tagInfo = new CreateOrReplaceTagInfo(
                "backup_tag", true, true, false, options);
        String expected = "CREATE OR REPLACE TAG backup_tag AS OF VERSION 555666";
        Assertions.assertEquals(expected, tagInfo.toSql());
    }

    @Test
    public void testCreateTagIfNotExistsWithRetainToSql() {
        // Test CREATE TAG IF NOT EXISTS with retain time
        TagOptions options = new TagOptions(Optional.empty(), Optional.of(86400000L));
        CreateOrReplaceTagInfo tagInfo = new CreateOrReplaceTagInfo(
                "daily_tag", true, false, true, options);
        String expected = "CREATE TAG IF NOT EXISTS daily_tag RETAIN 1440 MINUTES";
        Assertions.assertEquals(expected, tagInfo.toSql());
    }
}
