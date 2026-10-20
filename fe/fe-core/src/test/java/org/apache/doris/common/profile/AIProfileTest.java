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

import org.apache.doris.common.util.SafeStringBuilder;
import org.apache.doris.thrift.TCounter;
import org.apache.doris.thrift.TDetailedReportParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TQueryProfile;
import org.apache.doris.thrift.TRuntimeProfileNode;
import org.apache.doris.thrift.TRuntimeProfileTree;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TUnit;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class AIProfileTest {
    private static final TNetworkAddress BE_A = new TNetworkAddress("127.0.0.1", 9050);
    private static final TNetworkAddress BE_B = new TNetworkAddress("127.0.0.2", 9050);

    @Test
    public void testQuerySummarySumsLatestFragmentSnapshots() {
        Profile profile = ProfilePersistentTest.constructRandomProfile(0);
        ExecutionProfile execution = new ExecutionProfile(new TUniqueId(1, 2), Lists.newArrayList(3, 7));
        profile.addExecutionProfile(execution);
        execution.addFragmentBackend(3, 1L);
        execution.addFragmentBackend(3, 2L);
        execution.addFragmentBackend(7, 1L);
        report(execution, 3, BE_A, false, counter("AIFunctionRequestCount", TUnit.UNIT, 2),
                counter("AIFunctionTotalTokens", TUnit.UNIT, 10));
        report(execution, 3, BE_B, true, counter("AIFunctionRequestCount", TUnit.UNIT, 3),
                counter("AIFunctionTotalTokens", TUnit.UNIT, 20));
        report(execution, 7, BE_A, true, counter("AIFunctionRequestCount", TUnit.UNIT, 4),
                counter("AIFunctionTotalTokens", TUnit.UNIT, 30));
        report(execution, 3, BE_A, true, counter("AIFunctionRequestCount", TUnit.UNIT, 5),
                counter("AIFunctionTotalTokens", TUnit.UNIT, 40));
        // Repeated final reports and late periodic reports must not count twice or replace final values.
        report(execution, 3, BE_A, true, counter("AIFunctionRequestCount", TUnit.UNIT, 5),
                counter("AIFunctionTotalTokens", TUnit.UNIT, 40));
        report(execution, 3, BE_A, false, counter("AIFunctionRequestCount", TUnit.UNIT, 1));
        Assertions.assertTrue(execution.isCompleted());
        RuntimeProfile counters = new RuntimeProfile("AIFunctionSummary");
        execution.addAIFunctionSummaryCounters(counters);
        Assertions.assertEquals(1, counters.getCounterMap().get("AIFunctionRequestCount").getLevel());
        String text = content(profile);
        String summary = summary(text);
        Assertions.assertTrue(text.substring(text.indexOf("DetailProfile("))
                .contains("AIFunctionTotalTokens: 40"), text);
        Assertions.assertTrue(summary.contains("AIFunctionRequestCount: 12"), summary);
        Assertions.assertTrue(summary.contains("AIFunctionTotalTokens: 90"), summary);
    }

    @Test
    public void testSummaryPreservesUnitsAndPartialUsageAcrossExecutions() {
        Profile profile = ProfilePersistentTest.constructRandomProfile(2);
        report(profile.getExecutionProfiles().get(0), 0, BE_A, true,
                counter("AIFunctionRequestCount", TUnit.UNIT, 3),
                counter("AIFunctionPromptTokens", TUnit.UNIT, 7),
                counter("AIFunctionPromptUsageCount", TUnit.UNIT, 1),
                counter("AIFunctionHttpTime", TUnit.TIME_NS, 1000000));
        report(profile.getExecutionProfiles().get(1), 1, BE_A, true,
                counter("AIFunctionRequestCount", TUnit.UNIT, 2),
                counter("AIFunctionHttpTime", TUnit.TIME_NS, 2000000),
                counter("BytesRead", TUnit.BYTES, 1024),
                counter("AIUnrelatedCounter", TUnit.UNIT, 99));
        String summary = summary(content(profile));
        Assertions.assertTrue(summary.contains("AIFunctionRequestCount: 5"), summary);
        Assertions.assertTrue(summary.contains("AIFunctionPromptTokens: 7"), summary);
        Assertions.assertTrue(summary.contains("AIFunctionPromptUsageCount: 1"), summary);
        Assertions.assertTrue(summary.contains("AIFunctionHttpTime: 3.0ms"), summary);
        Assertions.assertFalse(summary.contains("AIFunctionCompletionTokens"), summary);
        Assertions.assertFalse(summary.contains("BytesRead"), summary);
        Assertions.assertFalse(summary.contains("AIUnrelatedCounter"), summary);
    }

    @Test
    public void testReplacementDoesNotRetainMissingUsageCounters() {
        Profile profile = ProfilePersistentTest.constructRandomProfile(1);
        ExecutionProfile execution = profile.getExecutionProfiles().get(0);
        report(execution, 0, BE_A, false, counter("AIFunctionRequestCount", TUnit.UNIT, 1),
                counter("AIFunctionTotalTokens", TUnit.UNIT, 10));
        report(execution, 0, BE_A, true, counter("AIFunctionRequestCount", TUnit.UNIT, 2));
        String summary = summary(content(profile));
        Assertions.assertTrue(summary.contains("AIFunctionRequestCount: 2"), summary);
        Assertions.assertFalse(summary.contains("AIFunctionTotalTokens"), summary);
    }

    @Test
    public void testNonAiQueryHasNoSummary() {
        Profile profile = ProfilePersistentTest.constructRandomProfile(1);
        report(profile.getExecutionProfiles().get(0), 0, BE_A, true,
                counter("BytesRead", TUnit.BYTES, 10));
        Assertions.assertFalse(content(profile).contains("AIFunctionSummary"));
    }

    @Test
    public void testSummaryCanBeReadWhileBackendsReport() throws Exception {
        Profile profile = ProfilePersistentTest.constructRandomProfile(1);
        ExecutionProfile execution = profile.getExecutionProfiles().get(0);
        report(execution, 0, BE_A, false, counter("AIFunctionRequestCount", TUnit.UNIT, 0));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<?> reporter = executor.submit(() -> {
                start.await();
                for (int i = 1; i <= 20; ++i) {
                    report(execution, 0, BE_A, i == 20, counter("AIFunctionRequestCount", TUnit.UNIT, i));
                }
                return null;
            });
            Future<?> reader = executor.submit(() -> {
                start.await();
                for (int i = 0; i < 20; ++i) {
                    Assertions.assertTrue(summary(content(profile)).contains("AIFunctionRequestCount:"));
                }
                return null;
            });
            start.countDown();
            reporter.get(30, TimeUnit.SECONDS);
            reader.get(30, TimeUnit.SECONDS);
            Assertions.assertTrue(summary(content(profile)).contains("AIFunctionRequestCount: 20"));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testSummaryUsesSnapshotWhenRetryExecutionIsAdded() {
        Profile profile = ProfilePersistentTest.constructRandomProfile(0);
        ExecutionProfile retry = new ExecutionProfile(new TUniqueId(3, 4), Lists.newArrayList(0));
        ExecutionProfile first = new ExecutionProfile(new TUniqueId(1, 2), Lists.newArrayList(0)) {
            private boolean retryAdded;

            @Override
            void addAIFunctionSummaryCounters(RuntimeProfile summary) {
                super.addAIFunctionSummaryCounters(summary);
                // Deterministically model a retry appending its execution during profile retrieval.
                if (!retryAdded) {
                    profile.addExecutionProfile(retry);
                    retryAdded = true;
                }
            }
        };
        profile.addExecutionProfile(first);
        report(first, 0, BE_A, true, counter("AIFunctionRequestCount", TUnit.UNIT, 2));
        report(retry, 0, BE_A, true, counter("AIFunctionRequestCount", TUnit.UNIT, 3));

        String firstSummary = summary(content(profile));
        Assertions.assertTrue(firstSummary.contains("AIFunctionRequestCount: 2"), firstSummary);
        String nextSummary = summary(content(profile));
        Assertions.assertTrue(nextSummary.contains("AIFunctionRequestCount: 5"), nextSummary);
    }

    @Test
    public void testSummarySurvivesProfileStorage(@TempDir Path directory) {
        Profile profile = ProfilePersistentTest.constructRandomProfile(1);
        report(profile.getExecutionProfiles().get(0), 0, BE_A, true,
                counter("AIFunctionRequestCount", TUnit.UNIT, 2), counter("AIFunctionTotalTokens", TUnit.UNIT, 17));
        String before = summary(content(profile));
        profile.markQueryFinished();
        profile.writeToStorage(directory.toString());
        Assertions.assertTrue(profile.profileHasBeenStored());
        Profile restored = Profile.read(profile.getProfileStoragePath());
        Assertions.assertNotNull(restored);
        SafeStringBuilder builder = new SafeStringBuilder();
        restored.getOnStorageProfile(builder);
        Assertions.assertEquals(before, summary(builder.toString()));
    }

    private static TCounter counter(String name, TUnit unit, long value) {
        return new TCounter(name, unit, value).setLevel(1);
    }

    private static void report(ExecutionProfile execution, int fragmentId, TNetworkAddress backend,
            boolean done, TCounter... counters) {
        TRuntimeProfileNode root = new TRuntimeProfileNode();
        root.setName("Fragment");
        root.setNumChildren(0);
        root.setCounters(Arrays.asList(counters));
        root.setInfoStrings(Maps.newHashMap());
        root.setInfoStringsDisplayOrder(Lists.newArrayList());
        Set<String> counterNames = new HashSet<>();
        for (TCounter counter : counters) {
            counterNames.add(counter.getName());
        }
        root.setChildCountersMap(Collections.singletonMap(RuntimeProfile.ROOT_COUNTER, counterNames));
        TDetailedReportParams fragment = new TDetailedReportParams();
        fragment.setIsFragmentLevel(true);
        fragment.setProfile(new TRuntimeProfileTree(Lists.newArrayList(root)));
        // AI counters on a pipeline/task are not a fragment contribution.
        TRuntimeProfileNode pipeline = root.deepCopy();
        pipeline.setCounters(Lists.newArrayList(counter("AIFunctionRequestCount", TUnit.UNIT, 1000)));
        pipeline.setChildCountersMap(Collections.singletonMap(RuntimeProfile.ROOT_COUNTER,
                Collections.singleton("AIFunctionRequestCount")));
        TDetailedReportParams task = new TDetailedReportParams();
        TRuntimeProfileNode firstTask = pipeline.deepCopy().setName("PipelineTask 0");
        TRuntimeProfileNode secondTask = pipeline.deepCopy().setName("PipelineTask 1");
        pipeline.setNumChildren(2);
        task.setProfile(new TRuntimeProfileTree(Lists.newArrayList(pipeline, firstTask, secondTask)));
        TQueryProfile query = new TQueryProfile();
        query.setQueryId(execution.getQueryId());
        query.setFragmentIdToProfile(Collections.singletonMap(fragmentId, Lists.newArrayList(fragment, task)));
        Assertions.assertTrue(execution.updateProfile(query, backend, done).ok());
    }

    private static String content(Profile profile) {
        SafeStringBuilder builder = new SafeStringBuilder();
        profile.getExecutionProfileContent(builder);
        return builder.toString();
    }

    private static String summary(String profile) {
        int start = profile.indexOf("AIFunctionSummary:");
        Assertions.assertTrue(start >= 0, profile);
        int end = profile.indexOf("MergedProfile:", start);
        if (end < 0) {
            end = profile.indexOf("DetailProfile(", start);
        }
        return profile.substring(start, end).trim();
    }
}
