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

package org.apache.doris.resource;

import org.apache.doris.catalog.LocalReplica;
import org.apache.doris.catalog.Replica;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

class BackendSelectionServiceTest {

    private static final Function<Candidate, Tag> CANDIDATE_TAG = candidate -> Tag.DEFAULT_BACKEND_TAG;

    @Test
    void testChooseLoadBackendRecordsAndReusesResolvedHint() throws Exception {
        ConnectContext context = new ConnectContext();
        Backend first = availableBackend(1L);
        Backend preferred = availableBackend(2L);
        List<Backend> candidates = ImmutableList.of(first, preferred);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.PREFER, "test");
        BackendSelectionPolicy policy = Mockito.mock(BackendSelectionPolicy.class);
        Mockito.when(policy.isLoadSelectionEnabled(context)).thenReturn(true);
        Mockito.when(policy.getLoadSelectionHint(context)).thenReturn(hint);
        Mockito.when(policy.hasLoadSelectionPreference(hint)).thenReturn(true);
        Mockito.when(policy.orderLoadCandidates(hint, candidates))
                .thenReturn(ImmutableList.of(preferred, first));

        try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);

            Assertions.assertSame(preferred, BackendSelectionService.chooseLoadBackend(context, candidates));
            Assertions.assertSame(hint, context.getLoadBackendSelectionDecision());
            Assertions.assertSame(hint, BackendSelectionService.resolveLoadSelectionHint(context));

            Mockito.verify(policy, Mockito.times(1)).getLoadSelectionHint(context);
            Mockito.verify(policy).orderLoadCandidates(hint, candidates);
        }
    }

    @Test
    void testNullContextDoesNotInvokeExtensionPolicy() throws Exception {
        try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedFactory.when(BackendSelectionPolicyFactory::get)
                    .thenThrow(new AssertionError("null context must not reach the extension policy"));

            Assertions.assertNull(BackendSelectionService.resolveLoadSelectionHint(null));
            Backend unavailable = availableBackend(1L);
            unavailable.setAlive(false);
            Backend available = availableBackend(2L);
            Assertions.assertSame(available, BackendSelectionService.chooseLoadBackend(
                    null, ImmutableList.of(unavailable, available)));
            available.setAlive(false);
            Assertions.assertNull(BackendSelectionService.chooseLoadBackend(
                    null, ImmutableList.of(unavailable, available)));
            mockedFactory.verifyNoInteractions();
        }
    }

    @Test
    void testRecordingNullClearsPreviousLoadHint() {
        ConnectContext context = new ConnectContext();
        context.recordLoadBackendSelectionDecision(new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.PREFER, "test"));

        context.recordLoadBackendSelectionDecision(null);

        Assertions.assertNull(context.getLoadBackendSelectionDecision());
    }

    @Test
    void testCaptureRestoreAndOrderLoadSelection() throws Exception {
        ConnectContext submissionContext = new ConnectContext();
        ConnectContext executionContext = new ConnectContext();
        Backend first = availableBackend(1L);
        Backend preferred = availableBackend(2L);
        List<Backend> candidates = ImmutableList.of(first, preferred);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.PREFER, "persisted");
        BackendSelectionPolicy policy = Mockito.mock(BackendSelectionPolicy.class);
        Mockito.when(policy.isLoadSelectionEnabled(submissionContext)).thenReturn(true);
        Mockito.when(policy.getLoadSelectionHint(submissionContext)).thenReturn(hint);
        Mockito.when(policy.hasLoadSelectionPreference(hint)).thenReturn(true);
        Mockito.when(policy.orderLoadCandidates(hint, candidates))
                .thenReturn(ImmutableList.of(preferred, first));

        try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);

            BackendSelection.SelectionHint captured =
                    BackendSelectionService.captureLoadSelection(submissionContext);
            BackendSelectionService.restoreLoadSelection(executionContext, captured);

            Assertions.assertSame(hint, captured);
            Assertions.assertSame(hint, executionContext.getLoadBackendSelectionDecision());
            Assertions.assertEquals(ImmutableList.of(preferred, first),
                    BackendSelectionService.orderLoadCandidates(executionContext, candidates));
            Mockito.verify(policy).getLoadSelectionHint(submissionContext);
            Mockito.verify(policy, Mockito.never()).getLoadSelectionHint(executionContext);
        }
    }

    @Test
    void testCloudModeGatesLoadSelectionResolution() {
        String oldCloudUniqueId = Config.cloud_unique_id;
        try {
            Config.cloud_unique_id = "cloud-test";
            ConnectContext context = new ConnectContext();
            context.recordLoadBackendSelectionDecision(new BackendSelection.SelectionHint(
                    "key_a", BackendSelection.Mode.PREFER, "test"));
            try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                    Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
                mockedFactory.when(BackendSelectionPolicyFactory::get)
                        .thenThrow(new AssertionError("cloud mode must not reach the extension policy"));

                Assertions.assertFalse(BackendSelectionService.isLoadSelectionEnabled(context));
                Assertions.assertNull(BackendSelectionService.resolveLoadSelectionHint(context));
                mockedFactory.verifyNoInteractions();
            }
        } finally {
            Config.cloud_unique_id = oldCloudUniqueId;
        }
    }

    @Test
    void testRequiredLoadSelectionReturnsOnlyPreferredCandidates() throws Exception {
        Backend first = availableBackend(1L);
        Backend preferred = availableBackend(2L);
        List<Backend> candidates = ImmutableList.of(first, preferred);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.REQUIRE, "test");
        BackendSelectionPolicy policy = Mockito.mock(BackendSelectionPolicy.class);
        Mockito.when(policy.partitionRequiredLoadCandidates(hint, candidates))
                .thenReturn(new BackendSelection.CandidateSelection<>(
                        ImmutableList.of(preferred), ImmutableList.of(first)));

        try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);

            Assertions.assertEquals(ImmutableList.of(preferred),
                    BackendSelectionService.orderLoadCandidates(hint, candidates));
        }
    }

    @Test
    void testRequiredLoadSelectionRejectsNoPreferredCandidate() throws Exception {
        Backend first = availableBackend(1L);
        List<Backend> candidates = ImmutableList.of(first);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.REQUIRE, "test");
        BackendSelectionPolicy policy = Mockito.mock(BackendSelectionPolicy.class);
        Mockito.when(policy.partitionRequiredLoadCandidates(hint, candidates))
                .thenReturn(new BackendSelection.CandidateSelection<>(ImmutableList.of(), candidates));

        try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);

            UserException exception = Assertions.assertThrows(UserException.class,
                    () -> BackendSelectionService.orderLoadCandidates(hint, candidates));
            Assertions.assertTrue(exception.getMessage().contains("No candidate satisfies required backend selection"));
        }
    }

    @Test
    void testRequiredLoadSelectionRejectsDroppedCandidate() throws Exception {
        assertInvalidRequiredLoadPartition(candidates -> new BackendSelection.CandidateSelection<>(
                ImmutableList.of(candidates.get(0)), ImmutableList.of()));
    }

    private void assertInvalidRequiredLoadPartition(
            Function<List<Backend>, BackendSelection.CandidateSelection<Backend>> invalidPartition)
            throws Exception {
        Backend first = availableBackend(1L);
        Backend second = availableBackend(2L);
        List<Backend> candidates = ImmutableList.of(first, second);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.REQUIRE, "test");
        BackendSelectionPolicy policy = Mockito.mock(BackendSelectionPolicy.class);
        Mockito.when(policy.partitionRequiredLoadCandidates(hint, candidates))
                .thenReturn(invalidPartition.apply(candidates));

        try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);

            UserException exception = Assertions.assertThrows(UserException.class,
                    () -> BackendSelectionService.orderLoadCandidates(hint, candidates));
            Assertions.assertTrue(exception.getMessage().contains("must partition all candidates"));
        }
    }

    @Test
    void testRequiredQuerySelectionReturnsOnlyPreferredCandidates() throws Exception {
        Candidate first = new Candidate("first");
        Candidate preferred = new Candidate("preferred");
        List<Candidate> candidates = ImmutableList.of(first, preferred);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.REQUIRE, "test");
        BackendSelectionPolicy policy = Mockito.mock(BackendSelectionPolicy.class);
        Mockito.when(policy.partitionRequiredQueryCandidates(hint, candidates, CANDIDATE_TAG))
                .thenReturn(new BackendSelection.CandidateSelection<>(
                        ImmutableList.of(preferred), ImmutableList.of(first)));

        try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);

            Assertions.assertEquals(ImmutableList.of(preferred),
                    BackendSelectionService.orderQueryCandidates(hint, candidates, CANDIDATE_TAG));
        }
    }

    @Test
    void testOrderQueryCandidatesWithinTiesReordersOnlyWithinTieGroups() throws Exception {
        Candidate first = new Candidate("first", 10);
        Candidate second = new Candidate("second", 10);
        Candidate third = new Candidate("third", 9);
        Candidate fourth = new Candidate("fourth", 9);
        List<Candidate> candidates = ImmutableList.of(first, second, third, fourth);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.PREFER, "test");
        BackendSelectionPolicy policy = Mockito.mock(BackendSelectionPolicy.class);
        Mockito.when(policy.hasQuerySelectionPreference(hint)).thenReturn(true);
        Mockito.when(policy.orderQueryCandidates(
                        Mockito.eq(hint), Mockito.anyList(), Mockito.eq(CANDIDATE_TAG)))
                .thenAnswer(invocation -> {
                    List<Candidate> ordered = invocation.getArgument(1);
                    Collections.reverse(ordered);
                    return ordered;
                });

        try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);

            List<Candidate> ordered = BackendSelectionService.orderQueryCandidatesWithinTies(
                    hint, candidates, Comparator.comparingInt(candidate -> candidate.priority), CANDIDATE_TAG);

            Assertions.assertEquals(ImmutableList.of(second, first, fourth, third), ordered);
            Assertions.assertNotSame(candidates, ordered);
            Assertions.assertEquals(ImmutableList.of(first, second, third, fourth), candidates);
            Mockito.verify(policy, Mockito.times(2)).orderQueryCandidates(
                    Mockito.eq(hint), Mockito.anyList(), Mockito.eq(CANDIDATE_TAG));
        }
    }

    @Test
    void testOrderQueryCandidatesWithinTiesReturnsOriginalWhenProviderKeepsOrder() throws Exception {
        Candidate first = new Candidate("first", 10);
        Candidate second = new Candidate("second", 9);
        List<Candidate> candidates = ImmutableList.of(first, second);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.PREFER, "test");
        BackendSelectionPolicy policy = Mockito.mock(BackendSelectionPolicy.class);
        Mockito.when(policy.hasQuerySelectionPreference(hint)).thenReturn(true);
        Mockito.when(policy.orderQueryCandidates(
                        Mockito.eq(hint), Mockito.anyList(), Mockito.eq(CANDIDATE_TAG)))
                .thenAnswer(invocation -> invocation.getArgument(1));

        try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);

            Assertions.assertSame(candidates, BackendSelectionService.orderQueryCandidatesWithinTies(
                    hint, candidates, Comparator.comparingInt(candidate -> candidate.priority), CANDIDATE_TAG));
        }
    }

    @Test
    void testOrderQueryCandidatesWithinTiesRejectsCandidateOutsideCurrentTieGroup() throws Exception {
        Candidate first = new Candidate("first", 10);
        Candidate second = new Candidate("second", 10);
        Candidate outside = new Candidate("outside", 9);
        List<Candidate> candidates = ImmutableList.of(first, second, outside);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.PREFER, "test");
        BackendSelectionPolicy policy = Mockito.mock(BackendSelectionPolicy.class);
        Mockito.when(policy.hasQuerySelectionPreference(hint)).thenReturn(true);
        Mockito.when(policy.orderQueryCandidates(
                        Mockito.eq(hint), Mockito.anyList(), Mockito.eq(CANDIDATE_TAG)))
                .thenAnswer(invocation -> ImmutableList.of(outside, first));

        try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);

            UserException exception = Assertions.assertThrows(UserException.class,
                    () -> BackendSelectionService.orderQueryCandidatesWithinTies(
                            hint, candidates, Comparator.comparingInt(candidate -> candidate.priority), CANDIDATE_TAG));
            Assertions.assertTrue(exception.getMessage().contains("orderQueryCandidatesWithinTies"));
        }
    }

    @Test
    void testOrderQueryCandidatesWithinTiesRejectsDroppedCandidate() throws Exception {
        assertInvalidTieOrder(candidates -> ImmutableList.of(candidates.get(0)));
    }

    @Test
    void testOrderQueryCandidatesWithinTiesRejectsDuplicateCandidate() throws Exception {
        assertInvalidTieOrder(candidates -> ImmutableList.of(candidates.get(0), candidates.get(0)));
    }

    @Test
    void testRequiredQuerySelectionWithinTiesUsesGlobalPartition() throws Exception {
        Candidate first = new Candidate("first", 10);
        Candidate second = new Candidate("second", 9);
        List<Candidate> candidates = ImmutableList.of(first, second);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.REQUIRE, "test");
        BackendSelectionPolicy policy = Mockito.mock(BackendSelectionPolicy.class);
        BackendSelection.CandidateSelection<Candidate> selection =
                new BackendSelection.CandidateSelection<>(ImmutableList.of(second), ImmutableList.of(first));
        Mockito.when(policy.partitionRequiredQueryCandidates(hint, candidates, CANDIDATE_TAG))
                .thenReturn(selection);

        try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);

            Assertions.assertEquals(ImmutableList.of(second),
                    BackendSelectionService.orderQueryCandidatesWithinTies(
                            hint, candidates, Comparator.comparingInt(candidate -> candidate.priority), CANDIDATE_TAG));
            Mockito.verify(policy).partitionRequiredQueryCandidates(hint, candidates, CANDIDATE_TAG);
            Mockito.verify(policy, Mockito.never()).orderQueryCandidates(
                    Mockito.any(), Mockito.anyList(), Mockito.any());
        }
    }

    @Test
    void testClassifyQuerySelectionUsesProviderOutcome() {
        Candidate candidate = new Candidate("candidate");
        List<Candidate> candidates = ImmutableList.of(candidate);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.PREFER, "test");
        BackendSelectionPolicy policy = Mockito.mock(BackendSelectionPolicy.class);
        Mockito.when(policy.hasQuerySelectionPreference(hint)).thenReturn(true);
        Mockito.when(policy.classifyQuerySelection(hint, candidates, CANDIDATE_TAG))
                .thenReturn(BackendSelection.QuerySelectionResult.FALLBACK_PREFERRED_UNAVAILABLE);

        try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);

            Assertions.assertEquals(BackendSelection.QuerySelectionResult.FALLBACK_PREFERRED_UNAVAILABLE,
                    BackendSelectionService.classifyQuerySelection(hint, candidates, CANDIDATE_TAG));
        }
    }

    @Test
    void testClassifyRequiredQuerySelectionAsPreferredHit() {
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.REQUIRE, "test");

        Assertions.assertEquals(BackendSelection.QuerySelectionResult.PREFERRED_HIT,
                BackendSelectionService.classifyQuerySelection(
                        hint, ImmutableList.of(new Candidate("candidate")), CANDIDATE_TAG));
    }

    @Test
    void testRequiredSingleReplicaSelectionDoesNotFallBack() throws Exception {
        ConnectContext context = new ConnectContext();
        Backend unavailable = availableBackend(1L);
        unavailable.setAlive(false);
        List<Backend> candidates = ImmutableList.of(unavailable);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.REQUIRE, "test");
        context.recordLoadBackendSelectionDecision(hint);
        BackendSelectionPolicy policy = Mockito.mock(BackendSelectionPolicy.class);
        Mockito.when(policy.partitionRequiredLoadCandidates(hint, candidates))
                .thenReturn(new BackendSelection.CandidateSelection<>(candidates, ImmutableList.of()));

        try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);

            UserException exception = Assertions.assertThrows(UserException.class,
                    () -> BackendSelectionService.chooseFirstPreferredLoadBackend(
                            context, candidates, Backend::isLoadAvailable));
            Assertions.assertTrue(exception.getMessage().contains("No available candidate satisfies required"));
        }
    }

    @Test
    void testOrderQueryCandidatesRejectsDroppedCandidate() throws Exception {
        assertInvalidQueryOrder(candidates -> ImmutableList.of(candidates.get(0)));
    }

    @Test
    void testOrderQueryCandidatesRejectsAddedCandidate() throws Exception {
        assertInvalidQueryOrder(candidates -> ImmutableList.of(candidates.get(0), new Candidate("same")));
    }

    @Test
    void testOrderQueryCandidatesRejectsDuplicateCandidate() throws Exception {
        assertInvalidQueryOrder(candidates -> ImmutableList.of(candidates.get(0), candidates.get(0)));
    }

    @Test
    void testOrderRepairSourceCandidatesRejectsInvalidProvider() throws Exception {
        Replica first = new LocalReplica();
        Replica second = new LocalReplica();
        BackendSelectionPolicy policy = new BackendSelectionPolicy() {
            @Override
            public List<Replica> orderRepairSourceCandidates(List<Replica> candidates, long destBackendId) {
                return ImmutableList.of(first, first);
            }
        };
        try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);
            UserException exception = Assertions.assertThrows(UserException.class,
                    () -> BackendSelectionService.orderRepairSourceCandidates(ImmutableList.of(first, second), 3L));
            Assertions.assertTrue(exception.getMessage().contains("orderRepairSourceCandidates"));
        }
    }

    private void assertInvalidQueryOrder(Function<List<Candidate>, List<Candidate>> invalidOrder) throws Exception {
        Candidate first = new Candidate("same");
        Candidate second = new Candidate("same");
        List<Candidate> candidates = ImmutableList.of(first, second);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.PREFER, "test");
        BackendSelectionPolicy policy = Mockito.mock(BackendSelectionPolicy.class);
        Mockito.when(policy.hasQuerySelectionPreference(hint)).thenReturn(true);
        Mockito.when(policy.orderQueryCandidates(hint, candidates, CANDIDATE_TAG))
                .thenReturn(invalidOrder.apply(candidates));
        try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);
            UserException exception = Assertions.assertThrows(UserException.class,
                    () -> BackendSelectionService.orderQueryCandidates(hint, candidates, CANDIDATE_TAG));
            Assertions.assertTrue(exception.getMessage().contains("orderQueryCandidates"));
        }
    }

    private void assertInvalidTieOrder(Function<List<Candidate>, List<Candidate>> invalidOrder) throws Exception {
        Candidate first = new Candidate("first", 10);
        Candidate second = new Candidate("second", 10);
        List<Candidate> candidates = ImmutableList.of(first, second);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.PREFER, "test");
        BackendSelectionPolicy policy = Mockito.mock(BackendSelectionPolicy.class);
        Mockito.when(policy.hasQuerySelectionPreference(hint)).thenReturn(true);
        Mockito.when(policy.orderQueryCandidates(
                        Mockito.eq(hint), Mockito.anyList(), Mockito.eq(CANDIDATE_TAG)))
                .thenAnswer(invocation -> invalidOrder.apply(invocation.getArgument(1)));
        try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);
            UserException exception = Assertions.assertThrows(UserException.class,
                    () -> BackendSelectionService.orderQueryCandidatesWithinTies(
                            hint, candidates, Comparator.comparingInt(candidate -> candidate.priority), CANDIDATE_TAG));
            Assertions.assertTrue(exception.getMessage().contains("orderQueryCandidatesWithinTies"));
        }
    }

    private static final class Candidate {
        private final String value;
        private final int priority;

        private Candidate(String value) {
            this(value, 0);
        }

        private Candidate(String value, int priority) {
            this.value = value;
            this.priority = priority;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Candidate && value.equals(((Candidate) other).value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }

    private Backend availableBackend(long id) {
        Backend backend = new Backend(id, "127.0.0." + id, 9050);
        backend.setAlive(true);
        return backend;
    }
}
