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
import org.apache.doris.resource.spi.BackendSelectionProvider;
import org.apache.doris.system.Backend;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

class BackendSelectionManagerTest {

    @AfterEach
    void resetBackendSelectionProvider() {
        BackendSelectionManager.resetProviderForTest();
    }


    private static final Function<Candidate, Tag> CANDIDATE_TAG = candidate -> Tag.DEFAULT_BACKEND_TAG;

    @Test
    void testLoadProviderRejectsDuplicateSpiImplementations() {
        ClassLoader classLoader = new BackendSelectionSpiClassLoader(
                FirstBackendSelectionProvider.class.getName(), SecondBackendSelectionProvider.class.getName());

        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class,
                () -> BackendSelectionManager.loadProvider(classLoader));

        Assertions.assertTrue(exception.getMessage().contains(FirstBackendSelectionProvider.class.getName()));
        Assertions.assertTrue(exception.getMessage().contains(SecondBackendSelectionProvider.class.getName()));
    }

    @Test
    void testLoadProviderPropagatesProviderConstructionFailure() {
        ClassLoader classLoader = new SingleBackendSelectionSpiClassLoader(
                FailingBackendSelectionProvider.class.getName());

        java.util.ServiceConfigurationError exception = Assertions.assertThrows(
                java.util.ServiceConfigurationError.class,
                () -> BackendSelectionManager.loadProvider(classLoader));

        Assertions.assertTrue(exception.getMessage().contains(FailingBackendSelectionProvider.class.getName()));
    }

    @Test
    void testNoProviderPreservesKernelQueryLoadAndRepairBehavior() throws Exception {
        BackendSelectionProvider noOpProvider = BackendSelectionManager.loadProvider(
                new EmptyBackendSelectionSpiClassLoader());
        BackendSelectionManager.setProviderForTest(noOpProvider);

        ConnectContext context = new ConnectContext();
        Candidate firstQueryCandidate = new Candidate("first");
        Candidate secondQueryCandidate = new Candidate("second");
        List<Candidate> queryCandidates = ImmutableList.of(firstQueryCandidate, secondQueryCandidate);
        Backend firstLoadCandidate = availableBackend(1L);
        Backend secondLoadCandidate = availableBackend(2L);
        List<Backend> loadCandidates = ImmutableList.of(firstLoadCandidate, secondLoadCandidate);
        Replica firstRepairCandidate = new LocalReplica();
        Replica secondRepairCandidate = new LocalReplica();
        List<Replica> repairCandidates = ImmutableList.of(firstRepairCandidate, secondRepairCandidate);

        BackendSelection.SelectionHint queryHint = BackendSelectionManager.getQuerySelectionHint(context);
        Assertions.assertEquals(BackendSelection.Mode.DEFAULT, queryHint.getMode());
        Assertions.assertEquals("", queryHint.getPreferredKey());
        Assertions.assertFalse(BackendSelectionManager.supportsRequiredSelection());
        Assertions.assertFalse(BackendSelectionManager.isLoadSelectionEnabled(context));
        Assertions.assertNull(BackendSelectionManager.resolveLoadSelectionHint(context));
        Assertions.assertFalse(BackendSelectionManager.isRepairSourceSelectionEnabled());
        Assertions.assertSame(queryCandidates,
                BackendSelectionManager.orderQueryCandidates(queryHint, queryCandidates, CANDIDATE_TAG));
        Assertions.assertSame(loadCandidates,
                BackendSelectionManager.orderLoadCandidates(context, loadCandidates));
        Assertions.assertSame(repairCandidates,
                BackendSelectionManager.orderRepairSourceCandidates(repairCandidates, 3L));
        Assertions.assertEquals(BackendSelectionProvider.RepairSourceSelectionResult.DISABLED,
                BackendSelectionManager.classifyRepairSource(
                        1L, 3L, repairCandidates, repairCandidates));
    }

    @Test
    void testChooseLoadBackendRecordsAndReusesResolvedHint() throws Exception {
        ConnectContext context = new ConnectContext();
        Backend first = availableBackend(1L);
        Backend preferred = availableBackend(2L);
        List<Backend> candidates = ImmutableList.of(first, preferred);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.PREFER, "test");
        BackendSelectionProvider policy = Mockito.mock(BackendSelectionProvider.class);
        Mockito.when(policy.isLoadSelectionEnabled(context)).thenReturn(true);
        Mockito.when(policy.getLoadSelectionHint(context)).thenReturn(hint);
        Mockito.when(policy.hasLoadSelectionPreference(hint)).thenReturn(true);
        Mockito.when(policy.orderLoadCandidates(hint, candidates))
                .thenReturn(ImmutableList.of(preferred, first));

        BackendSelectionManager.setProviderForTest(policy);

        Assertions.assertSame(preferred, BackendSelectionManager.chooseLoadBackend(context, candidates));
        Assertions.assertSame(hint, context.getLoadBackendSelectionDecision());
        Assertions.assertSame(hint, BackendSelectionManager.resolveLoadSelectionHint(context));

        Mockito.verify(policy, Mockito.times(1)).getLoadSelectionHint(context);
        Mockito.verify(policy).orderLoadCandidates(hint, candidates);
    }

    @Test
    void testNullContextDoesNotInvokeExtensionPolicy() throws Exception {
        BackendSelectionProvider provider = Mockito.mock(BackendSelectionProvider.class);
        BackendSelectionManager.setProviderForTest(provider);

        Assertions.assertNull(BackendSelectionManager.resolveLoadSelectionHint(null));
        Backend unavailable = availableBackend(1L);
        unavailable.setAlive(false);
        Backend available = availableBackend(2L);
        Assertions.assertSame(available, BackendSelectionManager.chooseLoadBackend(
                null, ImmutableList.of(unavailable, available)));
        available.setAlive(false);
        Assertions.assertNull(BackendSelectionManager.chooseLoadBackend(
                null, ImmutableList.of(unavailable, available)));
        Mockito.verifyNoInteractions(provider);
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
        BackendSelectionProvider policy = Mockito.mock(BackendSelectionProvider.class);
        Mockito.when(policy.isLoadSelectionEnabled(submissionContext)).thenReturn(true);
        Mockito.when(policy.getLoadSelectionHint(submissionContext)).thenReturn(hint);
        Mockito.when(policy.hasLoadSelectionPreference(hint)).thenReturn(true);
        Mockito.when(policy.orderLoadCandidates(hint, candidates))
                .thenReturn(ImmutableList.of(preferred, first));

        BackendSelectionManager.setProviderForTest(policy);

        BackendSelection.SelectionHint captured =
                    BackendSelectionManager.captureLoadSelection(submissionContext);
        BackendSelectionManager.restoreLoadSelection(executionContext, captured);

        Assertions.assertSame(hint, captured);
        Assertions.assertSame(hint, executionContext.getLoadBackendSelectionDecision());
        Assertions.assertEquals(ImmutableList.of(preferred, first),
                    BackendSelectionManager.orderLoadCandidates(executionContext, candidates));
        Mockito.verify(policy).getLoadSelectionHint(submissionContext);
        Mockito.verify(policy, Mockito.never()).getLoadSelectionHint(executionContext);
    }

    @Test
    void testRestoredLoadSelectionSurvivesStatementContextReset() {
        ConnectContext context = new ConnectContext();
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.REQUIRE, "persisted");

        BackendSelectionManager.restoreLoadSelection(context, hint);
        context.setStartTime();

        Assertions.assertSame(hint, BackendSelectionManager.resolveLoadSelectionHint(context));
    }

    @Test
    void testCloudModeGatesLoadSelectionResolution() {
        String oldCloudUniqueId = Config.cloud_unique_id;
        try {
            Config.cloud_unique_id = "cloud-test";
            ConnectContext context = new ConnectContext();
            context.recordLoadBackendSelectionDecision(new BackendSelection.SelectionHint(
                    "key_a", BackendSelection.Mode.PREFER, "test"));
            BackendSelectionProvider provider = Mockito.mock(BackendSelectionProvider.class);
            BackendSelectionManager.setProviderForTest(provider);

            Assertions.assertFalse(BackendSelectionManager.isLoadSelectionEnabled(context));
            Assertions.assertNull(BackendSelectionManager.resolveLoadSelectionHint(context));
            Mockito.verifyNoInteractions(provider);
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
        BackendSelectionProvider policy = Mockito.mock(BackendSelectionProvider.class);
        Mockito.when(policy.partitionRequiredLoadCandidates(hint, candidates))
                .thenReturn(new BackendSelection.CandidateSelection<>(
                        ImmutableList.of(preferred), ImmutableList.of(first)));

        BackendSelectionManager.setProviderForTest(policy);

        Assertions.assertEquals(ImmutableList.of(preferred),
                    BackendSelectionManager.orderLoadCandidates(hint, candidates));
    }

    @Test
    void testRequiredLoadSelectionRejectsNoPreferredCandidate() throws Exception {
        Backend first = availableBackend(1L);
        List<Backend> candidates = ImmutableList.of(first);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.REQUIRE, "test");
        BackendSelectionProvider policy = Mockito.mock(BackendSelectionProvider.class);
        Mockito.when(policy.partitionRequiredLoadCandidates(hint, candidates))
                .thenReturn(new BackendSelection.CandidateSelection<>(ImmutableList.of(), candidates));

        BackendSelectionManager.setProviderForTest(policy);

        UserException exception = Assertions.assertThrows(UserException.class,
                    () -> BackendSelectionManager.orderLoadCandidates(hint, candidates));
        Assertions.assertTrue(exception.getMessage().contains("No candidate satisfies required backend selection"));
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
        BackendSelectionProvider policy = Mockito.mock(BackendSelectionProvider.class);
        Mockito.when(policy.partitionRequiredLoadCandidates(hint, candidates))
                .thenReturn(invalidPartition.apply(candidates));

        BackendSelectionManager.setProviderForTest(policy);

        UserException exception = Assertions.assertThrows(UserException.class,
                    () -> BackendSelectionManager.orderLoadCandidates(hint, candidates));
        Assertions.assertTrue(exception.getMessage().contains("must partition all candidates"));
    }

    @Test
    void testRequiredQuerySelectionReturnsOnlyPreferredCandidates() throws Exception {
        Candidate first = new Candidate("first");
        Candidate preferred = new Candidate("preferred");
        List<Candidate> candidates = ImmutableList.of(first, preferred);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.REQUIRE, "test");
        BackendSelectionProvider policy = Mockito.mock(BackendSelectionProvider.class);
        Mockito.when(policy.partitionRequiredQueryCandidates(hint, candidates, CANDIDATE_TAG))
                .thenReturn(new BackendSelection.CandidateSelection<>(
                        ImmutableList.of(preferred), ImmutableList.of(first)));

        BackendSelectionManager.setProviderForTest(policy);

        Assertions.assertEquals(ImmutableList.of(preferred),
                    BackendSelectionManager.orderQueryCandidates(hint, candidates, CANDIDATE_TAG));
    }

    @Test
    void testPreferredQuerySelectionReturnsPreferredCandidatesWhenAvailable() throws Exception {
        Candidate first = new Candidate("first");
        Candidate preferred = new Candidate("preferred");
        List<Candidate> candidates = ImmutableList.of(first, preferred);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.PREFER, "test");
        BackendSelectionProvider policy = Mockito.mock(BackendSelectionProvider.class);
        Mockito.when(policy.hasQuerySelectionPreference(hint)).thenReturn(true);
        Mockito.when(policy.partitionPreferredQueryCandidates(hint, candidates, CANDIDATE_TAG))
                .thenReturn(new BackendSelection.CandidateSelection<>(
                        ImmutableList.of(preferred), ImmutableList.of(first)));

        BackendSelectionManager.setProviderForTest(policy);

        BackendSelection.CandidateSelection<Candidate> selection =
                    BackendSelectionManager.partitionPreferredQueryCandidates(hint, candidates, CANDIDATE_TAG);

        Assertions.assertEquals(ImmutableList.of(preferred), selection.getPreferredCandidates());
        Assertions.assertEquals(ImmutableList.of(first), selection.getFallbackCandidates());
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
        BackendSelectionProvider policy = Mockito.mock(BackendSelectionProvider.class);
        Mockito.when(policy.hasQuerySelectionPreference(hint)).thenReturn(true);
        Mockito.when(policy.orderQueryCandidates(
                        Mockito.eq(hint), Mockito.anyList(), Mockito.eq(CANDIDATE_TAG)))
                .thenAnswer(invocation -> {
                    List<Candidate> ordered = invocation.getArgument(1);
                    Collections.reverse(ordered);
                    return ordered;
                });

        BackendSelectionManager.setProviderForTest(policy);

        List<Candidate> ordered = BackendSelectionManager.orderQueryCandidatesWithinTies(
                    hint, candidates, Comparator.comparingInt(candidate -> candidate.priority), CANDIDATE_TAG);

        Assertions.assertEquals(ImmutableList.of(second, first, fourth, third), ordered);
        Assertions.assertNotSame(candidates, ordered);
        Assertions.assertEquals(ImmutableList.of(first, second, third, fourth), candidates);
        Mockito.verify(policy, Mockito.times(2)).orderQueryCandidates(
                    Mockito.eq(hint), Mockito.anyList(), Mockito.eq(CANDIDATE_TAG));
    }

    @Test
    void testOrderQueryCandidatesWithinTiesReturnsOriginalWhenProviderKeepsOrder() throws Exception {
        Candidate first = new Candidate("first", 10);
        Candidate second = new Candidate("second", 9);
        List<Candidate> candidates = ImmutableList.of(first, second);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.PREFER, "test");
        BackendSelectionProvider policy = Mockito.mock(BackendSelectionProvider.class);
        Mockito.when(policy.hasQuerySelectionPreference(hint)).thenReturn(true);
        Mockito.when(policy.orderQueryCandidates(
                        Mockito.eq(hint), Mockito.anyList(), Mockito.eq(CANDIDATE_TAG)))
                .thenAnswer(invocation -> invocation.getArgument(1));

        BackendSelectionManager.setProviderForTest(policy);

        Assertions.assertSame(candidates, BackendSelectionManager.orderQueryCandidatesWithinTies(
                    hint, candidates, Comparator.comparingInt(candidate -> candidate.priority), CANDIDATE_TAG));
    }

    @Test
    void testOrderQueryCandidatesWithinTiesRejectsCandidateOutsideCurrentTieGroup() throws Exception {
        Candidate first = new Candidate("first", 10);
        Candidate second = new Candidate("second", 10);
        Candidate outside = new Candidate("outside", 9);
        List<Candidate> candidates = ImmutableList.of(first, second, outside);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.PREFER, "test");
        BackendSelectionProvider policy = Mockito.mock(BackendSelectionProvider.class);
        Mockito.when(policy.hasQuerySelectionPreference(hint)).thenReturn(true);
        Mockito.when(policy.orderQueryCandidates(
                        Mockito.eq(hint), Mockito.anyList(), Mockito.eq(CANDIDATE_TAG)))
                .thenAnswer(invocation -> ImmutableList.of(outside, first));

        BackendSelectionManager.setProviderForTest(policy);

        UserException exception = Assertions.assertThrows(UserException.class,
                    () -> BackendSelectionManager.orderQueryCandidatesWithinTies(
                            hint, candidates, Comparator.comparingInt(candidate -> candidate.priority), CANDIDATE_TAG));
        Assertions.assertTrue(exception.getMessage().contains("orderQueryCandidatesWithinTies"));
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
        BackendSelectionProvider policy = Mockito.mock(BackendSelectionProvider.class);
        BackendSelection.CandidateSelection<Candidate> selection =
                new BackendSelection.CandidateSelection<>(ImmutableList.of(second), ImmutableList.of(first));
        Mockito.when(policy.partitionRequiredQueryCandidates(hint, candidates, CANDIDATE_TAG))
                .thenReturn(selection);

        BackendSelectionManager.setProviderForTest(policy);

        Assertions.assertEquals(ImmutableList.of(second),
                    BackendSelectionManager.orderQueryCandidatesWithinTies(
                            hint, candidates, Comparator.comparingInt(candidate -> candidate.priority), CANDIDATE_TAG));
        Mockito.verify(policy).partitionRequiredQueryCandidates(hint, candidates, CANDIDATE_TAG);
        Mockito.verify(policy, Mockito.never()).orderQueryCandidates(
                    Mockito.any(), Mockito.anyList(), Mockito.any());
    }

    @Test
    void testClassifyQuerySelectionUsesProviderOutcome() {
        Candidate candidate = new Candidate("candidate");
        List<Candidate> candidates = ImmutableList.of(candidate);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.PREFER, "test");
        BackendSelectionProvider policy = Mockito.mock(BackendSelectionProvider.class);
        Mockito.when(policy.hasQuerySelectionPreference(hint)).thenReturn(true);
        Mockito.when(policy.classifyQuerySelection(hint, candidates, CANDIDATE_TAG))
                .thenReturn(BackendSelection.QuerySelectionResult.FALLBACK_PREFERRED_UNAVAILABLE);

        BackendSelectionManager.setProviderForTest(policy);

        Assertions.assertEquals(BackendSelection.QuerySelectionResult.FALLBACK_PREFERRED_UNAVAILABLE,
                    BackendSelectionManager.classifyQuerySelection(hint, candidates, CANDIDATE_TAG));
    }

    @Test
    void testClassifyRequiredQuerySelectionAsPreferredHit() {
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.REQUIRE, "test");

        Assertions.assertEquals(BackendSelection.QuerySelectionResult.PREFERRED_HIT,
                BackendSelectionManager.classifyQuerySelection(
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
        BackendSelectionProvider policy = Mockito.mock(BackendSelectionProvider.class);
        Mockito.when(policy.partitionRequiredLoadCandidates(hint, candidates))
                .thenReturn(new BackendSelection.CandidateSelection<>(candidates, ImmutableList.of()));

        BackendSelectionManager.setProviderForTest(policy);

        UserException exception = Assertions.assertThrows(UserException.class,
                    () -> BackendSelectionManager.chooseFirstPreferredLoadBackend(
                            context, candidates, Backend::isLoadAvailable));
        Assertions.assertTrue(exception.getMessage().contains("No available candidate satisfies required"));
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
        BackendSelectionProvider policy = new BackendSelectionProvider() {
            @Override
            public List<Replica> orderRepairSourceCandidates(List<Replica> candidates, long destBackendId) {
                return ImmutableList.of(first, first);
            }
        };
        BackendSelectionManager.setProviderForTest(policy);
        UserException exception = Assertions.assertThrows(UserException.class,
                    () -> BackendSelectionManager.orderRepairSourceCandidates(ImmutableList.of(first, second), 3L));
        Assertions.assertTrue(exception.getMessage().contains("orderRepairSourceCandidates"));
    }

    private void assertInvalidQueryOrder(Function<List<Candidate>, List<Candidate>> invalidOrder) throws Exception {
        Candidate first = new Candidate("same");
        Candidate second = new Candidate("same");
        List<Candidate> candidates = ImmutableList.of(first, second);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.PREFER, "test");
        BackendSelectionProvider policy = Mockito.mock(BackendSelectionProvider.class);
        Mockito.when(policy.hasQuerySelectionPreference(hint)).thenReturn(true);
        Mockito.when(policy.orderQueryCandidates(hint, candidates, CANDIDATE_TAG))
                .thenReturn(invalidOrder.apply(candidates));
        BackendSelectionManager.setProviderForTest(policy);
        UserException exception = Assertions.assertThrows(UserException.class,
                    () -> BackendSelectionManager.orderQueryCandidates(hint, candidates, CANDIDATE_TAG));
        Assertions.assertTrue(exception.getMessage().contains("orderQueryCandidates"));
    }

    private void assertInvalidTieOrder(Function<List<Candidate>, List<Candidate>> invalidOrder) throws Exception {
        Candidate first = new Candidate("first", 10);
        Candidate second = new Candidate("second", 10);
        List<Candidate> candidates = ImmutableList.of(first, second);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.PREFER, "test");
        BackendSelectionProvider policy = Mockito.mock(BackendSelectionProvider.class);
        Mockito.when(policy.hasQuerySelectionPreference(hint)).thenReturn(true);
        Mockito.when(policy.orderQueryCandidates(
                        Mockito.eq(hint), Mockito.anyList(), Mockito.eq(CANDIDATE_TAG)))
                .thenAnswer(invocation -> invalidOrder.apply(invocation.getArgument(1)));
        BackendSelectionManager.setProviderForTest(policy);
        UserException exception = Assertions.assertThrows(UserException.class,
                    () -> BackendSelectionManager.orderQueryCandidatesWithinTies(
                            hint, candidates, Comparator.comparingInt(candidate -> candidate.priority), CANDIDATE_TAG));
        Assertions.assertTrue(exception.getMessage().contains("orderQueryCandidatesWithinTies"));
    }

    private static final class BackendSelectionSpiClassLoader extends ClassLoader {
        private static final String SPI_FILE =
                "META-INF/services/" + BackendSelectionProvider.class.getName();
        private final String[] providerClassNames;

        private BackendSelectionSpiClassLoader(String... providerClassNames) {
            super(BackendSelectionManagerTest.class.getClassLoader());
            this.providerClassNames = providerClassNames;
        }

        @Override
        public java.util.Enumeration<URL> getResources(String name) throws IOException {
            if (!SPI_FILE.equals(name)) {
                return super.getResources(name);
            }
            String content = String.join("\n", providerClassNames) + "\n";
            URLStreamHandler handler = new URLStreamHandler() {
                @Override
                protected URLConnection openConnection(URL url) {
                    return new URLConnection(url) {
                        @Override
                        public void connect() {
                        }

                        @Override
                        public InputStream getInputStream() {
                            return new ByteArrayInputStream(content.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                        }
                    };
                }
            };
            URL resource = new URL("synthetic", "", -1, SPI_FILE, handler);
            return java.util.Collections.enumeration(java.util.Collections.singletonList(resource));
        }
    }

    public static class FirstBackendSelectionProvider implements BackendSelectionProvider {
    }

    public static class SecondBackendSelectionProvider implements BackendSelectionProvider {
    }

    private static final class EmptyBackendSelectionSpiClassLoader extends ClassLoader {
        private static final String SPI_FILE =
                "META-INF/services/" + BackendSelectionProvider.class.getName();

        private EmptyBackendSelectionSpiClassLoader() {
            super(BackendSelectionManagerTest.class.getClassLoader());
        }

        @Override
        public java.util.Enumeration<java.net.URL> getResources(String name) throws java.io.IOException {
            if (SPI_FILE.equals(name)) {
                return java.util.Collections.emptyEnumeration();
            }
            return super.getResources(name);
        }
    }

    private static final class SingleBackendSelectionSpiClassLoader extends ClassLoader {
        private static final String SPI_FILE =
                "META-INF/services/" + BackendSelectionProvider.class.getName();
        private final String providerClassName;

        private SingleBackendSelectionSpiClassLoader(String providerClassName) {
            super(BackendSelectionManagerTest.class.getClassLoader());
            this.providerClassName = providerClassName;
        }

        @Override
        public java.util.Enumeration<java.net.URL> getResources(String name) throws java.io.IOException {
            if (!SPI_FILE.equals(name)) {
                return super.getResources(name);
            }
            String content = providerClassName + "\n";
            java.net.URLStreamHandler handler = new java.net.URLStreamHandler() {
                @Override
                protected java.net.URLConnection openConnection(java.net.URL url) {
                    return new java.net.URLConnection(url) {
                        @Override
                        public void connect() {
                        }

                        @Override
                        public java.io.InputStream getInputStream() {
                            return new java.io.ByteArrayInputStream(
                                    content.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                        }
                    };
                }
            };
            java.net.URL resource = new java.net.URL("synthetic", "", -1, SPI_FILE, handler);
            return java.util.Collections.enumeration(java.util.Collections.singletonList(resource));
        }
    }

    public static class FailingBackendSelectionProvider implements BackendSelectionProvider {
        public FailingBackendSelectionProvider() {
            throw new IllegalStateException("provider construction failed");
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
