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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.hint.UseMvHint;
import org.apache.doris.nereids.rules.exploration.mv.InitMaterializationContextHook;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Optional;

public class PreloadExternalMetadataTest {
    private String cloudUniqueId;

    @BeforeEach
    public void enableCloudMode() {
        cloudUniqueId = Config.cloud_unique_id;
        Config.cloud_unique_id = "test_cloud";
    }

    @AfterEach
    public void restoreCloudMode() {
        Config.cloud_unique_id = cloudUniqueId;
    }

    @Test
    public void cloudMtmvCandidateFailuresAreIsolatedButErrorsFailLoud() throws AnalysisException {
        StatementContext statementContext = Mockito.mock(StatementContext.class);
        MTMV firstPassBad = Mockito.mock(MTMV.class);
        MTMV secondPassBad = Mockito.mock(MTMV.class);
        MTMV healthy = Mockito.mock(MTMV.class);
        MTMVPartitionInfo firstInfo = Mockito.mock(MTMVPartitionInfo.class);
        MTMVPartitionInfo secondInfo = Mockito.mock(MTMVPartitionInfo.class);
        MTMVPartitionInfo healthyInfo = Mockito.mock(MTMVPartitionInfo.class);
        MTMVRelatedTableIf secondPassPct = Mockito.mock(MTMVRelatedTableIf.class);
        MTMVRelatedTableIf healthyPct = Mockito.mock(MTMVRelatedTableIf.class);
        for (MTMV mtmv : Arrays.asList(firstPassBad, secondPassBad, healthy)) {
            prepareEligibleMtmv(statementContext, mtmv);
        }
        Mockito.when(statementContext.getCandidateMTMVs()).thenReturn(
                new LinkedHashSet<>(Arrays.asList(firstPassBad, secondPassBad, healthy)));
        Mockito.when(statementContext.getPreloadedMtmvRefreshContext(Mockito.any())).thenReturn(Optional.empty());
        Mockito.when(firstPassBad.getMvPartitionInfo()).thenReturn(firstInfo);
        Mockito.when(secondPassBad.getMvPartitionInfo()).thenReturn(secondInfo);
        Mockito.when(healthy.getMvPartitionInfo()).thenReturn(healthyInfo);
        Mockito.when(firstInfo.getPctTables()).thenThrow(new RuntimeException("first-pass failure"));
        Mockito.when(secondInfo.getPctTables()).thenReturn(Collections.singleton(secondPassPct));
        Mockito.when(healthyInfo.getPctTables()).thenReturn(Collections.singleton(healthyPct));
        Mockito.doThrow(new RuntimeException("second-pass failure")).when(statementContext)
                .loadSnapshots(secondPassPct, Optional.empty(), Optional.empty());
        MTMVRefreshContext seed = Mockito.mock(MTMVRefreshContext.class);
        try (MockedStatic<MTMVRefreshContext> contexts = Mockito.mockStatic(MTMVRefreshContext.class)) {
            contexts.when(() -> MTMVRefreshContext.buildCloudPreloadSeed(
                    healthy, Collections.singleton(healthyPct))).thenAnswer(invocation -> {
                        Mockito.verify(statementContext).loadSnapshots(
                                healthyPct, Optional.empty(), Optional.empty());
                        return seed;
                    });
            new PreloadExternalMetadata().executePreload(statementContext);
            Mockito.verify(statementContext).putPreloadedMtmvRefreshContext(healthy, seed);
            Mockito.verify(statementContext, Mockito.never())
                    .putPreloadedMtmvRefreshContext(Mockito.argThat(mtmv -> mtmv != healthy), Mockito.any());
            AssertionError fatal = new AssertionError("fatal");
            contexts.when(() -> MTMVRefreshContext.buildCloudPreloadSeed(
                    healthy, Collections.singleton(healthyPct)))
                    .thenThrow(fatal);
            Assertions.assertThrows(AssertionError.class,
                    () -> new PreloadExternalMetadata().executePreload(statementContext));
        }
    }

    @Test
    public void cloudMtmvPreloadSkipsDisabledAndGracePeriodCandidates() {
        StatementContext statementContext = Mockito.mock(StatementContext.class);
        MTMV disabled = Mockito.mock(MTMV.class);
        MTMV withinGracePeriod = Mockito.mock(MTMV.class);
        Partition partition = Mockito.mock(Partition.class);
        Mockito.when(statementContext.getConnectContext()).thenReturn(Mockito.mock(ConnectContext.class));
        Mockito.when(statementContext.getPlannerHooks()).thenReturn(Collections.singleton(rewriteHook()));
        Mockito.when(statementContext.getCandidateMTMVs()).thenReturn(
                new LinkedHashSet<>(Arrays.asList(disabled, withinGracePeriod)));
        Mockito.when(statementContext.getMtmvRewriteEpochMillis()).thenReturn(100L);
        Mockito.when(withinGracePeriod.isUseForRewrite()).thenReturn(true);
        Mockito.when(withinGracePeriod.canBeCandidate()).thenReturn(true);
        Mockito.when(withinGracePeriod.getRelation()).thenReturn(Mockito.mock(MTMVRelation.class));
        Mockito.when(withinGracePeriod.getGracePeriod()).thenReturn(60_000L);
        Mockito.when(withinGracePeriod.getPartitions()).thenReturn(Collections.singleton(partition));
        Mockito.when(partition.getVisibleVersionTime()).thenReturn(100L);
        new PreloadExternalMetadata().executePreload(statementContext);
        Mockito.verify(disabled, Mockito.never()).getMvPartitionInfo();
        Mockito.verify(withinGracePeriod, Mockito.never()).getMvPartitionInfo();
        Mockito.verify(statementContext, Mockito.never())
                .putPreloadedMtmvRefreshContext(Mockito.any(), Mockito.any());
    }

    @Test
    public void excludedMtmvDoesNotConsumeCloudPreloadBudget() throws AnalysisException {
        StatementContext statementContext = Mockito.mock(StatementContext.class);
        MTMV excluded = Mockito.mock(MTMV.class);
        MTMV selected = Mockito.mock(MTMV.class);
        MTMVPartitionInfo excludedInfo = Mockito.mock(MTMVPartitionInfo.class);
        MTMVPartitionInfo selectedInfo = Mockito.mock(MTMVPartitionInfo.class);
        MTMVRelatedTableIf firstPct = Mockito.mock(MTMVRelatedTableIf.class,
                Mockito.withSettings().extraInterfaces(MvccTable.class));
        MTMVRelatedTableIf secondPct = Mockito.mock(MTMVRelatedTableIf.class,
                Mockito.withSettings().extraInterfaces(MvccTable.class));
        MTMVRelatedTableIf selectedPct = Mockito.mock(MTMVRelatedTableIf.class,
                Mockito.withSettings().extraInterfaces(MvccTable.class));
        prepareEligibleMtmv(statementContext, excluded);
        prepareEligibleMtmv(statementContext, selected);
        Mockito.when(statementContext.getCandidateMTMVs()).thenReturn(
                new LinkedHashSet<>(Arrays.asList(excluded, selected)));
        Mockito.when(statementContext.getPreloadedMtmvRefreshContext(Mockito.any())).thenReturn(Optional.empty());
        Mockito.when(excluded.getMvPartitionInfo()).thenReturn(excludedInfo);
        Mockito.when(selected.getMvPartitionInfo()).thenReturn(selectedInfo);
        Mockito.when(excludedInfo.getPctTables()).thenReturn(ImmutableSet.of(firstPct, secondPct));
        Mockito.when(selectedInfo.getPctTables()).thenReturn(Collections.singleton(selectedPct));
        Mockito.when(excluded.getFullQualifiers()).thenReturn(Arrays.asList("internal", "db", "excluded"));
        Mockito.when(selected.getFullQualifiers()).thenReturn(Arrays.asList("internal", "db", "selected"));
        UseMvHint useMvHint = Mockito.mock(UseMvHint.class);
        Mockito.when(useMvHint.getHintName()).thenReturn("USE_MV");
        Mockito.when(useMvHint.getUseMvTableColumnMap()).thenReturn(Collections.singletonMap(
                Arrays.asList("internal", "db", "selected"), false));
        Mockito.when(statementContext.getHints()).thenReturn(Collections.singletonList(useMvHint));
        Mockito.when(statementContext.getConnectContext().getSessionVariable()
                .getMaterializedViewRewriteCloudPreloadSnapshotNum()).thenReturn(1);
        MTMVRefreshContext seed = Mockito.mock(MTMVRefreshContext.class);
        try (MockedStatic<MTMVRefreshContext> contexts = Mockito.mockStatic(MTMVRefreshContext.class)) {
            contexts.when(() -> MTMVRefreshContext.buildCloudPreloadSeed(
                    selected, Collections.singleton(selectedPct))).thenReturn(seed);
            new PreloadExternalMetadata().executePreload(statementContext);
            Mockito.verify(excluded, Mockito.never()).getMvPartitionInfo();
            Mockito.verify(statementContext).loadSnapshots(selectedPct, Optional.empty(), Optional.empty());
            Mockito.verify(statementContext).putPreloadedMtmvRefreshContext(selected, seed);
        }
    }

    private static void prepareEligibleMtmv(StatementContext statementContext, MTMV mtmv) {
        Partition partition = Mockito.mock(Partition.class);
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        SessionVariable sessionVariable = Mockito.mock(SessionVariable.class);
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(sessionVariable.getMaterializedViewRewriteCloudPreloadSnapshotNum()).thenReturn(8);
        Mockito.when(statementContext.getConnectContext()).thenReturn(connectContext);
        Mockito.when(statementContext.getPlannerHooks()).thenReturn(Collections.singleton(rewriteHook()));
        Mockito.when(mtmv.isUseForRewrite()).thenReturn(true);
        Mockito.when(mtmv.canBeCandidate()).thenReturn(true);
        Mockito.when(mtmv.getRelation()).thenReturn(Mockito.mock(MTMVRelation.class));
        Mockito.when(mtmv.getPartitions()).thenReturn(Collections.singleton(partition));
    }

    private static InitMaterializationContextHook rewriteHook() {
        return new InitMaterializationContextHook() {
            @Override
            protected boolean rejectMtmvCandidate(ConnectContext connectContext, MTMV mtmv) {
                return false;
            }
        };
    }
}
