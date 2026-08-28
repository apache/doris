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
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.exploration.mv.InitMaterializationContextHook;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Optional;

public class PreloadExternalMetadataTest {

    @Test
    public void cloudMtmvVersionsArePreloadedEvenWhenExternalPreloadIsDisabled() throws AnalysisException {
        String originalCloudUniqueId = Config.cloud_unique_id;
        Config.cloud_unique_id = "test_cloud";
        try {
            StatementContext statementContext = Mockito.mock(StatementContext.class);
            MTMV mtmv = Mockito.mock(MTMV.class);
            MTMVPartitionInfo partitionInfo = Mockito.mock(MTMVPartitionInfo.class);
            MTMVRelatedTableIf pctTable = Mockito.mock(MTMVRelatedTableIf.class);
            MTMVRefreshContext refreshContext = Mockito.mock(MTMVRefreshContext.class);
            prepareEligibleMtmv(statementContext, mtmv);
            Mockito.when(statementContext.getCandidateMTMVs()).thenReturn(Collections.singleton(mtmv));
            Mockito.when(statementContext.getPreloadedMtmvRefreshContext(mtmv)).thenReturn(Optional.empty());
            Mockito.when(mtmv.getMvPartitionInfo()).thenReturn(partitionInfo);
            Mockito.when(partitionInfo.getPctTables()).thenReturn(Collections.singleton(pctTable));

            try (MockedStatic<MTMVRefreshContext> refreshContextStatic =
                    Mockito.mockStatic(MTMVRefreshContext.class)) {
                refreshContextStatic.when(() -> MTMVRefreshContext.buildContext(mtmv))
                        .thenAnswer(invocation -> {
                            Mockito.verify(statementContext).loadSnapshots(
                                    pctTable, Optional.empty(), Optional.empty());
                            return refreshContext;
                        });

                new PreloadExternalMetadata().executePreload(statementContext);

                refreshContextStatic.verify(() -> MTMVRefreshContext.buildContext(mtmv), Mockito.times(1));
                Mockito.verify(statementContext).putPreloadedMtmvRefreshContext(mtmv, refreshContext);
            }
        } finally {
            Config.cloud_unique_id = originalCloudUniqueId;
        }
    }

    @Test
    public void cloudMtmvConnectorFailureSkipsOnlyThatCandidate() throws AnalysisException {
        String originalCloudUniqueId = Config.cloud_unique_id;
        Config.cloud_unique_id = "test_cloud";
        try {
            StatementContext statementContext = Mockito.mock(StatementContext.class);
            MTMV mtmv = Mockito.mock(MTMV.class);
            MTMVPartitionInfo partitionInfo = Mockito.mock(MTMVPartitionInfo.class);
            prepareEligibleMtmv(statementContext, mtmv);
            Mockito.when(statementContext.getCandidateMTMVs()).thenReturn(Collections.singleton(mtmv));
            Mockito.when(statementContext.getPreloadedMtmvRefreshContext(mtmv)).thenReturn(Optional.empty());
            Mockito.when(mtmv.getMvPartitionInfo()).thenReturn(partitionInfo);
            Mockito.when(partitionInfo.getPctTables()).thenReturn(Collections.emptySet());

            try (MockedStatic<MTMVRefreshContext> refreshContextStatic =
                    Mockito.mockStatic(MTMVRefreshContext.class)) {
                refreshContextStatic.when(() -> MTMVRefreshContext.buildContext(mtmv))
                        .thenThrow(new DorisConnectorException("metastore unavailable"));

                new PreloadExternalMetadata().executePreload(statementContext);

                Mockito.verify(statementContext, Mockito.never())
                        .putPreloadedMtmvRefreshContext(Mockito.any(), Mockito.any());
            }
        } finally {
            Config.cloud_unique_id = originalCloudUniqueId;
        }
    }

    @Test
    public void cloudMtmvPreloadSkipsCandidateDisabledForRewrite() {
        String originalCloudUniqueId = Config.cloud_unique_id;
        Config.cloud_unique_id = "test_cloud";
        try {
            StatementContext statementContext = Mockito.mock(StatementContext.class);
            MTMV mtmv = Mockito.mock(MTMV.class);
            Mockito.when(statementContext.getConnectContext()).thenReturn(Mockito.mock(ConnectContext.class));
            Mockito.when(statementContext.getPlannerHooks()).thenReturn(
                    Collections.singleton(rewriteHook()));
            Mockito.when(statementContext.getCandidateMTMVs()).thenReturn(Collections.singleton(mtmv));

            new PreloadExternalMetadata().executePreload(statementContext);

            Mockito.verify(mtmv, Mockito.never()).getMvPartitionInfo();
            Mockito.verify(statementContext, Mockito.never())
                    .putPreloadedMtmvRefreshContext(Mockito.any(), Mockito.any());
        } finally {
            Config.cloud_unique_id = originalCloudUniqueId;
        }
    }

    @Test
    public void cloudMtmvPreloadSkipsPartitionsInsideGracePeriod() {
        String originalCloudUniqueId = Config.cloud_unique_id;
        Config.cloud_unique_id = "test_cloud";
        try {
            StatementContext statementContext = Mockito.mock(StatementContext.class);
            MTMV mtmv = Mockito.mock(MTMV.class);
            Partition partition = Mockito.mock(Partition.class);
            Mockito.when(statementContext.getConnectContext()).thenReturn(Mockito.mock(ConnectContext.class));
            Mockito.when(statementContext.getPlannerHooks()).thenReturn(
                    Collections.singleton(rewriteHook()));
            Mockito.when(statementContext.getCandidateMTMVs()).thenReturn(Collections.singleton(mtmv));
            Mockito.when(mtmv.isUseForRewrite()).thenReturn(true);
            Mockito.when(mtmv.canBeCandidate()).thenReturn(true);
            Mockito.when(mtmv.getRelation()).thenReturn(Mockito.mock(MTMVRelation.class));
            Mockito.when(mtmv.getGracePeriod()).thenReturn(60_000L);
            Mockito.when(mtmv.getPartitions()).thenReturn(Collections.singleton(partition));
            Mockito.when(partition.getVisibleVersionTime()).thenReturn(System.currentTimeMillis());

            new PreloadExternalMetadata().executePreload(statementContext);

            Mockito.verify(mtmv, Mockito.never()).getMvPartitionInfo();
            Mockito.verify(statementContext, Mockito.never())
                    .putPreloadedMtmvRefreshContext(Mockito.any(), Mockito.any());
        } finally {
            Config.cloud_unique_id = originalCloudUniqueId;
        }
    }

    private static void prepareEligibleMtmv(StatementContext statementContext, MTMV mtmv) {
        Partition partition = Mockito.mock(Partition.class);
        Mockito.when(statementContext.getConnectContext()).thenReturn(Mockito.mock(ConnectContext.class));
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
