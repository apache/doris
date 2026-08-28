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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.nereids.StatementContext;

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
            MTMVRefreshContext refreshContext = Mockito.mock(MTMVRefreshContext.class);
            Mockito.when(statementContext.getCandidateMTMVs()).thenReturn(Collections.singleton(mtmv));
            Mockito.when(statementContext.getPreloadedMtmvRefreshContext(mtmv)).thenReturn(Optional.empty());

            try (MockedStatic<MTMVRefreshContext> refreshContextStatic =
                    Mockito.mockStatic(MTMVRefreshContext.class)) {
                refreshContextStatic.when(() -> MTMVRefreshContext.buildContext(mtmv))
                        .thenReturn(refreshContext);

                new PreloadExternalMetadata().executePreload(statementContext);

                refreshContextStatic.verify(() -> MTMVRefreshContext.buildContext(mtmv), Mockito.times(1));
                Mockito.verify(statementContext).putPreloadedMtmvRefreshContext(mtmv, refreshContext);
            }
        } finally {
            Config.cloud_unique_id = originalCloudUniqueId;
        }
    }
}
