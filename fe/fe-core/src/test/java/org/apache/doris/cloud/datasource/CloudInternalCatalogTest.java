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

package org.apache.doris.cloud.datasource;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.DdlException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

public class CloudInternalCatalogTest {
    @Test
    public void testCreateTableConflictDropsOnlyLosingIndexes() throws Exception {
        testCreateTableConflictCleanup(Cloud.MetaServiceCode.OK);
    }

    @Test
    public void testCreateTableConflictReportsCleanupFailure() throws Exception {
        testCreateTableConflictCleanup(Cloud.MetaServiceCode.INVALID_ARGUMENT);
    }

    private void testCreateTableConflictCleanup(Cloud.MetaServiceCode code) throws Exception {
        OlapTable loser = Mockito.mock(OlapTable.class);
        List<Long> indexIds = Arrays.asList(31L, 32L);
        Mockito.when(loser.getId()).thenReturn(21L);
        Mockito.when(loser.getIndexIdList()).thenReturn(indexIds);
        MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
        Mockito.when(proxy.dropIndex(Mockito.any())).thenReturn(Cloud.IndexResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(code).setMsg("cleanup failed"))
                .build());
        try (MockedStatic<MetaServiceProxy> mockedProxy = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedProxy.when(MetaServiceProxy::getInstance).thenReturn(proxy);
            CloudInternalCatalog catalog = new CloudInternalCatalog();
            if (code == Cloud.MetaServiceCode.OK) {
                catalog.onCreateTableConflict(11L, loser);
            } else {
                DdlException error = Assertions.assertThrows(DdlException.class,
                        () -> catalog.onCreateTableConflict(11L, loser));
                Assertions.assertTrue(error.getMessage().contains("cleanup failed"));
            }
            ArgumentCaptor<Cloud.IndexRequest> request = ArgumentCaptor.forClass(Cloud.IndexRequest.class);
            Mockito.verify(proxy).dropIndex(request.capture());
            Assertions.assertEquals(11L, request.getValue().getDbId());
            Assertions.assertEquals(21L, request.getValue().getTableId());
            Assertions.assertEquals(indexIds, request.getValue().getIndexIdsList());
        }
    }

}
