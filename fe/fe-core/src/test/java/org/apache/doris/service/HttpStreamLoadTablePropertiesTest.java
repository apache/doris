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

package org.apache.doris.service;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.qe.HttpStreamParams;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TOlapTableSink;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPlanFragment;
import org.apache.doris.thrift.TStreamLoadPutResult;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.TimeUnit;

public class HttpStreamLoadTablePropertiesTest {
    @Test
    public void testHttpLoadUsesPlannedSchemaVersion() {
        verifyPlannedMetadata(23);
    }

    @Test
    public void testGroupCommitUsesPlannedSchemaVersion() {
        verifyPlannedMetadata(-1);
    }

    private void verifyPlannedMetadata(long txnId) {
        OlapTable table = Mockito.spy(new OlapTable());
        // The current table has no base-index metadata. The response must use the
        // completed plan, even if schema change has since replaced the index.
        table.setBaseIndexId(9L);
        Mockito.doReturn(false).when(table).getEnableUniqueKeyMergeOnWrite();
        Mockito.doReturn(false).when(table).enableTso();
        Mockito.doReturn(1000).when(table).getGroupCommitIntervalMs();
        Mockito.doReturn(1024).when(table).getGroupCommitDataBytes();
        TOlapTableSink sink = new TOlapTableSink();
        sink.setTableId(41);
        sink.setBaseSchemaVersion(7);
        TPipelineFragmentParams plan = new TPipelineFragmentParams();
        plan.setFragment(new TPlanFragment().setOutputSink(new TDataSink().setOlapTableSink(sink)));
        HttpStreamParams params = new HttpStreamParams();
        params.setTable(table);
        params.setTxnId(txnId);
        params.setParams(plan);
        TStreamLoadPutResult result = new TStreamLoadPutResult();
        result.setPipelineParams(plan);
        FrontendServiceImpl service = Mockito.mock(FrontendServiceImpl.class);

        Deencapsulation.invoke(service, "setHttpStreamLoadTableProperties", params, result);

        Assertions.assertEquals(41, result.getTableId());
        Assertions.assertEquals(7, result.getBaseSchemaVersion());
        Assertions.assertEquals(1000, result.getGroupCommitIntervalMs());
        Assertions.assertEquals(1024, result.getGroupCommitDataBytes());
        Mockito.verify(table, Mockito.never()).getBaseSchemaVersion();
        Mockito.verify(table, Mockito.never()).tryReadLock(Mockito.anyLong(), Mockito.any(TimeUnit.class));
        Mockito.verify(table, Mockito.never()).readUnlock();
    }
}
