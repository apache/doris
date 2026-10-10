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
import org.apache.doris.common.lock.MonitoredReentrantReadWriteLock;
import org.apache.doris.qe.HttpStreamParams;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TStreamLoadPutResult;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class HttpStreamLoadTablePropertiesTest {
    @Test
    public void testSchemaVersionIsReadUnderTableLock() {
        OlapTable table = Mockito.spy(new OlapTable());
        Mockito.doReturn(false).when(table).getEnableUniqueKeyMergeOnWrite();
        Mockito.doReturn(false).when(table).enableTso();
        Mockito.doReturn(1000).when(table).getGroupCommitIntervalMs();
        Mockito.doReturn(1024).when(table).getGroupCommitDataBytes();
        Mockito.doAnswer(invocation -> {
            Assertions.assertEquals(1, readHoldCount(table));
            return 7;
        }).when(table).getBaseSchemaVersion();
        HttpStreamParams params = new HttpStreamParams();
        params.setTable(table);
        TStreamLoadPutResult result = new TStreamLoadPutResult();
        result.setPipelineParams(new TPipelineFragmentParams());
        FrontendServiceImpl service = Mockito.mock(FrontendServiceImpl.class);

        Deencapsulation.invoke(service, "setHttpStreamLoadTableProperties",
                new TStreamLoadPutRequest(), params, result);

        Assertions.assertEquals(7, result.getBaseSchemaVersion());
        Assertions.assertEquals(0, readHoldCount(table));
    }

    @Test
    public void testSchemaReadFailureReleasesTableLock() {
        OlapTable table = Mockito.spy(new OlapTable());
        Mockito.doReturn(false).when(table).getEnableUniqueKeyMergeOnWrite();
        Mockito.doReturn(false).when(table).enableTso();
        Mockito.doThrow(new IllegalStateException("schema read failed")).when(table).getBaseSchemaVersion();
        HttpStreamParams params = new HttpStreamParams();
        params.setTable(table);
        TStreamLoadPutResult result = new TStreamLoadPutResult();
        result.setPipelineParams(new TPipelineFragmentParams());
        FrontendServiceImpl service = Mockito.mock(FrontendServiceImpl.class);

        Assertions.assertThrows(IllegalStateException.class,
                () -> Deencapsulation.invoke(service, "setHttpStreamLoadTableProperties",
                        new TStreamLoadPutRequest(), params, result));
        Assertions.assertEquals(0, readHoldCount(table));
    }

    private int readHoldCount(OlapTable table) {
        MonitoredReentrantReadWriteLock lock = Deencapsulation.getField(table, "rwLock");
        return lock.getReadHoldCount();
    }
}
