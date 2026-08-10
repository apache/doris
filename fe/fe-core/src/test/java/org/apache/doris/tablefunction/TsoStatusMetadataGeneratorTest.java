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

package org.apache.doris.tablefunction;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.thrift.TFetchSchemaTableDataRequest;
import org.apache.doris.thrift.TFetchSchemaTableDataResult;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TSchemaTableName;
import org.apache.doris.thrift.TSchemaTableRequestParams;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.tso.TSOService;
import org.apache.doris.tso.TSOTimestamp;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TsoStatusMetadataGeneratorTest {
    private boolean originalEnableFeatureBinlog;
    private Env env;
    private TSOService tsoService;
    private MockedStatic<Env> mockedEnv;

    @BeforeEach
    public void setUp() {
        originalEnableFeatureBinlog = Config.enable_feature_binlog;
        Config.enable_feature_binlog = true;

        env = Mockito.mock(Env.class);
        tsoService = Mockito.mock(TSOService.class);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getTSOService()).thenReturn(tsoService);
    }

    @AfterEach
    public void tearDown() {
        mockedEnv.close();
        Config.enable_feature_binlog = originalEnableFeatureBinlog;
    }

    @Test
    public void testTsoStatusResult() throws Exception {
        long physicalTime = 1_725_000_000_000L;
        long logicalCounter = 17L;
        long currentTso = TSOTimestamp.composeTimestamp(physicalTime, logicalCounter);
        long windowEndPhysicalTime = physicalTime + 5_000L;
        Mockito.when(tsoService.getStatusSnapshot()).thenReturn(
                new TSOService.TSOStatusSnapshot(true, currentTso, windowEndPhysicalTime));

        TFetchSchemaTableDataResult result = MetadataGenerator.getSchemaTableData(newRequest());

        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
        Assertions.assertEquals(1, result.getDataBatchSize());
        TRow row = result.getDataBatch().get(0);
        Assertions.assertEquals(windowEndPhysicalTime, row.getColumnValue().get(0).getLongVal());
        Assertions.assertEquals(currentTso, row.getColumnValue().get(1).getLongVal());
        Assertions.assertEquals(physicalTime, row.getColumnValue().get(2).getLongVal());
        Assertions.assertEquals(logicalCounter, row.getColumnValue().get(3).getLongVal());
        Mockito.verify(tsoService).getStatusSnapshot();
        Mockito.verify(tsoService, Mockito.never()).getTSO();
    }

    @Test
    public void testColumnFiltering() throws Exception {
        long physicalTime = 1_725_000_000_000L;
        long currentTso = TSOTimestamp.composeTimestamp(physicalTime, 17L);
        Mockito.when(tsoService.getStatusSnapshot()).thenReturn(
                new TSOService.TSOStatusSnapshot(true, currentTso, physicalTime + 5_000L));
        TFetchSchemaTableDataRequest request = newRequest();
        request.getSchemaTableParams().setColumnsName(
                ImmutableList.of("current_tso_physical_time", "current_tso"));

        TFetchSchemaTableDataResult result = MetadataGenerator.getSchemaTableData(request);

        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
        Assertions.assertEquals(2, result.getDataBatch().get(0).getColumnValueSize());
        Assertions.assertEquals(physicalTime, result.getDataBatch().get(0).getColumnValue().get(0).getLongVal());
        Assertions.assertEquals(currentTso, result.getDataBatch().get(0).getColumnValue().get(1).getLongVal());
    }

    @Test
    public void testDisabled() throws Exception {
        Config.enable_feature_binlog = false;

        TFetchSchemaTableDataResult result = MetadataGenerator.getSchemaTableData(newRequest());

        Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, result.getStatus().getStatusCode());
        Assertions.assertTrue(result.getStatus().getErrorMsgs().get(0).contains("enable_feature_binlog"));
        Mockito.verifyNoInteractions(tsoService);
    }

    @Test
    public void testNotCalibrated() throws Exception {
        Mockito.when(tsoService.getStatusSnapshot()).thenReturn(
                new TSOService.TSOStatusSnapshot(false, 0L, 0L));

        TFetchSchemaTableDataResult result = MetadataGenerator.getSchemaTableData(newRequest());

        Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, result.getStatus().getStatusCode());
        Assertions.assertTrue(result.getStatus().getErrorMsgs().get(0).contains("not calibrated"));
        Mockito.verify(tsoService).getStatusSnapshot();
    }

    private TFetchSchemaTableDataRequest newRequest() {
        TFetchSchemaTableDataRequest request = new TFetchSchemaTableDataRequest();
        request.setSchemaTableName(TSchemaTableName.TSO_STATUS);
        request.setSchemaTableParams(new TSchemaTableRequestParams());
        return request;
    }
}
