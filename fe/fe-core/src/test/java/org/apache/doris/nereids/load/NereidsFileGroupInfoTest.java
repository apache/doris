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

package org.apache.doris.nereids.load;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.datasource.property.fileformat.CsvFileFormatProperties;
import org.apache.doris.datasource.scan.FederationBackendPolicy;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

class NereidsFileGroupInfoTest {
    @Test
    void loadRangesCarryLiteralNullPathValueAsNull() throws Exception {
        // Splittable and unsplittable files build their ranges in different methods.
        for (boolean splittable : new boolean[] {false, true}) {
            CsvFileFormatProperties format = new CsvFileFormatProperties("csv");
            format.analyzeFileFormatProperties(new HashMap<>(), true);
            NereidsBrokerFileGroup fileGroup = new NereidsBrokerFileGroup(1L, false, null,
                    Lists.newArrayList("s3://bucket/table/*/data.csv"), null, null, Lists.newArrayList("p"),
                    Lists.newArrayList(), null, null, null, null, LoadTask.MergeType.APPEND, null, -1L,
                    false, false, format);
            BrokerDesc brokerDesc = Mockito.mock(BrokerDesc.class);
            Mockito.when(brokerDesc.getFileType()).thenReturn(TFileType.FILE_S3);
            Mockito.when(brokerDesc.getStorageType()).thenReturn(StorageBackend.StorageType.S3);
            FederationBackendPolicy backendPolicy = Mockito.mock(FederationBackendPolicy.class);
            Mockito.when(backendPolicy.getNextBe()).thenReturn(new Backend(1L, "127.0.0.1", 9050));
            Mockito.when(backendPolicy.numBackends()).thenReturn(1);
            TBrokerFileStatus fileStatus = new TBrokerFileStatus("s3://bucket/table/p=\\N/data.csv", false, 10,
                    splittable);
            NereidsFileGroupInfo fileGroupInfo = new NereidsFileGroupInfo(1L, 2L, Mockito.mock(OlapTable.class),
                    brokerDesc, fileGroup, Lists.newArrayList(fileStatus), 1, false, 1);
            fileGroupInfo.getFileStatusAndCalcInstance(backendPolicy);
            NereidsParamCreateContext context = new NereidsParamCreateContext();
            context.fileGroup = fileGroup;
            context.params = new TFileScanRangeParams();

            List<TScanRangeLocations> locations = new ArrayList<>();
            fileGroupInfo.createScanRangeLocations(context, backendPolicy, locations);

            Assertions.assertEquals(1, locations.size());
            List<TFileRangeDesc> ranges = locations.get(0).getScanRange().getExtScanRange().getFileScanRange()
                    .getRanges();
            Assertions.assertEquals(1, ranges.size());
            // A load keeps the meaning a literal \N had before the null flag existed: SQL NULL.
            Assertions.assertEquals(Collections.singletonList(""), ranges.get(0).getColumnsFromPath());
            Assertions.assertEquals(Collections.singletonList(true), ranges.get(0).getColumnsFromPathIsNull());
        }
    }
}
