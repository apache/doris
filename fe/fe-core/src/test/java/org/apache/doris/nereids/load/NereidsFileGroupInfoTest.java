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
import org.apache.doris.catalog.Table;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.FederationBackendPolicy;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TScanRangeLocations;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class NereidsFileGroupInfoTest {

    @Test
    public void testCreateScanRangeLocationsUnsplittable(
            @Injectable FederationBackendPolicy backendPolicy,
            @Injectable NereidsBrokerFileGroup fileGroup,
            @Injectable Table targetTable,
            @Mocked BrokerDesc brokerDesc) throws UserException {

        List<TBrokerFileStatus> fileStatuses = new ArrayList<>();
        TBrokerFileStatus lzoFile = new TBrokerFileStatus();
        lzoFile.path = "hdfs://localhost:8900/data.csv.lzo";
        lzoFile.size = 100;
        fileStatuses.add(lzoFile);

        TBrokerFileStatus plainFile = new TBrokerFileStatus();
        plainFile.path = "hdfs://localhost:8900/data.csv";
        plainFile.size = 50;
        fileStatuses.add(plainFile);

        NereidsFileGroupInfo fileGroupInfo = new NereidsFileGroupInfo(1L, 1L, targetTable, brokerDesc, fileGroup, fileStatuses, 2, false, 1);
        Deencapsulation.setField(fileGroupInfo, "numInstances", 1);

        TFileScanRangeParams params = new TFileScanRangeParams();
        NereidsParamCreateContext context = new NereidsParamCreateContext();
        context.params = params;
        context.fileGroup = fileGroup;

        new Expectations() {
            {
                fileGroup.getFileFormatProperties().getFormatName();
                result = "csv";
                fileGroup.getFileFormatProperties().getCompressionType();
                result = TFileCompressType.UNKNOWN; // infer from path
                fileGroup.getColumnNamesFromPath();
                result = new ArrayList<String>();
            }
        };

        List<TScanRangeLocations> scanRangeLocations = new ArrayList<>();
        fileGroupInfo.createScanRangeLocationsUnsplittable(context, backendPolicy, scanRangeLocations);

        Assertions.assertEquals(1, scanRangeLocations.size());
        List<TFileRangeDesc> ranges = scanRangeLocations.get(0).getScanRange().getExtScanRange().getFileScanRange().getRanges();
        Assertions.assertEquals(2, ranges.size());

        // Check LZO file
        TFileRangeDesc lzoRange = ranges.get(0);
        Assertions.assertEquals(TFileFormatType.FORMAT_CSV_PLAIN, lzoRange.getFormatType());
        Assertions.assertEquals(TFileCompressType.LZOP, lzoRange.getCompressType());

        // Check Plain file
        TFileRangeDesc plainRange = ranges.get(1);
        Assertions.assertEquals(TFileFormatType.FORMAT_CSV_PLAIN, plainRange.getFormatType());
        Assertions.assertEquals(TFileCompressType.PLAIN, plainRange.getCompressType());

        Assertions.assertFalse(params.isSetFormatType());
        Assertions.assertFalse(params.isSetCompressType());
    }
}
