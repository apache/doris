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

package org.apache.doris.common.proc;

import com.google.common.collect.ImmutableList;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.load.StreamLoadRecordMgr;

import java.util.List;

/*
    SHOW PROC "/stream_loads"
    show statistic of all of running loads

    RESULT:
    Label | DbId | FinishTime
 */
public class StreamLoadProcNode implements ProcNodeInterface {

    private static final ImmutableList<String> TITLE_NAMES =
            new ImmutableList.Builder<String>()
                    .add("Label")
                    .add("DbId")
                    .add("FinishTime")
                    .build();

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult baseProcResult = new BaseProcResult();
        baseProcResult.setNames(TITLE_NAMES);
        StreamLoadRecordMgr streamLoadRecordMgr = Catalog.getCurrentCatalog().getStreamLoadRecordMgr();
        try {
            List<StreamLoadRecordMgr.StreamLoadItem> streamLoadJobList = streamLoadRecordMgr.getStreamLoadRecords();
            for (StreamLoadRecordMgr.StreamLoadItem streamLoadItem : streamLoadJobList) {
                baseProcResult.addRow(streamLoadItem.getStatistics());
            }
        } catch (Exception e) {
            throw new AnalysisException("failed to get all of stream load job");
        }
        return baseProcResult;
    }
}
