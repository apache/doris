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

package org.apache.doris.cdcclient.source.reader;

import java.util.List;
import java.util.Map;
import org.apache.doris.cdcclient.model.request.FetchRecordReq;
import org.apache.doris.cdcclient.model.request.FetchTableSplitsReq;
import org.apache.doris.cdcclient.model.request.JobBaseRecordReq;
import org.apache.doris.cdcclient.model.response.RecordWithMeta;
import org.apache.doris.cdcclient.source.split.SourceSplit;

/**
 * SourceReader interface
 *
 * @param <Split> Split (MySqlSplit)
 * @param <SplitState> SplitState(MySqlSplitState)
 */
public interface SourceReader<Split, SplitState> {
    /** Initialization, called when the program starts */
    void initialize();

    /** Divide the data to be read. For example: split mysql to chunks */
    List<? extends SourceSplit> getSourceSplits(FetchTableSplitsReq config);

    /** Reading Data */
    RecordWithMeta read(FetchRecordReq meta) throws Exception;

    /** Reading Data for split reader */
    default SplitReadResult<Split, SplitState> readSplitRecords(JobBaseRecordReq baseReq)
            throws Exception {
        throw new UnsupportedOperationException(
                "readSplitRecords is not supported by " + this.getClass().getName());
    }

    /** Extract offset information from snapshot split state. */
    Map<String, String> extractSnapshotOffset(Object splitState, Object split);

    /** Extract offset information from binlog split. */
    Map<String, String> extractBinlogOffset(Object split, boolean pureBinlogPhase);

    /**
     * Get split ID from the split. This method should be implemented by each SourceReader to handle
     * its specific Split type.
     *
     * @param split the split
     * @return split ID, or null if split is null
     */
    String getSplitId(Object split);

    /** Called when closing */
    void close(Long jobId);

    void finishSplitRecords();
}
