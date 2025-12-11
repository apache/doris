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

import org.apache.doris.cdcclient.model.response.RecordWithMeta;
import org.apache.doris.job.cdc.request.CompareOffsetRequest;
import org.apache.doris.job.cdc.request.FetchRecordRequest;
import org.apache.doris.job.cdc.request.FetchTableSplitsRequest;
import org.apache.doris.job.cdc.request.JobBaseConfig;
import org.apache.doris.job.cdc.request.JobBaseRecordRequest;
import org.apache.doris.job.cdc.split.AbstractSourceSplit;

import org.apache.flink.api.connector.source.SourceSplit;

import java.util.List;
import java.util.Map;

/** Source Reader Interface */
public interface SourceReader {
    /** Initialization, called when the program starts */
    void initialize();

    /** Divide the data to be read. For example: split mysql to chunks */
    List<AbstractSourceSplit> getSourceSplits(FetchTableSplitsRequest config);

    /** Reading Data */
    RecordWithMeta read(FetchRecordRequest meta) throws Exception;

    /** Reading Data for split reader */
    SplitReadResult readSplitRecords(JobBaseRecordRequest baseReq) throws Exception;

    /** Extract offset information from snapshot split state. */
    Map<String, String> extractSnapshotOffset(SourceSplit split, Object splitState);

    /** Extract offset information from binlog split. */
    Map<String, String> extractBinlogOffset(SourceSplit split);

    /** Is the split a binlog split */
    boolean isBinlogSplit(SourceSplit split);

    /** Is the split a snapshot split */
    boolean isSnapshotSplit(SourceSplit split);

    /** Finish reading all split records */
    void finishSplitRecords();

    /** Get the end offset for the job */
    Map<String, String> getEndOffset(JobBaseConfig jobConfig);

    /** Compare the offsets */
    int compareOffset(CompareOffsetRequest compareOffsetRequest);

    /** Called when closing */
    void close(Long jobId);
}
