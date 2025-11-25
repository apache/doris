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

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import org.apache.doris.cdcclient.model.JobConfig;
import org.apache.doris.cdcclient.model.req.BaseRecordReq;
import org.apache.doris.cdcclient.model.req.FetchRecordReq;
import org.apache.doris.cdcclient.model.resp.RecordWithMeta;
import org.apache.doris.cdcclient.source.split.SourceSplit;

/**
 * SourceReader 接口，支持泛型以指定 Split 和 SplitState 类型。
 *
 * @param <Split> Split 类型（如 MySqlSplit）
 * @param <SplitState> SplitState 类型（如 MySqlSplitState）
 */
public interface SourceReader<Split, SplitState> {

    /** Initialization, called when the program starts */
    void initialize();

    /**
     * Divide the data to be read. For example: split mysql to chunks
     *
     * @return
     */
    List<? extends SourceSplit> getSourceSplits(JobConfig config) throws JsonProcessingException;

    /**
     * Reading Data
     *
     * @param meta
     * @return
     * @throws Exception
     */
    RecordWithMeta read(FetchRecordReq meta) throws Exception;

    /**
     * Reading Data for split reader
     *
     * @param baseReq 基础请求
     * @return 读取结果，包含 SourceRecord 列表和状态信息
     */
    default SplitReadResult<Split, SplitState> readSplitRecords(BaseRecordReq baseReq)
            throws Exception {
        throw new UnsupportedOperationException(
                "readSplitRecords is not supported by " + this.getClass().getName());
    }

    /** Called when closing */
    void close(Long jobId);
}
