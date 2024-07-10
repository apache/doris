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

package org.apache.doris.cdcloader.mysql.reader;

import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.doris.cdcloader.mysql.rest.model.JobConfig;
import org.apache.doris.job.extensions.cdc.state.SourceSplit;

public interface SourceReader<T,C> {

    /**
     * Initialization, called when the program starts
     */
    void initialize();

    /**
     * Divide the data to be read. For example: split mysql to chunks
     * @return
     */
    List<? extends SourceSplit> getSourceSplits(JobConfig config) throws JsonProcessingException;

    /**
     * Reading Data
     * @param meta
     * @return
     * @throws Exception
     */
    T read(C meta) throws Exception;

    /**
     * Called when closing
     */
    void close(Long jobId);
}
