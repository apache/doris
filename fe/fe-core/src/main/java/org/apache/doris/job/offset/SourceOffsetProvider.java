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

package org.apache.doris.job.offset;

import org.apache.doris.job.extensions.insert.streaming.StreamingJobProperties;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;

import java.util.Map;

/**
 * Interface for managing offsets and metadata of a data source.
 */
public interface SourceOffsetProvider {

    /**
     * Get source type, e.g. s3, kafka
     * @return
     */
    String getSourceType();

    /**
     * Get next offset to consume
     * @return
     */
    Offset getNextOffset(StreamingJobProperties jobProps, Map<String, String> properties);

    /**
     * Get current offset
     * @return
     */
    Offset getCurrentOffset();

    /**
     * Get sync offset to show
     * @return
     */
    String getSyncOffset();

    /**
     * Get remote offset
     * @return
     */
    String getRemoteOffset();

    /**
     * Rewrite the TVF parameters in the SQL based on the current offset.
     * @param nextOffset
     * @return rewritten InsertIntoTableCommand
     */
    InsertIntoTableCommand rewriteTvfParams(InsertIntoTableCommand originCommand, Offset nextOffset);

    /**
     * Update the offset of the source.
     * @param offset
     */
    void updateOffset(Offset offset);

    /**
     * Fetch remote meta information, such as listing files in S3 or getting latest offsets in Kafka.
     */
    void fetchRemoteMeta(Map<String, String> properties) throws Exception;

    /**
     * Whether there is more data to consume
     * @return
     */
    boolean hasMoreDataToConsume();

}

