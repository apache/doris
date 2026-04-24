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

import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.streaming.StreamingInsertJob;
import org.apache.doris.job.extensions.insert.streaming.StreamingJobProperties;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;

import java.util.List;
import java.util.Map;

/**
 * Interface for managing offsets and metadata of a data source.
 */
public interface SourceOffsetProvider {

    /**
     * Get source type, e.g. s3, kafka
     *
     * @return
     */
    String getSourceType();

    /**
     * Initialize the offset provider with job ID and original TVF properties.
     * Only sets in-memory fields; safe to call on both fresh start and FE restart.
     * May perform remote calls (e.g. fetching snapshot splits), so throws JobException.
     */
    default void ensureInitialized(Long jobId, Map<String, String> originTvfProps) throws JobException {}

    /**
     * Performs one-time initialization that must run only on fresh job creation, not on FE restart.
     * For example, fetching and persisting snapshot splits to the meta table.
     * Default: no-op (most providers need no extra setup).
     */
    default void initOnCreate() throws JobException {}

    /**
     * Get next offset to consume
     *
     * @return
     */
    Offset getNextOffset(StreamingJobProperties jobProps, Map<String, String> properties);

    /**
     * Get current offset to show
     *
     * @return
     */
    String getShowCurrentOffset();

    /**
     * Get remote datasource max offset to show
     *
     * @return
     */
    String getShowMaxOffset();

    /**
     * Rewrite the TVF parameters in the SQL based on the current offset.
     * Only implemented by TVF-based providers (e.g. S3, cdc_stream).
     *
     * @param nextOffset
     * @return rewritten InsertIntoTableCommand
     */
    InsertIntoTableCommand rewriteTvfParams(InsertIntoTableCommand originCommand, Offset nextOffset, long taskId);

    /**
     * Update the offset of the source.
     *
     * @param offset
     */
    void updateOffset(Offset offset);

    /**
     * Fetch remote meta information, such as listing files in S3 or getting latest offsets in Kafka.
     */
    void fetchRemoteMeta(Map<String, String> properties) throws Exception;

    /**
     * Whether there is more data to consume
     *
     * @return
     */
    boolean hasMoreDataToConsume();

    /**
     * Deserialize string offset to Offset
     *
     * @return
     */
    Offset deserializeOffset(String offset);

    /**
     * Deserialize offset property to Offset
     *
     * @return
     */
    Offset deserializeOffsetProperty(String offset);

    /**
     * Replaying OffsetProvider is currently only required by JDBC.
     *
     * @return
     */
    default void replayIfNeed(StreamingInsertJob job)  throws JobException {
    }

    default String getPersistInfo() {
        return null;
    }

    /**
     * Restore offset from persisted string during image load (gsonPostProcess).
     * Called immediately after the provider is created so that even PAUSED jobs
     * have the correct offset state.
     */
    default void restoreFromPersistInfo(String persistInfo) {
    }

    /**
     * Returns the serialized JSON offset to store in txn commit attachment.
     * Default: serialize running offset directly (e.g. S3 path).
     * CDC stream TVF overrides to pull actual end offset from BE after fetchRecordStream completes.
     * scanBackendIds: IDs of the BEs that ran the TVF scan node, used to locate taskOffsetCache.
     */
    default String getCommitOffsetJson(Offset runningOffset, long taskId, List<Long> scanBackendIds) {
        return runningOffset.toSerializedJson();
    }

    /**
     * Called after each task is committed. Providers that track data availability
     * (e.g. JDBC binlog) can use this to update internal state such as hasMoreData.
     * Default: no-op.
     */
    default void onTaskCommitted(long scannedRows, long loadBytes) {}

    /**
     * Applies the end offset from a committed task back onto the running offset object
     * in-place, so that showRange() can display the full [start, end] interval.
     * Default: no-op (only meaningful for JDBC providers).
     */
    default void applyEndOffsetToTask(Offset runningOffset, Offset endOffset) {}

    /**
     * Returns true if the provider has reached a natural completion point
     * and the job should be marked as FINISHED.
     * Default: false (most providers run indefinitely).
     */
    default boolean hasReachedEnd() {
        return false;
    }

    /**
     * Get the lag of the data source in seconds.
     * For CDC sources, lag = (now - last consumed event timestamp) in seconds.
     *
     * @return lag in seconds as string, empty string if not applicable
     */
    default String getLag() {
        return "";
    }

}

