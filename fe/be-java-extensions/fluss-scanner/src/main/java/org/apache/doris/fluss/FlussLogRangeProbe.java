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

package org.apache.doris.fluss;

import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableOrPartition;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.FetchLogResponse;
import org.apache.fluss.rpc.messages.PbFetchLogReqForBucket;
import org.apache.fluss.rpc.messages.PbFetchLogReqForTable;
import org.apache.fluss.rpc.messages.PbFetchLogRespForBucket;
import org.apache.fluss.rpc.protocol.FetchLogReadPreference;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Diagnose a bounded scanner that received no progress. Fluss 1.0 discards successful empty fetches:
 * a lake-covered offset therefore looks exactly like an idle scanner through {@code LogScanner.poll}.
 * A direct, non-consuming fetch retains the response's high watermark and whether the start is served
 * by local records or a remote segment, so we can report an unreadable range instead of polling forever.
 */
final class FlussLogRangeProbe implements BoundedLogRecords.LogRangeProbe {

    private static final int PROBE_BYTES = 1024 * 1024;
    private static final int PROBE_TIMEOUT_SECONDS = 15;

    private final MetadataUpdater metadataUpdater;
    private final TableBucket tableBucket;

    FlussLogRangeProbe(FlussConnection connection, TableBucket tableBucket) {
        this.metadataUpdater = connection.getMetadataUpdater();
        this.tableBucket = tableBucket;
    }

    @Override
    public BoundedLogRecords.ProbeResult probe(long offset) throws IOException {
        Optional<BucketLocation> location = metadataUpdater.getBucketLocation(tableBucket);
        if (!location.isPresent() || location.get().getLeader() == null) {
            return new BoundedLogRecords.ProbeResult(true, -1L);
        }
        int leader = location.get().getLeader();
        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(leader);
        if (gateway == null) {
            // The scanner already handles leader changes and metadata refresh. A probe must not turn
            // a transient routing gap into a query error before that recovery path can run.
            return new BoundedLogRecords.ProbeResult(true, -1L);
        }
        FetchLogRequest request = new FetchLogRequest()
                .setFollowerServerId(-1)
                .setMaxBytes(PROBE_BYTES)
                .setMinBytes(0)
                .setMaxWaitMs(0)
                .setReadPreference(FetchLogReadPreference.REMOTE_FIRST.value());
        PbFetchLogReqForTable tableRequest = request.addTablesReq()
                .setTableId(tableBucket.getTableId())
                .setProjectionPushdownEnabled(false);
        PbFetchLogReqForBucket bucketRequest = tableRequest.addBucketsReq()
                .setBucketId(tableBucket.getBucket())
                .setFetchOffset(offset)
                .setMaxFetchBytes(PROBE_BYTES);
        Long partitionId = tableBucket.getPartitionId();
        if (partitionId != null) {
            bucketRequest.setPartitionId(partitionId);
        }
        metadataUpdater.getCluster()
                .getBucketCount(TableOrPartition.of(tableBucket.getTableId(), partitionId))
                .ifPresent(bucketRequest::setRoutingBucketCount);

        FetchLogResponse response;
        try {
            response = gateway.fetchLog(request).get(PROBE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while checking Fluss log range " + tableBucket, e);
        } catch (ExecutionException | TimeoutException e) {
            // The ordinary scanner owns fetch retries and errors. This probe only diagnoses a
            // successful empty response, which the SDK discards before it reaches that scanner.
            return new BoundedLogRecords.ProbeResult(true, -1L);
        }
        ByteBuf buffer = response.getParsedByteBuf();
        try {
            if (response.getTablesRespsCount() != 1
                    || response.getTablesRespAt(0).getBucketsRespsCount() != 1) {
                throw new IOException("Fluss returned no fetch status for " + tableBucket);
            }
            PbFetchLogRespForBucket bucket = response.getTablesRespAt(0).getBucketsRespAt(0);
            if (bucket.hasErrorCode() && bucket.getErrorCode() != 0) {
                return new BoundedLogRecords.ProbeResult(true, -1L);
            }
            boolean readable = bucket.hasRemoteLogFetchInfo() || bucket.getRecordsSize() > 0
                    || bucket.hasFilteredEndOffset();
            return new BoundedLogRecords.ProbeResult(readable,
                    bucket.hasHighWatermark() ? bucket.getHighWatermark() : -1L);
        } finally {
            if (buffer != null) {
                buffer.release();
            }
        }
    }
}
