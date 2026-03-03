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

package org.apache.doris.datasource.kinesis;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.LoadException;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.proto.InternalService;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class KinesisUtil {
    private static final Logger LOG = LogManager.getLogger(KinesisUtil.class);

    /**
     * Get all shard IDs for the given Kinesis stream.
     * This delegates to a BE node which uses the AWS SDK to call ListShards API.
     *
     * @param region   AWS region of the stream
     * @param stream   Kinesis stream name
     * @param endpoint optional custom endpoint (e.g. LocalStack), empty string for default
     * @param convertedCustomProperties AWS credentials and other properties
     * @return list of shard IDs (e.g. ["shardId-000000000000", "shardId-000000000001"])
     */
    public static List<String> getAllKinesisShards(String region, String stream, String endpoint,
            Map<String, String> convertedCustomProperties) throws LoadException {
        try {
            InternalService.PKinesisLoadInfo.Builder kinesisInfoBuilder =
                    InternalService.PKinesisLoadInfo.newBuilder()
                            .setRegion(region)
                            .setStream(stream)
                            .addAllProperties(convertedCustomProperties.entrySet().stream()
                                    .map(e -> InternalService.PStringPair.newBuilder()
                                            .setKey(e.getKey())
                                            .setVal(e.getValue())
                                            .build())
                                    .collect(Collectors.toList()));
            if (endpoint != null && !endpoint.isEmpty()) {
                kinesisInfoBuilder.setEndpoint(endpoint);
            }

            InternalService.PProxyRequest request = InternalService.PProxyRequest.newBuilder()
                    .setKinesisMetaRequest(
                            InternalService.PKinesisMetaProxyRequest.newBuilder()
                                    .setKinesisInfo(kinesisInfoBuilder))
                    .setTimeoutSecs(Config.max_get_kafka_meta_timeout_second)
                    .build();

            return getInfoRequest(request, Config.max_get_kafka_meta_timeout_second)
                    .getKinesisMetaResult().getShardIdsList();
        } catch (Exception e) {
            throw new LoadException(
                    "Failed to get shards of Kinesis stream: " + stream + ". error: " + e.getMessage());
        }
    }

    private static InternalService.PProxyResult getInfoRequest(InternalService.PProxyRequest request,
            int timeout) throws LoadException {
        long startTime = System.currentTimeMillis();
        int retryTimes = 0;
        TNetworkAddress address = null;
        Future<InternalService.PProxyResult> future = null;
        InternalService.PProxyResult result = null;
        Set<Long> failedBeIds = new HashSet<>();
        TStatusCode code = null;
        String errorMsg = null;

        try {
            while (retryTimes < 3) {
                List<Long> backendIds = new ArrayList<>();
                for (Long beId : Env.getCurrentSystemInfo().getAllBackendIds(true)) {
                    Backend backend = Env.getCurrentSystemInfo().getBackend(beId);
                    if (backend != null && backend.isLoadAvailable()
                            && !backend.isDecommissioned()
                            && !failedBeIds.contains(beId)
                            && !Env.getCurrentEnv().getRoutineLoadManager().isInBlacklist(beId)) {
                        backendIds.add(beId);
                    }
                }
                if (backendIds.isEmpty()) {
                    for (Long beId : Env.getCurrentEnv().getRoutineLoadManager().getBlacklist().keySet()) {
                        backendIds.add(beId);
                    }
                }
                if (backendIds.isEmpty()) {
                    MetricRepo.COUNTER_ROUTINE_LOAD_GET_META_FAIL_COUNT.increase(1L);
                    if (failedBeIds.isEmpty()) {
                        errorMsg = "no alive backends";
                    }
                    throw new LoadException("failed to get info: " + errorMsg + ",");
                }
                Collections.shuffle(backendIds);
                Backend be = Env.getCurrentSystemInfo().getBackend(backendIds.get(0));
                address = new TNetworkAddress(be.getHost(), be.getBrpcPort());
                long beId = be.getId();

                try {
                    future = BackendServiceProxy.getInstance().getInfo(address, request);
                    result = future.get(timeout, TimeUnit.SECONDS);
                } catch (Exception e) {
                    errorMsg = e.getMessage();
                    LOG.warn("failed to get kinesis info request to {} err {}", address, e.getMessage());
                    failedBeIds.add(beId);
                    retryTimes++;
                    continue;
                }
                code = TStatusCode.findByValue(result.getStatus().getStatusCode());
                if (code != TStatusCode.OK) {
                    errorMsg = result.getStatus().getErrorMsgsList().toString();
                    LOG.warn("failed to get kinesis info request to {} err {}", address,
                            result.getStatus().getErrorMsgsList());
                    failedBeIds.add(beId);
                    retryTimes++;
                } else {
                    return result;
                }
            }

            MetricRepo.COUNTER_ROUTINE_LOAD_GET_META_FAIL_COUNT.increase(1L);
            throw new LoadException("failed to get kinesis info: " + errorMsg + ",");
        } finally {
            if (code != null && code == TStatusCode.OK && !failedBeIds.isEmpty()) {
                for (Long beId : failedBeIds) {
                    Env.getCurrentEnv().getRoutineLoadManager().addToBlacklist(beId);
                    LOG.info("add beId {} to blacklist, blacklist: {}", beId,
                            Env.getCurrentEnv().getRoutineLoadManager().getBlacklist());
                }
            }
            long endTime = System.currentTimeMillis();
            MetricRepo.COUNTER_ROUTINE_LOAD_GET_META_LANTENCY.increase(endTime - startTime);
            MetricRepo.COUNTER_ROUTINE_LOAD_GET_META_COUNT.increase(1L);
        }
    }
}
