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

package org.apache.doris.datasource.kafka;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.proto.InternalService;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class KafkaUtil {
    private static final Logger LOG = LogManager.getLogger(KafkaUtil.class);

    public static List<Integer> getAllKafkaPartitions(String brokerList, String topic,
            Map<String, String> convertedCustomProperties) throws UserException {
        try {
            InternalService.PProxyRequest request = InternalService.PProxyRequest.newBuilder().setKafkaMetaRequest(
                    InternalService.PKafkaMetaProxyRequest.newBuilder()
                            .setKafkaInfo(InternalService.PKafkaLoadInfo.newBuilder()
                                    .setBrokers(brokerList)
                                    .setTopic(topic)
                                    .addAllProperties(convertedCustomProperties.entrySet().stream()
                                            .map(e -> InternalService.PStringPair.newBuilder().setKey(e.getKey())
                                                    .setVal(e.getValue()).build()).collect(Collectors.toList())
                                    )
                            )
            ).build();
            return getInfoRequest(request, Config.max_get_kafka_meta_timeout_second)
                    .getKafkaMetaResult().getPartitionIdsList();
        } catch (Exception e) {
            throw new LoadException(
                    "Failed to get all partitions of kafka topic: " + topic + " error: " + e.getMessage());
        }
    }

    // Get offsets by times.
    // The input parameter "timestampOffsets" is <partition, timestamp>
    // Tne return value is <partition, offset>
    public static List<Pair<Integer, Long>> getOffsetsForTimes(String brokerList, String topic,
            Map<String, String> convertedCustomProperties, List<Pair<Integer, Long>> timestampOffsets)
            throws LoadException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("begin to get offsets for times of topic: {}, {}", topic, timestampOffsets);
        }
        try {
            InternalService.PKafkaMetaProxyRequest.Builder metaRequestBuilder =
                    InternalService.PKafkaMetaProxyRequest.newBuilder()
                            .setKafkaInfo(InternalService.PKafkaLoadInfo.newBuilder()
                                    .setBrokers(brokerList)
                                    .setTopic(topic)
                                    .addAllProperties(
                                            convertedCustomProperties.entrySet().stream().map(
                                                    e -> InternalService.PStringPair.newBuilder()
                                                            .setKey(e.getKey())
                                                            .setVal(e.getValue())
                                                            .build()
                                            ).collect(Collectors.toList())
                                    )
                            );
            for (Pair<Integer, Long> pair : timestampOffsets) {
                metaRequestBuilder.addOffsetTimes(InternalService.PIntegerPair.newBuilder().setKey(pair.first)
                        .setVal(pair.second).build());
            }

            InternalService.PProxyRequest request = InternalService.PProxyRequest.newBuilder().setKafkaMetaRequest(
                    metaRequestBuilder).setTimeoutSecs(Config.max_get_kafka_meta_timeout_second).build();
            InternalService.PProxyResult result = getInfoRequest(request, Config.max_get_kafka_meta_timeout_second);

            List<InternalService.PIntegerPair> pairs = result.getPartitionOffsets().getOffsetTimesList();
            List<Pair<Integer, Long>> partitionOffsets = Lists.newArrayList();
            for (InternalService.PIntegerPair pair : pairs) {
                partitionOffsets.add(Pair.of(pair.getKey(), pair.getVal()));
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("finish to get offsets for times of topic: {}, {}", topic, partitionOffsets);
            }
            return partitionOffsets;
        } catch (Exception e) {
            LOG.warn("failed to get offsets for times.", e);
            throw new LoadException(
                    "Failed to get offsets for times of kafka topic: " + topic + ". error: " + e.getMessage());
        }
    }

    public static List<Pair<Integer, Long>> getLatestOffsets(long jobId, UUID taskId, String brokerList, String topic,
                                                             Map<String, String> convertedCustomProperties,
                                                             List<Integer> partitionIds) throws LoadException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("begin to get latest offsets for partitions {} in topic: {}, task {}, job {}",
                    partitionIds, topic, taskId, jobId);
        }
        try {
            InternalService.PKafkaMetaProxyRequest.Builder metaRequestBuilder =
                    InternalService.PKafkaMetaProxyRequest.newBuilder()
                            .setKafkaInfo(InternalService.PKafkaLoadInfo.newBuilder()
                                    .setBrokers(brokerList)
                                    .setTopic(topic)
                                    .addAllProperties(
                                            convertedCustomProperties.entrySet().stream().map(
                                                    e -> InternalService.PStringPair.newBuilder()
                                                            .setKey(e.getKey())
                                                            .setVal(e.getValue())
                                                            .build()
                                            ).collect(Collectors.toList())
                                    )
                            );
            for (Integer partitionId : partitionIds) {
                metaRequestBuilder.addPartitionIdForLatestOffsets(partitionId);
            }
            InternalService.PProxyRequest request = InternalService.PProxyRequest.newBuilder().setKafkaMetaRequest(
                    metaRequestBuilder).setTimeoutSecs(Config.max_get_kafka_meta_timeout_second).build();
            InternalService.PProxyResult result = getInfoRequest(request, Config.max_get_kafka_meta_timeout_second);

            List<InternalService.PIntegerPair> pairs = result.getPartitionOffsets().getOffsetTimesList();
            List<Pair<Integer, Long>> partitionOffsets = Lists.newArrayList();
            for (InternalService.PIntegerPair pair : pairs) {
                partitionOffsets.add(Pair.of(pair.getKey(), pair.getVal()));
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("finish to get latest offsets for partitions {} in topic: {}, task {}, job {}",
                        partitionOffsets, topic, taskId, jobId);
            }
            return partitionOffsets;
        } catch (Exception e) {
            LOG.warn("failed to get latest offsets.", e);
            throw new LoadException(
                    "Failed to get latest offsets of kafka topic: " + topic + ". error: " + e.getMessage());
        }
    }

    public static List<Pair<Integer, Long>> getRealOffsets(String brokerList, String topic,
                                                             Map<String, String> convertedCustomProperties,
                                                             List<Pair<Integer, Long>> offsets)
                                                             throws LoadException {
        // filter values greater than 0 as these offsets is real offset
        // only update offset like OFFSET_BEGINNING or OFFSET_END
        List<Pair<Integer, Long>> offsetFlags = new ArrayList<>();
        List<Pair<Integer, Long>> realOffsets = new ArrayList<>();
        for (Pair<Integer, Long> pair : offsets) {
            if (pair.second < 0) {
                offsetFlags.add(pair);
            } else {
                realOffsets.add(pair);
            }
        }
        if (offsetFlags.size() == 0) {
            LOG.info("do not need update and directly return offsets for partitions {} in topic: {}", offsets, topic);
            return offsets;
        }

        try {
            InternalService.PKafkaMetaProxyRequest.Builder metaRequestBuilder =
                    InternalService.PKafkaMetaProxyRequest.newBuilder()
                            .setKafkaInfo(InternalService.PKafkaLoadInfo.newBuilder()
                                    .setBrokers(brokerList)
                                    .setTopic(topic)
                                    .addAllProperties(
                                            convertedCustomProperties.entrySet().stream().map(
                                                    e -> InternalService.PStringPair.newBuilder()
                                                            .setKey(e.getKey())
                                                            .setVal(e.getValue())
                                                            .build()
                                            ).collect(Collectors.toList())
                                    )
                            );
            for (Pair<Integer, Long> pair : offsetFlags) {
                metaRequestBuilder.addOffsetFlags(InternalService.PIntegerPair.newBuilder().setKey(pair.first)
                        .setVal(pair.second).build());
            }
            InternalService.PProxyRequest request = InternalService.PProxyRequest.newBuilder().setKafkaMetaRequest(
                    metaRequestBuilder).setTimeoutSecs(Config.max_get_kafka_meta_timeout_second).build();
            InternalService.PProxyResult result = getInfoRequest(request, Config.max_get_kafka_meta_timeout_second);

            List<InternalService.PIntegerPair> pairs = result.getPartitionOffsets().getOffsetTimesList();
            List<Pair<Integer, Long>> partitionOffsets = Lists.newArrayList();
            for (InternalService.PIntegerPair pair : pairs) {
                partitionOffsets.add(Pair.of(pair.getKey(), pair.getVal()));
            }
            realOffsets.addAll(partitionOffsets);
            LOG.info("finish to get real offsets for partitions {} in topic: {}", realOffsets, topic);
            return realOffsets;
        } catch (Exception e) {
            LOG.warn("failed to get real offsets.", e);
            throw new LoadException(
                    "Failed to get real offsets of kafka topic: " + topic + ". error: " + e.getMessage());
        }
    }

    private static InternalService.PProxyResult getInfoRequest(InternalService.PProxyRequest request, int timeout)
                                                        throws LoadException {
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
                // If there are no available backends, utilize the blacklist.
                // Special scenarios include:
                // 1. A specific job that connects to Kafka may time out for topic config or network error,
                //    leaving only one backend operational.
                // 2. If that sole backend is decommissioned, the aliveBackends list becomes empty.
                // Hence, in such cases, it's essential to rely on the blacklist to obtain meta information.
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
                    result = future.get(Config.max_get_kafka_meta_timeout_second, TimeUnit.SECONDS);
                } catch (Exception e) {
                    errorMsg = e.getMessage();
                    LOG.warn("failed to get info request to " + address + " err " + e.getMessage());
                    failedBeIds.add(beId);
                    retryTimes++;
                    continue;
                }
                code = TStatusCode.findByValue(result.getStatus().getStatusCode());
                if (code != TStatusCode.OK) {
                    errorMsg = result.getStatus().getErrorMsgsList().toString();
                    LOG.warn("failed to get info request to "
                            + address + " err " + result.getStatus().getErrorMsgsList());
                    failedBeIds.add(beId);
                    retryTimes++;
                } else {
                    return result;
                }
            }

            MetricRepo.COUNTER_ROUTINE_LOAD_GET_META_FAIL_COUNT.increase(1L);
            throw new LoadException("failed to get info: " + errorMsg + ",");
        } finally {
            // Ensure that not all BE added to the blacklist.
            // For single request:
            //     Only when the final success is achieved, the failed BE will be added to the blacklist,
            //     ensuring that there are always BE nodes that are not on the blacklist.
            // For multiple requests:
            //     If there is only one BE left without being blacklisted after multiple jitters,
            //     even if this BE fails, it will not be blacklisted.
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
