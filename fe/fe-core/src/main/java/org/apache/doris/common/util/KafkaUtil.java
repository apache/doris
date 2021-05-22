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

package org.apache.doris.common.util;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.proto.InternalService;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class KafkaUtil {
    private static final Logger LOG = LogManager.getLogger(KafkaUtil.class);

    public static List<Integer> getAllKafkaPartitions(String brokerList, String topic,
            Map<String, String> convertedCustomProperties) throws UserException {
        BackendService.Client client = null;
        TNetworkAddress address = null;
        boolean ok = false;
        try {
            List<Long> backendIds = Catalog.getCurrentSystemInfo().getBackendIds(true);
            if (backendIds.isEmpty()) {
                throw new LoadException("Failed to get all partitions. No alive backends");
            }
            Collections.shuffle(backendIds);
            Backend be = Catalog.getCurrentSystemInfo().getBackend(backendIds.get(0));
            address = new TNetworkAddress(be.getHost(), be.getBrpcPort());
            
            // create request
            InternalService.PProxyRequest request = InternalService.PProxyRequest.newBuilder().setKafkaMetaRequest(
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
                            )
            ).build();
            
            // get info
            Future<InternalService.PProxyResult> future = BackendServiceProxy.getInstance().getInfo(address, request);
            InternalService.PProxyResult result = future.get(5, TimeUnit.SECONDS);
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                throw new UserException("failed to get kafka partition info: " + result.getStatus().getErrorMsgsList());
            } else {
                return result.getKafkaMetaResult().getPartitionIdsList();
            }
        } catch (Exception e) {
            LOG.warn("failed to get partitions.", e);
            throw new LoadException(
                    "Failed to get all partitions of kafka topic: " + topic + ". error: " + e.getMessage());
        } finally {
            if (ok) {
                ClientPool.backendPool.returnObject(address, client);
            } else {
                ClientPool.backendPool.invalidateObject(address, client);
            }
        }
    }

    // Get offsets by times.
    // The input parameter "timestampOffsets" is <partition, timestamp>
    // Tne return value is <partition, offset>
    public static List<Pair<Integer, Long>> getOffsetsForTimes(String brokerList, String topic,
                                                               Map<String, String> convertedCustomProperties,
                                                               List<Pair<Integer, Long>> timestampOffsets) throws LoadException {
        BackendService.Client client = null;
        TNetworkAddress address = null;
        LOG.debug("begin to get offsets for times of topic: {}, {}", topic, timestampOffsets);
        boolean ok = false;
        try {
            List<Long> backendIds = Catalog.getCurrentSystemInfo().getBackendIds(true);
            if (backendIds.isEmpty()) {
                throw new LoadException("Failed to get offset for times. No alive backends");
            }
            Collections.shuffle(backendIds);
            Backend be = Catalog.getCurrentSystemInfo().getBackend(backendIds.get(0));
            address = new TNetworkAddress(be.getHost(), be.getBrpcPort());

            // create request
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
                    metaRequestBuilder).build();

            // get info
            Future<InternalService.PProxyResult> future = BackendServiceProxy.getInstance().getInfo(address, request);
            InternalService.PProxyResult result = future.get(5, TimeUnit.SECONDS);
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                throw new UserException("failed to get kafka partition info: " + result.getStatus().getErrorMsgsList());
            } else {
                List<InternalService.PIntegerPair> pairs = result.getPartitionOffsets().getOffsetTimesList();
                List<Pair<Integer, Long>> partitionOffsets = Lists.newArrayList();
                for (InternalService.PIntegerPair pair : pairs) {
                    partitionOffsets.add(Pair.create(pair.getKey(), pair.getVal()));
                }
                LOG.debug("finish to get offsets for times of topic: {}, {}", topic, partitionOffsets);
                return partitionOffsets;
            }
        } catch (Exception e) {
            LOG.warn("failed to get offsets for times.", e);
            throw new LoadException(
                    "Failed to get offsets for times of kafka topic: " + topic + ". error: " + e.getMessage());
        } finally {
            if (ok) {
                ClientPool.backendPool.returnObject(address, client);
            } else {
                ClientPool.backendPool.invalidateObject(address, client);
            }
        }
    }
}


