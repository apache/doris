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
import org.apache.doris.common.UserException;
import org.apache.doris.proto.PKafkaLoadInfo;
import org.apache.doris.proto.PKafkaMetaProxyRequest;
import org.apache.doris.proto.PProxyRequest;
import org.apache.doris.proto.PProxyResult;
import org.apache.doris.proto.PStringPair;
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
            PKafkaLoadInfo kafkaLoadInfo = new PKafkaLoadInfo();
            kafkaLoadInfo.brokers = brokerList;
            kafkaLoadInfo.topic = topic;
            for (Map.Entry<String, String> entry : convertedCustomProperties.entrySet()) {
                PStringPair pair = new PStringPair();
                pair.key = entry.getKey();
                pair.val = entry.getValue();
                if (kafkaLoadInfo.properties == null) {
                    kafkaLoadInfo.properties = Lists.newArrayList();
                }
                kafkaLoadInfo.properties.add(pair);
            }
            PKafkaMetaProxyRequest kafkaRequest = new PKafkaMetaProxyRequest();
            kafkaRequest.kafka_info = kafkaLoadInfo;
            PProxyRequest request = new PProxyRequest();
            request.kafka_meta_request = kafkaRequest;
            
            // get info
            Future<PProxyResult> future = BackendServiceProxy.getInstance().getInfo(address, request);
            PProxyResult result = future.get(5, TimeUnit.SECONDS);
            TStatusCode code = TStatusCode.findByValue(result.status.status_code);
            if (code != TStatusCode.OK) {
                throw new UserException("failed to get kafka partition info: " + result.status.error_msgs);
            } else {
                return result.kafka_meta_result.partition_ids;
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
}

