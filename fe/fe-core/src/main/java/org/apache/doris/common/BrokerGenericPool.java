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

package org.apache.doris.common;

import org.apache.doris.thrift.TBrokerOperationStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.apache.doris.thrift.TBrokerPingBrokerRequest;
import org.apache.doris.thrift.TBrokerVersion;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BrokerGenericPool extends GenericPool {

    private static final Logger LOG = LogManager.getLogger(BrokerGenericPool.class);

    public BrokerGenericPool(String className, GenericKeyedObjectPoolConfig config, int timeoutMs) {
        super(className, config, timeoutMs);
        BrokerThriftClientFactory factory = new BrokerThriftClientFactory();
        pool = new GenericKeyedObjectPool<>(factory, config);
    }

    private class BrokerThriftClientFactory extends ThriftClientFactory {
        @Override
        public boolean validateObject(TNetworkAddress key, PooledObject p) {
            try {
                TBrokerPingBrokerRequest request = new TBrokerPingBrokerRequest(TBrokerVersion.VERSION_ONE,
                        key.getHostname());
                TBrokerOperationStatus status = ((TPaloBrokerService.Client) p.getObject()).ping(request);
                return status.getStatusCode() == TBrokerOperationStatusCode.OK;
            } catch (Exception e) {
                LOG.error("broker client validate error: ", e);
                return false;
            }
        }

    }

}
