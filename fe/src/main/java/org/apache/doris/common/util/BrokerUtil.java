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

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.UserException;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TBrokerListPathRequest;
import org.apache.doris.thrift.TBrokerListResponse;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.apache.doris.thrift.TBrokerVersion;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.List;

public class BrokerUtil {
    private static final Logger LOG = LogManager.getLogger(BrokerUtil.class);

    public static void parseBrokerFile(String path, BrokerDesc brokerDesc, List<TBrokerFileStatus> fileStatuses)
            throws UserException {
        FsBroker broker = null;
        try {
            String localIP = FrontendOptions.getLocalHostAddress();
            broker = Catalog.getInstance().getBrokerMgr().getBroker(brokerDesc.getName(), localIP);
        } catch (AnalysisException e) {
            throw new UserException(e.getMessage());
        }
        TNetworkAddress address = new TNetworkAddress(broker.ip, broker.port);
        TPaloBrokerService.Client client = null;
        try {
            client = ClientPool.brokerPool.borrowObject(address);
        } catch (Exception e) {
            try {
                client = ClientPool.brokerPool.borrowObject(address);
            } catch (Exception e1) {
                throw new UserException("Create connection to broker(" + address + ") failed.");
            }
        }
        boolean failed = true;
        try {
            TBrokerListPathRequest request = new TBrokerListPathRequest(
                    TBrokerVersion.VERSION_ONE, path, false, brokerDesc.getProperties());
            TBrokerListResponse tBrokerListResponse = null;
            try {
                tBrokerListResponse = client.listPath(request);
            } catch (TException e) {
                ClientPool.brokerPool.reopen(client);
                tBrokerListResponse = client.listPath(request);
            }
            if (tBrokerListResponse.getOpStatus().getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new UserException("Broker list path failed.path=" + path
                        + ",broker=" + address + ",msg=" + tBrokerListResponse.getOpStatus().getMessage());
            }
            failed = false;
            for (TBrokerFileStatus tBrokerFileStatus : tBrokerListResponse.getFiles()) {
                if (tBrokerFileStatus.isDir) {
                    continue;
                }
                fileStatuses.add(tBrokerFileStatus);
            }
        } catch (TException e) {
            LOG.warn("Broker list path exception, path={}, address={}, exception={}", path, address, e);
            throw new UserException("Broker list path exception.path=" + path + ",broker=" + address);
        } finally {
            if (failed) {
                ClientPool.brokerPool.invalidateObject(address, client);
            } else {
                ClientPool.brokerPool.returnObject(address, client);
            }
        }
    }

    public static String printBroker(String brokerName, TNetworkAddress address) {
        return brokerName + "[" + address.toString() + "]";
    }

}
