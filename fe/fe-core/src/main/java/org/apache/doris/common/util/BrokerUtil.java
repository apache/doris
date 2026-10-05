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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.UserException;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

public class BrokerUtil {

    public static String printBroker(String brokerName, TNetworkAddress address) {
        return brokerName + "[" + address.toString() + "]";
    }

    public static TNetworkAddress getAddress(BrokerDesc brokerDesc) throws UserException {
        FsBroker broker = null;
        try {
            String localIP = FrontendOptions.getLocalHostAddress();
            broker = Env.getCurrentEnv().getBrokerMgr().getBroker(brokerDesc.getName(), localIP);
        } catch (AnalysisException e) {
            throw new UserException(e.getMessage());
        }
        return new TNetworkAddress(broker.host, broker.port);
    }

    public static TPaloBrokerService.Client borrowClient(TNetworkAddress address) throws UserException {
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
        return client;
    }
}
