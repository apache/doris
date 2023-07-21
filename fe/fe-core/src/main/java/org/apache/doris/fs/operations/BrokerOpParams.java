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

package org.apache.doris.fs.operations;

import org.apache.doris.thrift.TBrokerFD;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

public class BrokerOpParams extends OpParams {
    private final TPaloBrokerService.Client client;
    private final TNetworkAddress address;
    private final TBrokerFD fd;
    private String remoteFilePath;

    protected BrokerOpParams(TPaloBrokerService.Client client, TNetworkAddress address, TBrokerFD fd) {
        this(client, address, null, fd);
    }

    protected BrokerOpParams(TPaloBrokerService.Client client, TNetworkAddress address,
                          String remoteFilePath, TBrokerFD fd) {
        this.client = client;
        this.address = address;
        this.remoteFilePath = remoteFilePath;
        this.fd = fd;
    }

    public TPaloBrokerService.Client client() {
        return client;
    }

    public TNetworkAddress address() {
        return address;
    }

    public String remotePath() {
        return remoteFilePath;
    }

    public TBrokerFD fd() {
        return fd;
    }
}
