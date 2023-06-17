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

package org.apache.doris.regression.suite.client

import org.apache.doris.thrift.BackendService
import org.apache.doris.thrift.TNetworkAddress
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket
import org.apache.thrift.transport.TTransportException

class BackendClientImpl {
    private TSocket tSocket

    public TNetworkAddress address
    public int httpPort
    public BackendService.Client client

    BackendClientImpl(TNetworkAddress address, int httpPort) throws TTransportException {
        this.address = address
        this.httpPort = httpPort
        this.tSocket = new TSocket(address.hostname, address.port)
        this.client = new BackendService.Client(new TBinaryProtocol(this.tSocket))
        this.tSocket.open()
    }

    String toString() {
        return "BackendClientImpl: {" + address.toString() + " }"
    }

    void close() {
        tSocket.close()
    }
}
