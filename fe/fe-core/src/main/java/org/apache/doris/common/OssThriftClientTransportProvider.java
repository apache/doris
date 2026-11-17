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

import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.tls.PrivateRpcTls;
import org.apache.doris.tls.TlsThriftSocket;
import org.apache.doris.tls.server.TlsProtocolSet.Protocol;

import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;

public class OssThriftClientTransportProvider implements ThriftClientTransportProvider {
    @Override
    public boolean supportsPrivateFileKeys() {
        // Legacy subclasses may override transport creation without using the OSS TLS implementation.
        return getClass() == OssThriftClientTransportProvider.class
                && PrivateRpcTls.supportsPrivateFileKeys(Protocol.THRIFT);
    }

    @Override
    public TTransport createTransport(TNetworkAddress address, int timeoutMs, boolean nonBlocking) throws Exception {
        TSocket socket = PrivateRpcTls.enabled(Protocol.THRIFT)
                ? new TlsThriftSocket(address.hostname, address.port, timeoutMs)
                : new TSocket(address.hostname, address.port, timeoutMs);
        return nonBlocking ? new TFramedTransport(socket) : socket;
    }

    @Override
    public TTransport createTransport(TNetworkAddress address, int timeoutMs, boolean nonBlocking, RpcPurpose purpose)
            throws Exception {
        // A legacy custom provider may subclass OSS and override the original method. Preserve that
        // dispatch instead of silently replacing its transport policy through the new overload.
        if (purpose != RpcPurpose.EXTERNAL_SERVICE || getClass() != OssThriftClientTransportProvider.class) {
            return createTransport(address, timeoutMs, nonBlocking);
        }
        TSocket socket = new TSocket(address.hostname, address.port, timeoutMs);
        return nonBlocking ? new TFramedTransport(socket) : socket;
    }
}
