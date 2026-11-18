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

import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class LegacyThriftProviderCompatibilityTest {
    @Test
    void purposeSelectionPreservesLegacySubclassOverrides() throws Exception {
        try (TSocket selected = new TSocket("unused.invalid", 1)) {
            OssThriftClientTransportProvider provider = new OssThriftClientTransportProvider() {
                @Override
                public TTransport createTransport(TNetworkAddress address, int timeout, boolean nonBlocking) {
                    return selected;
                }
            };
            for (ThriftClientTransportProvider.RpcPurpose purpose : ThriftClientTransportProvider.RpcPurpose.values()) {
                Assertions.assertSame(selected, provider.createTransport(
                        new TNetworkAddress("unused.invalid", 1), 100, false, purpose));
                Assertions.assertSame(selected, provider.createTransport(
                        new TNetworkAddress("127.0.0.1", 1), 100, false, purpose, "unused.invalid"));
            }
        }
    }
}
