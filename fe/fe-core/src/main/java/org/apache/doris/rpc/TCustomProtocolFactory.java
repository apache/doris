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

package org.apache.doris.rpc;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransport;

/*
*   CustomProtocolFactory can change maxMessageSize of TTransport,
*   so that we can transfer message whose filed size is very large.
*/
public class TCustomProtocolFactory implements TProtocolFactory {
    private final int maxMessageSize;

    @Override
    public TProtocol getProtocol(TTransport tTransport) {
        tTransport.getConfiguration().setMaxMessageSize(maxMessageSize);
        return new TBinaryProtocol(tTransport);
    }

    public TCustomProtocolFactory(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }
}
