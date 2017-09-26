// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.common;

import com.baidu.palo.thrift.TNetworkAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.server.ServerContext;

public class ThriftServerContext implements ServerContext {
    private static final Logger LOG = LogManager.getLogger(ThriftServerEventProcessor.class);
    private TNetworkAddress client;

    public ThriftServerContext(TNetworkAddress clientAddress) {
        this.client = clientAddress;
    }

    public TNetworkAddress getClient() {
        return client;
    }
}
