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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.ServiceLoader;

public final class ThriftClientTransportProviderFactory {
    private static final Logger LOG = LogManager.getLogger(ThriftClientTransportProviderFactory.class);

    private ThriftClientTransportProviderFactory() {
    }

    public static ThriftClientTransportProvider create() {
        ServiceLoader<ThriftClientTransportProvider> loader = ServiceLoader.load(ThriftClientTransportProvider.class);
        Iterator<ThriftClientTransportProvider> iterator = loader.iterator();
        if (iterator.hasNext()) {
            ThriftClientTransportProvider provider = iterator.next();
            LOG.info("Using ThriftClientTransportProvider implementation: {}", provider.getClass().getName());
            return provider;
        }
        LOG.info("No custom ThriftClientTransportProvider found, fallback to OSS implementation");
        return new OssThriftClientTransportProvider();
    }
}
