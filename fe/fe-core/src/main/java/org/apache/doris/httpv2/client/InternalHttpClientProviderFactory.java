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

package org.apache.doris.httpv2.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.ServiceLoader;

public final class InternalHttpClientProviderFactory {
    private static final Logger LOG = LogManager.getLogger(InternalHttpClientProviderFactory.class);
    private static final InternalHttpClientProvider INSTANCE = loadProvider();

    private InternalHttpClientProviderFactory() {
    }

    public static InternalHttpClientProvider getProvider() {
        return INSTANCE;
    }

    private static InternalHttpClientProvider loadProvider() {
        ServiceLoader<InternalHttpClientProvider> loader = ServiceLoader.load(InternalHttpClientProvider.class);
        Iterator<InternalHttpClientProvider> iterator = loader.iterator();
        if (iterator.hasNext()) {
            InternalHttpClientProvider provider = iterator.next();
            LOG.info("Using InternalHttpClientProvider implementation: {}", provider.getClass().getName());
            return provider;
        }
        LOG.info("No custom InternalHttpClientProvider found, fallback to OSS implementation");
        return new OssInternalHttpClientProvider();
    }
}
