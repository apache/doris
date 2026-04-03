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

package org.apache.doris.filesystem.broker;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.spi.FileSystemProvider;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * SPI provider for the Doris Broker filesystem.
 *
 * <p>Recognizes maps that contain "_STORAGE_TYPE_" = "BROKER" plus a resolved
 * "BROKER_HOST" key. The host and port are pre-resolved by fe-core's
 * {@code FileSystemFactory.getBrokerFileSystem()} before calling into the SPI,
 * avoiding any dependency on BrokerMgr here.
 *
 * <p>Registered via:
 * META-INF/services/org.apache.doris.filesystem.spi.FileSystemProvider
 */
public class BrokerFileSystemProvider implements FileSystemProvider {

    static final String KEY_TYPE      = "_STORAGE_TYPE_";
    static final String KEY_HOST      = "BROKER_HOST";
    static final String KEY_PORT      = "BROKER_PORT";
    static final String KEY_CLIENT_ID = "BROKER_CLIENT_ID";

    @Override
    public boolean supports(Map<String, String> properties) {
        return "BROKER".equals(properties.get(KEY_TYPE))
                && properties.containsKey(KEY_HOST);
    }

    @Override
    public FileSystem create(Map<String, String> properties) throws IOException {
        String host = properties.get(KEY_HOST);
        String portStr = properties.get(KEY_PORT);
        if (host == null || host.isEmpty()) {
            throw new IOException("BROKER_HOST is required");
        }
        if (portStr == null || portStr.isEmpty()) {
            throw new IOException("BROKER_PORT is required");
        }
        int port;
        try {
            port = Integer.parseInt(portStr);
        } catch (NumberFormatException e) {
            throw new IOException("Invalid BROKER_PORT: " + portStr);
        }
        String clientId = properties.getOrDefault(KEY_CLIENT_ID, "fe");
        Map<String, String> brokerParams = extractBrokerParams(properties);
        return new BrokerSpiFileSystem(host, port, clientId, brokerParams);
    }

    private Map<String, String> extractBrokerParams(Map<String, String> properties) {
        Map<String, String> params = new HashMap<>(properties);
        params.remove(KEY_TYPE);
        params.remove(KEY_HOST);
        params.remove(KEY_PORT);
        params.remove(KEY_CLIENT_ID);
        return params;
    }

    @Override
    public String name() {
        return "Broker";
    }
}
