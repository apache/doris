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

package org.apache.doris.connector.spi;

/**
 * A neutral, immutable broker backend address (host + port) returned by
 * {@link ConnectorContext#getBrokerAddresses()} for a {@code FILE_BROKER} write target.
 *
 * <p>This is a Thrift-free SPI carrier: the engine resolves the catalog's bound broker (a fe-core concern
 * the connector must not import) and hands back these neutral host/port pairs; the connector — which has
 * the Thrift types — maps each to its own {@code TNetworkAddress}. Exactly the same pattern as
 * {@link ConnectorContext#getBackendFileType}, which returns a {@code TFileType} enum name as a String the
 * connector maps back, keeping this SPI free of Thrift dependencies.
 */
public final class ConnectorBrokerAddress {

    private final String host;
    private final int port;

    public ConnectorBrokerAddress(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
