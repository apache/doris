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
 * Context provided to connectors during pre-creation validation (CREATE CATALOG).
 *
 * <p>The engine implements this interface to expose the infrastructure services a connector may need
 * while it validates a catalog it is about to create: reading and storing catalog properties, and
 * deferring a BE-side connectivity test to the engine. Each connector type calls only the services
 * relevant to its own validation logic.</p>
 *
 * <p>Validating and resolving a driver jar is NOT an engine service any more: the policy lives with the
 * connectors, in {@link DriverUrlPolicy}, fed from {@link ConnectorContext#getEnvironment()}. Keeping it
 * here meant the engine carried one connector family's file-format rules; every connector that loads a
 * driver jar now applies the one shared policy itself.</p>
 */
public interface ConnectorValidationContext {

    /** Returns the catalog ID. */
    long getCatalogId();

    /** Returns a catalog property value, or {@code null} if not set. */
    String getProperty(String key);

    /** Stores a computed property back into the catalog configuration. */
    void storeProperty(String key, String value);

    /**
     * Registers a BE→external connectivity test request. The engine will
     * execute this test after {@code preCreateValidation()} returns by
     * sending the payload to an alive backend via BRPC.
     *
     * <p>Connectors build the serialized Thrift descriptor (e.g., TTableDescriptor);
     * the engine handles finding an alive backend, BRPC transport, and result checking.</p>
     *
     * @param serializedDescriptor the Thrift-serialized connection descriptor
     * @param connectionTypeValue the connection type identifier (e.g., TOdbcTableType value)
     * @param testQuery a simple query to verify connectivity (e.g., "SELECT 1")
     */
    void requestBeConnectivityTest(byte[] serializedDescriptor, int connectionTypeValue,
            String testQuery);
}
