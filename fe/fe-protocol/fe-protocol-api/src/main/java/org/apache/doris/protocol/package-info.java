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

/**
 * Protocol SPI API Package.
 *
 * <h2>Overview</h2>
 * <p>This package defines the Service Provider Interface (SPI) for database
 * protocol implementations in Apache Doris. The architecture allows multiple
 * protocols (MySQL, Arrow Flight SQL, PostgreSQL, etc.) to coexist and be
 * loaded dynamically.
 *
 * <h2>Core Interfaces</h2>
 * <ul>
 *   <li>{@link org.apache.doris.protocol.ProtocolHandler} - Main protocol handler SPI</li>
 *   <li>{@link org.apache.doris.protocol.ProtocolConfig} - Protocol configuration</li>
 *   <li>{@link org.apache.doris.protocol.ProtocolContext} - Connection context</li>
 *   <li>{@link org.apache.doris.protocol.ProtocolLoader} - SPI loader utility</li>
 * </ul>
 *
 * <h2>Design Principles</h2>
 * <ol>
 *   <li><strong>Protocol Independence</strong>: Each protocol is an independent module</li>
 *   <li><strong>Kernel Decoupling</strong>: The kernel only depends on SPI interfaces</li>
 *   <li><strong>Extensibility</strong>: New protocols can be added via SPI</li>
 *   <li><strong>Backward Compatibility</strong>: Existing protocol behavior is preserved</li>
 * </ol>
 *
 * <h2>Module Structure</h2>
 * <pre>
 * fe-protocol/
 * ├── fe-protocol-api/        # SPI interfaces (this package)
 * ├── fe-protocol-mysql/      # MySQL protocol implementation
 * └── fe-protocol-arrowflight/ # Arrow Flight SQL implementation
 * </pre>
 *
 * <h2>Adding a New Protocol</h2>
 * <ol>
 *   <li>Create a new module under fe-protocol/</li>
 *   <li>Implement {@link org.apache.doris.protocol.ProtocolHandler}</li>
 *   <li>Register in META-INF/services/org.apache.doris.protocol.ProtocolHandler</li>
 *   <li>Add module dependency to fe-core</li>
 * </ol>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * // In QeService (kernel)
 * ProtocolConfig config = new ProtocolConfig(mysqlPort, arrowFlightPort, scheduler);
 * List<ProtocolHandler> handlers = ProtocolLoader.loadConfiguredProtocols(config);
 *
 * for (ProtocolHandler handler : handlers) {
 *     handler.setAcceptor(this::handleConnection);
 *     handler.start();
 * }
 * }</pre>
 */
package org.apache.doris.protocol;
