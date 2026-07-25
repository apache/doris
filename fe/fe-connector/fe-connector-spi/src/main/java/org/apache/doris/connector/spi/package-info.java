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
 * The plugin entry point, plus the engine services a connector CONSUMES.
 *
 * <p>Two kinds of type live here, and the difference is who implements them:</p>
 *
 * <ul>
 * <li>{@link ConnectorProvider} — <b>implemented by the connector</b>. It is the single entry point the
 *     engine discovers, via Java ServiceLoader or directory-based plugin loading; a connector registers it
 *     in {@code META-INF/services}.</li>
 * <li>{@link ConnectorContext}, {@link ConnectorBrokerAddress} — <b>implemented (or produced) by the
 *     engine</b> and handed to the connector: the storage, filesystem and authentication services a plugin
 *     may use without depending on the engine.</li>
 * </ul>
 *
 * <p><b>This package is NOT where the interfaces a connector implements live.</b> Apart from
 * {@link ConnectorProvider}, essentially everything a connector implements — {@code Connector},
 * {@code ConnectorMetadata} and its sub-interfaces, the scan / write / procedure providers, the handle and
 * value types, the capability enum — is in {@code fe-connector-api}, which this module depends on and
 * therefore brings in transitively. <b>The design rules for the whole connector surface (where to declare a
 * capability, which exception to throw, where thrift is allowed, object lifetimes and threading) are written
 * down once, in that module's {@code org.apache.doris.connector.api} package documentation. Read it
 * first.</b></p>
 *
 * <p>Note that the two module names are inverted relative to common usage, where "SPI" names what a plugin
 * implements and "API" names the engine services it consumes. Here {@code fe-connector-spi} is the smaller
 * module and holds mostly engine-provided services. The names are deliberately not being changed; read the
 * content rather than the name.</p>
 *
 * <p>Types in this package deliberately avoid thrift even for data that ends up at BE (an enum NAME as a
 * {@code String}, a neutral broker-address type, a thrift enum's integer VALUE as an {@code int}). Treat that
 * as a local convention of this module, not as a guarantee that the connector surface is thrift-free: this
 * module depends on {@code fe-connector-api}, which does use thrift types at the BE protocol boundary.</p>
 */
package org.apache.doris.connector.spi;
