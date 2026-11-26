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
 * fe-core's storage facade over the fe-filesystem SPI. Map of this package:
 *
 * <ul>
 *   <li><b>{@link org.apache.doris.datasource.storage.StorageRegistry}</b> — THE extension
 *       surface: built-in provider bind order + type ids, and the uri-scheme routing table.
 *       Integrating a new storage system? Read its class javadoc first; an S3-compatible
 *       plugin needs no fe-core change at all.</li>
 *   <li>{@link org.apache.doris.datasource.storage.StorageTypeId} — plain type identifier
 *       (successor of the legacy typed enum); carries no behavior.</li>
 *   <li>{@link org.apache.doris.datasource.storage.StorageAdapter} — the consumer facade over
 *       one SPI binding (backend map, hadoop conf, uri validation, credentials). Consumers
 *       depend on it; providers never do.</li>
 *   <li>{@link org.apache.doris.datasource.storage.S3ThriftAdapter} /
 *       {@link org.apache.doris.datasource.storage.CloudObjectStoreAdapter} — frozen wire glue
 *       (BE thrift / cloud meta-service PB) over persisted user-namespace maps. Byte-parity
 *       locked by golden tests; not extension surface.</li>
 *   <li>{@link org.apache.doris.datasource.storage.S3ResourceCompat} — legacy quarantine for
 *       the frozen cold-storage Resource entities; dies with them. Never consulted by
 *       routing or plugins.</li>
 *   <li>{@link org.apache.doris.datasource.storage.StorageUriUtils} — verbatim port of the
 *       legacy uri normalization the SPI does not implement; facade-internal.</li>
 * </ul>
 */
package org.apache.doris.datasource.storage;
