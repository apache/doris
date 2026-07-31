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

package org.apache.doris.filesystem.properties;

import java.util.Map;

/**
 * Storage properties that can be passed to BE through an RPC adapter.
 *
 * <p>This interface deliberately exposes only a neutral key-value form. The API
 * module should not depend on Thrift or any other RPC framework; fe-core adapters
 * are responsible for converting the map to concrete RPC structures.</p>
 */
public interface BackendStorageProperties {

    /**
     * Returns the backend storage kind used to choose the corresponding BE RPC adapter.
     */
    BackendStorageKind backendKind();

    /**
     * Converts to a neutral key-value representation. Adapters in fe-core are responsible
     * for creating RPC-specific structures such as TS3StorageParam.
     *
     * @return storage parameters using names understood by the corresponding adapter
     */
    Map<String, String> toMap();
}
