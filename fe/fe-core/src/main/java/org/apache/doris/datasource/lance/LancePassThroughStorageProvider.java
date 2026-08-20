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

package org.apache.doris.datasource.lance;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Every provider Doris has no vocabulary of its own for - Azure, GCS, OSS, Tencent COS, local
 * files, and anything Lance adds later.
 *
 * <p>Both halves are deliberately inert. Doris cannot spell static credentials for these, so such
 * a dataset is reachable only through what its namespace vends, and those options are the
 * namespace's to name: Lance's OSS provider reads {@code access_key_id} and requires
 * {@code endpoint}, object_store's Azure parser reads {@code endpoint} and takes {@code token} as
 * a bearer token. Rewriting any of that onto the S3 spellings would leave the dataset unreachable,
 * which is exactly what this class exists to prevent.
 */
final class LancePassThroughStorageProvider implements LanceStorageProvider {

    static final LancePassThroughStorageProvider INSTANCE = new LancePassThroughStorageProvider();

    private LancePassThroughStorageProvider() {
    }

    @Override
    public Map<String, String> fromDorisProperties(Map<String, String> backendProperties) {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, String> normalizeVended(Map<String, String> vendedOptions) {
        return vendedOptions == null ? new HashMap<>() : new HashMap<>(vendedOptions);
    }
}
