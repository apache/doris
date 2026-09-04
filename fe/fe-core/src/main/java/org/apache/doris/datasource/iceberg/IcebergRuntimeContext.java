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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

/** Immutable execution state retained together with one Iceberg table generation. */
public final class IcebergRuntimeContext {
    private final ExecutionAuthenticator authenticator;
    private final ThreadPoolExecutor planningExecutor;
    private final MetastoreProperties metastoreProperties;
    private final Map<StorageProperties.Type, StorageProperties> storageProperties;

    IcebergRuntimeContext(ExecutionAuthenticator authenticator, ThreadPoolExecutor planningExecutor,
            MetastoreProperties metastoreProperties,
            Map<StorageProperties.Type, StorageProperties> storageProperties) {
        this.authenticator = authenticator;
        this.planningExecutor = planningExecutor;
        this.metastoreProperties = metastoreProperties;
        this.storageProperties = ImmutableMap.copyOf(storageProperties);
    }

    public ExecutionAuthenticator getAuthenticator() {
        return authenticator;
    }

    public ThreadPoolExecutor getPlanningExecutor() {
        return planningExecutor;
    }

    public MetastoreProperties getMetastoreProperties() {
        return metastoreProperties;
    }

    public Map<StorageProperties.Type, StorageProperties> getStorageProperties() {
        return storageProperties;
    }
}
