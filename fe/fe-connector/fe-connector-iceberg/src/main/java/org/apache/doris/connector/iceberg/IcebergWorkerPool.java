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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.iceberg.SystemConfigs;
import org.apache.iceberg.util.ThreadPools;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Initializes the SDK-owned worker pool once for all Iceberg catalogs in this FE. */
final class IcebergWorkerPool {
    private static final Logger LOG = LogManager.getLogger(IcebergWorkerPool.class);
    // Guarded by the class monitor. Catalog creation may race, but the plugin configuration is FE-wide.
    private static boolean initialized;

    private IcebergWorkerPool() {
    }

    static synchronized void initialize(ConnectorContext context) {
        if (initialized) {
            return;
        }
        Integer configuredSize = IcebergConf.workerThreadPoolSize(context);
        if (configuredSize != null) {
            // Keep the SDK's pool rather than adding a Doris-only planning pool: SDK-internal manifest
            // writes and scans must use the same limit. SystemConfigs caches this value on first access.
            System.setProperty(SystemConfigs.WORKER_THREAD_POOL_SIZE.propertyKey(), configuredSize.toString());
        }
        int actualSize = ThreadPools.WORKER_THREAD_POOL_SIZE;
        if (configuredSize != null && actualSize != configuredSize) {
            throw new IllegalStateException("Iceberg worker pool was initialized with " + actualSize
                    + " threads before iceberg.conf " + IcebergConf.CONF_WORKER_THREAD_POOL_SIZE
                    + "=" + configuredSize + " was applied; restart the FE to apply the limit");
        }
        LOG.info("Iceberg shared worker pool size: {} threads across all catalogs", actualSize);
        initialized = true;
    }
}
