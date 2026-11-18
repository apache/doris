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

package org.apache.doris.datasource.scan;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.Env;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorVectorSearch;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenMetadata;

import java.util.Optional;

/** The shared plugin boundary used by SQL TopN and an explicit vector-search TVF. */
public final class ConnectorVectorSearchPlanner {
    private ConnectorVectorSearchPlanner() {
    }

    public static Optional<ConnectorVectorSearch> prepare(PluginDrivenExternalTable table,
            Optional<TableSnapshot> snapshot, Optional<TableScanParams> scanParams, ConnectorVectorSearch search) {
        PluginDrivenExternalCatalog catalog = (PluginDrivenExternalCatalog) table.getCatalog();
        Connector connector = catalog.getConnector();
        ConnectorSession session = catalog.buildConnectorSession();
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(connector.getClass().getClassLoader());
            ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
            return table.observeConnectorMetadata(session, metadata).execute(() -> {
                ConnectorTableHandle handle = table.resolveConnectorTableHandle(session, metadata)
                        .orElseThrow(() -> new IllegalArgumentException("Cannot resolve table " + table.getName()));
                handle = PluginDrivenScanNode.applyMvccSnapshotPin(metadata, session, handle,
                        MvccUtil.getSnapshotFromContext(table, snapshot, scanParams));
                ConnectorScanPlanProvider provider = connector.getScanPlanProvider(handle);
                if (provider == null) {
                    return Optional.empty();
                }
                ClassLoader beforeProvider = Thread.currentThread().getContextClassLoader();
                try {
                    Thread.currentThread().setContextClassLoader(provider.getClass().getClassLoader());
                    return provider.prepareVectorSearch(session, handle, search,
                            PluginDrivenScanNode.commonBackendCapabilities(
                                    Env.getCurrentSystemInfo().getAllClusterBackends(true)));
                } finally {
                    Thread.currentThread().setContextClassLoader(beforeProvider);
                }
            });
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }
}
