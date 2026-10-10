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

package org.apache.doris.datasource.plugin;

import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.write.ConnectorWriteDistribution;
import org.apache.doris.connector.spi.write.ConnectorWritePlanProvider;

import java.util.Optional;

/** Connector objects and write traits resolved together for one physical table sink. */
public class ConnectorWritePlanContext {
    private final ConnectorSession session;
    private final ConnectorMetadata metadata;
    private final ConnectorTableHandle tableHandle;
    private final ConnectorWritePlanProvider provider;
    private final ConnectorWriteDistribution distribution;
    private final boolean requiresParallelWrite;
    private final boolean requiresPartitionLocalSort;
    private final boolean requiresPartitionHashWrite;
    private final boolean requiresFullSchemaWriteOrder;

    /** Creates an immutable context from one connector-provider resolution. */
    public ConnectorWritePlanContext(ConnectorSession session, ConnectorMetadata metadata,
            ConnectorTableHandle tableHandle, ConnectorWritePlanProvider provider,
            ConnectorWriteDistribution distribution, boolean requiresParallelWrite,
            boolean requiresPartitionLocalSort, boolean requiresPartitionHashWrite,
            boolean requiresFullSchemaWriteOrder) {
        this.session = session;
        this.metadata = metadata;
        this.tableHandle = tableHandle;
        this.provider = provider;
        this.distribution = distribution;
        this.requiresParallelWrite = requiresParallelWrite;
        this.requiresPartitionLocalSort = requiresPartitionLocalSort;
        this.requiresPartitionHashWrite = requiresPartitionHashWrite;
        this.requiresFullSchemaWriteOrder = requiresFullSchemaWriteOrder;
    }

    public ConnectorSession getSession() {
        return session;
    }

    public ConnectorMetadata getMetadata() {
        return metadata;
    }

    public ConnectorTableHandle getTableHandle() {
        return tableHandle;
    }

    public ConnectorWritePlanProvider getProvider() {
        return provider;
    }

    public Optional<ConnectorWriteDistribution> getDistribution() {
        return Optional.ofNullable(distribution);
    }

    public boolean requiresParallelWrite() {
        return requiresParallelWrite;
    }

    public boolean requiresPartitionLocalSort() {
        return requiresPartitionLocalSort;
    }

    public boolean requiresPartitionHashWrite() {
        return requiresPartitionHashWrite;
    }

    public boolean requiresFullSchemaWriteOrder() {
        return requiresFullSchemaWriteOrder;
    }
}
