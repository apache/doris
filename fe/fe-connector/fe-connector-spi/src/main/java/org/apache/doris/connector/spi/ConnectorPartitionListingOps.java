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

import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Enumerating a table's partitions.
 *
 * <p>Minimum implementation set: a connector whose tables can be partitioned implements
 * {@link #listPartitionNames} and {@link #listPartitions}. A connector without partitioned tables implements
 * nothing here and the empty defaults are correct for it.</p>
 *
 * <p>When supplying {@link ConnectorPartitionInfo} objects, note that the ordered partition values are
 * mandatory on the MVCC partition-item path — see that class for what silently happens when they are
 * omitted.</p>
 */
public interface ConnectorPartitionListingOps {

    /**
     * Lists all partition display names (e.g., {@code "year=2024/month=01"}).
     *
     * <p>Should be cheap and avoid loading per-partition metadata.</p>
     */
    @ConnectorMustImplement(when = "the connector has partitioned tables")
    default List<String> listPartitionNames(ConnectorSession session,
            ConnectorTableHandle handle) {
        return Collections.emptyList();
    }

    /**
     * Lists partitions matching the optional filter, with full metadata.
     *
     * <p>Connectors should push the filter into the metastore / catalog when
     * possible. {@code filter} is empty when the caller wants the full list.</p>
     */
    @ConnectorMustImplement(when = "the connector has partitioned tables")
    default List<ConnectorPartitionInfo> listPartitions(ConnectorSession session,
            ConnectorTableHandle handle,
            Optional<ConnectorExpression> filter) {
        return Collections.emptyList();
    }

}
