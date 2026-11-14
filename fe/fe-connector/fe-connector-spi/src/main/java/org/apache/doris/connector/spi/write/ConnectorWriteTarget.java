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

package org.apache.doris.connector.spi.write;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

import java.util.List;
import java.util.Optional;

/**
 * Immutable statement-bound writer metadata, separate from source snapshot and commit constraints.
 *
 * <p>Connectors implement this interface to retain their native writer metadata. The engine only reads
 * this neutral view and passes the same target back to the provider; it never serializes it to BE.
 * Resource ownership remains with the connector statement scope and transaction.</p>
 */
public interface ConnectorWriteTarget {
    ConnectorTableHandle getTableHandle();

    Optional<String> getBranchName();

    List<ConnectorColumn> getColumns();

    List<String> getPartitionColumnNames();

    /** Opaque metadata-generation fence, or null when the connector has no such fence. */
    String getMetadataIdentity();
}
