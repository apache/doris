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

import org.apache.doris.connector.spi.ddl.BranchChange;
import org.apache.doris.connector.spi.ddl.DropRefChange;
import org.apache.doris.connector.spi.ddl.PartitionFieldChange;
import org.apache.doris.connector.spi.ddl.TagChange;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

/**
 * Snapshot references (branches and tags) and partition-spec evolution.
 *
 * <p><b>The whole domain is optional</b>, and only meaningful for a format that has a snapshot log with named
 * references and a mutable partition spec. Every default fails loud with a "not supported" message.</p>
 *
 * <p>Minimum implementation set: the two groups are independent, but each is all-or-nothing. Implement all
 * four of {@link #createOrReplaceBranch} / {@link #createOrReplaceTag} / {@link #dropBranch} /
 * {@link #dropTag} together — a half set lets a user create a branch that they then cannot drop. Likewise
 * implement {@link #addPartitionField} / {@link #dropPartitionField} / {@link #replacePartitionField}
 * together.</p>
 */
public interface ConnectorSnapshotRefOps {

    /** Creates or replaces a named branch (snapshot ref) on the table. */
    @ConnectorMustImplement(when = "the connector exposes branches and tags")
    default void createOrReplaceBranch(ConnectorSession session, ConnectorTableHandle handle,
            BranchChange branch) {
        throw new DorisConnectorException("CREATE/REPLACE BRANCH not supported");
    }

    /** Creates or replaces a named tag (snapshot ref) on the table. */
    @ConnectorMustImplement(when = "the connector exposes branches and tags")
    default void createOrReplaceTag(ConnectorSession session, ConnectorTableHandle handle,
            TagChange tag) {
        throw new DorisConnectorException("CREATE/REPLACE TAG not supported");
    }

    /** Drops a named branch (snapshot ref) from the table. */
    @ConnectorMustImplement(when = "the connector exposes branches and tags")
    default void dropBranch(ConnectorSession session, ConnectorTableHandle handle,
            DropRefChange branch) {
        throw new DorisConnectorException("DROP BRANCH not supported");
    }

    /** Drops a named tag (snapshot ref) from the table. */
    @ConnectorMustImplement(when = "the connector exposes branches and tags")
    default void dropTag(ConnectorSession session, ConnectorTableHandle handle,
            DropRefChange tag) {
        throw new DorisConnectorException("DROP TAG not supported");
    }

    /** Adds a partition field (column reference + optional transform) to the table's partition spec. */
    @ConnectorMustImplement(when = "the connector supports partition-spec evolution")
    default void addPartitionField(ConnectorSession session, ConnectorTableHandle handle,
            PartitionFieldChange change) {
        throw new DorisConnectorException("ADD PARTITION FIELD not supported");
    }

    /** Drops a partition field from the table's partition spec. */
    @ConnectorMustImplement(when = "the connector supports partition-spec evolution")
    default void dropPartitionField(ConnectorSession session, ConnectorTableHandle handle,
            PartitionFieldChange change) {
        throw new DorisConnectorException("DROP PARTITION FIELD not supported");
    }

    /** Replaces a partition field (removes the old field, adds the new one) in the table's partition spec. */
    @ConnectorMustImplement(when = "the connector supports partition-spec evolution")
    default void replacePartitionField(ConnectorSession session, ConnectorTableHandle handle,
            PartitionFieldChange change) {
        throw new DorisConnectorException("REPLACE PARTITION FIELD not supported");
    }
}
