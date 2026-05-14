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

package org.apache.doris.connector.api;

import org.apache.doris.connector.api.handle.ConnectorDeleteHandle;
import org.apache.doris.connector.api.handle.ConnectorInsertHandle;
import org.apache.doris.connector.api.handle.ConnectorMergeHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.write.ConnectorWriteConfig;

import java.util.Collection;
import java.util.List;

/**
 * Write (DML) operations that a connector may support.
 *
 * <p>Follows a two-phase lifecycle for each write operation:</p>
 * <ol>
 *   <li>{@code begin*} — initialize the write, return an opaque handle</li>
 *   <li>{@code finish*} — commit using collected BE fragments; or {@code abort*} on failure</li>
 * </ol>
 *
 * <p>All methods have default implementations that throw
 * {@link DorisConnectorException}, so connectors only override what they support.</p>
 */
public interface ConnectorWriteOps {

    // ──────────────────── Capability Queries ────────────────────

    /** Returns {@code true} if this connector supports INSERT operations. */
    default boolean supportsInsert() {
        return false;
    }

    /** Returns {@code true} if this connector supports DELETE operations. */
    default boolean supportsDelete() {
        return false;
    }

    /** Returns {@code true} if this connector supports MERGE (INSERT + DELETE) operations. */
    default boolean supportsMerge() {
        return false;
    }

    // ──────────────────── Write Configuration ────────────────────

    /**
     * Returns the write configuration for this table.
     *
     * <p>The engine uses the returned {@link ConnectorWriteConfig} to select the
     * appropriate Thrift data sink type and pass properties to BE.</p>
     *
     * @param session current session
     * @param handle the target table handle
     * @param columns the columns being written (ordered to match INSERT column list)
     * @return write configuration describing sink type, format, location, etc.
     */
    default ConnectorWriteConfig getWriteConfig(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumn> columns) {
        throw new DorisConnectorException("Write not supported");
    }

    // ──────────────────── INSERT ────────────────────

    /**
     * Begins an insert operation and returns an opaque handle.
     *
     * @param session current session
     * @param handle the target table handle
     * @param columns the columns being inserted (ordered to match INSERT column list)
     * @return an opaque insert handle carrying connector-internal state
     */
    default ConnectorInsertHandle beginInsert(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumn> columns) {
        throw new DorisConnectorException("INSERT not supported");
    }

    /**
     * Commits the insert operation using collected fragments from BE.
     *
     * @param session current session
     * @param handle the insert handle from {@link #beginInsert}
     * @param fragments serialized commit info collected from BE nodes
     */
    default void finishInsert(ConnectorSession session,
            ConnectorInsertHandle handle,
            Collection<byte[]> fragments) {
        throw new DorisConnectorException("INSERT not supported");
    }

    /**
     * Aborts a previously started insert operation.
     * Called on failure to clean up any partial writes.
     *
     * @param session current session
     * @param handle the insert handle from {@link #beginInsert}
     */
    default void abortInsert(ConnectorSession session,
            ConnectorInsertHandle handle) {
        // default: no-op — connector may not require explicit cleanup
    }

    // ──────────────────── DELETE ────────────────────

    /**
     * Begins a delete operation and returns an opaque handle.
     *
     * @param session current session
     * @param handle the target table handle
     * @return an opaque delete handle
     */
    default ConnectorDeleteHandle beginDelete(
            ConnectorSession session,
            ConnectorTableHandle handle) {
        throw new DorisConnectorException("DELETE not supported");
    }

    /**
     * Commits the delete operation using collected fragments.
     *
     * @param session current session
     * @param handle the delete handle from {@link #beginDelete}
     * @param fragments serialized commit info collected from BE nodes
     */
    default void finishDelete(ConnectorSession session,
            ConnectorDeleteHandle handle,
            Collection<byte[]> fragments) {
        throw new DorisConnectorException("DELETE not supported");
    }

    /**
     * Aborts a previously started delete operation.
     *
     * @param session current session
     * @param handle the delete handle from {@link #beginDelete}
     */
    default void abortDelete(ConnectorSession session,
            ConnectorDeleteHandle handle) {
        // default: no-op
    }

    // ──────────────────── MERGE (INSERT + DELETE) ────────────────────

    /**
     * Begins a merge (combined insert+delete) operation.
     * Used by connectors that support merge-on-read (e.g., Iceberg).
     *
     * @param session current session
     * @param handle the target table handle
     * @return an opaque merge handle
     */
    default ConnectorMergeHandle beginMerge(
            ConnectorSession session,
            ConnectorTableHandle handle) {
        throw new DorisConnectorException("MERGE not supported");
    }

    /**
     * Commits the merge operation using collected fragments.
     *
     * @param session current session
     * @param handle the merge handle from {@link #beginMerge}
     * @param fragments serialized commit info collected from BE nodes
     */
    default void finishMerge(ConnectorSession session,
            ConnectorMergeHandle handle,
            Collection<byte[]> fragments) {
        throw new DorisConnectorException("MERGE not supported");
    }

    /**
     * Aborts a previously started merge operation.
     *
     * @param session current session
     * @param handle the merge handle from {@link #beginMerge}
     */
    default void abortMerge(ConnectorSession session,
            ConnectorMergeHandle handle) {
        // default: no-op
    }
}
