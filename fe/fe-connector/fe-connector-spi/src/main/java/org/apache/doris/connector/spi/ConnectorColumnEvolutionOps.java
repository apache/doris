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

import org.apache.doris.connector.spi.ddl.ConnectorColumnPath;
import org.apache.doris.connector.spi.ddl.ConnectorColumnPosition;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

import java.util.List;

/**
 * Column schema evolution: {@code ALTER TABLE ... ADD / DROP / RENAME / MODIFY COLUMN}, flat and nested.
 *
 * <p><b>The whole domain is optional</b> — every default fails loud with a "not supported" message, which is
 * the correct answer for a connector whose format cannot evolve columns.</p>
 *
 * <p>Minimum implementation set:</p>
 * <ul>
 * <li><b>Flat column evolution</b>: implement the six top-level ops as a GROUP —
 *     {@link #addColumn}, {@link #addColumns}, {@link #dropColumn}, {@link #renameColumn},
 *     {@link #modifyColumn}, {@link #reorderColumns}. Both connectors that support column evolution
 *     implement all six; a partial set means some {@code ALTER} statements succeed on a table while others
 *     report "not supported" on the same table.</li>
 * <li><b>Nested column evolution</b>: the four {@code *NestedColumn} ops plus
 *     {@link #modifyColumnComment}, only for a format that can evolve fields inside a struct.</li>
 * </ul>
 *
 * <p>A heterogeneous gateway connector must route BOTH groups to its sibling. Routing only the flat six is a
 * silent trap: nested {@code ALTER} statements then hit the gateway's own throwing default even though the
 * sibling supports them.</p>
 *
 * <p>Dotted column paths (e.g. {@code s.b}, {@code arr.element.c}, {@code m.value}) are carried neutrally by
 * {@link ConnectorColumnPath}; a single-part path targets a top-level column. The fe-core bridge routes
 * top-level ADD/DROP/RENAME/MODIFY through the flat ops and reserves the {@code *NestedColumn} ops for the
 * nested case, except {@link #modifyColumnComment}, which is the sole entrypoint for
 * {@code MODIFY COLUMN ... COMMENT} (flat and nested alike). Distinct names (rather than overloads of the flat
 * {@code String} / {@link ConnectorColumn} ops) keep {@code Mockito.any()} / null call sites in connector tests
 * unambiguous.</p>
 */
public interface ConnectorColumnEvolutionOps {

    /**
     * Adds a column to the table at the given position.
     *
     * @param position where to place the column ({@link ConnectorColumnPosition#FIRST} /
     *        {@link ConnectorColumnPosition#after(String)}); {@code null} appends at the end.
     */
    @ConnectorMustImplement(when = "the connector supports column evolution")
    default void addColumn(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumn column, ConnectorColumnPosition position) {
        throw new DorisConnectorException("ADD COLUMN not supported");
    }

    /** Adds multiple columns to the table, appended in order. */
    @ConnectorMustImplement(when = "the connector supports column evolution")
    default void addColumns(ConnectorSession session, ConnectorTableHandle handle,
            List<ConnectorColumn> columns) {
        throw new DorisConnectorException("ADD COLUMNS not supported");
    }

    /** Drops the named column from the table. */
    @ConnectorMustImplement(when = "the connector supports column evolution")
    default void dropColumn(ConnectorSession session, ConnectorTableHandle handle,
            String columnName) {
        throw new DorisConnectorException("DROP COLUMN not supported");
    }

    /** Renames a column. */
    @ConnectorMustImplement(when = "the connector supports column evolution")
    default void renameColumn(ConnectorSession session, ConnectorTableHandle handle,
            String oldName, String newName) {
        throw new DorisConnectorException("RENAME COLUMN not supported");
    }

    /**
     * Modifies a column's type and/or comment, optionally repositioning it.
     *
     * @param position where to move the column; {@code null} keeps its current position.
     */
    @ConnectorMustImplement(when = "the connector supports column evolution")
    default void modifyColumn(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumn column, ConnectorColumnPosition position) {
        throw new DorisConnectorException("MODIFY COLUMN not supported");
    }

    /** Reorders the table's columns to match the given full ordered list of column names. */
    @ConnectorMustImplement(when = "the connector supports column evolution")
    default void reorderColumns(ConnectorSession session, ConnectorTableHandle handle,
            List<String> newOrder) {
        throw new DorisConnectorException("REORDER COLUMNS not supported");
    }

    /**
     * Adds a field at {@code path} (the full path of the new field: its parent struct plus the new leaf name).
     *
     * @param position where to place the new field within its parent struct; {@code null} appends at the end.
     */
    @ConnectorMustImplement(when = "the connector supports nested column evolution")
    default void addNestedColumn(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumnPath path, ConnectorColumn column, ConnectorColumnPosition position) {
        throw new DorisConnectorException("nested ADD COLUMN not supported");
    }

    /** Drops the field at {@code path}. */
    @ConnectorMustImplement(when = "the connector supports nested column evolution")
    default void dropNestedColumn(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumnPath path) {
        throw new DorisConnectorException("nested DROP COLUMN not supported");
    }

    /** Renames the field at {@code path} to {@code newName} (a leaf name, not a path). */
    @ConnectorMustImplement(when = "the connector supports nested column evolution")
    default void renameNestedColumn(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumnPath path, String newName) {
        throw new DorisConnectorException("nested RENAME COLUMN not supported");
    }

    /**
     * Modifies the field at {@code path} (type / comment / nullability), optionally repositioning it within
     * its parent struct.
     *
     * @param position where to move the field; {@code null} keeps its current position.
     */
    @ConnectorMustImplement(when = "the connector supports nested column evolution")
    default void modifyNestedColumn(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumnPath path, ConnectorColumn column, ConnectorColumnPosition position) {
        throw new DorisConnectorException("nested MODIFY COLUMN not supported");
    }

    /** Sets (or clears, with {@code ""}) the comment/doc of the field at {@code path}. */
    @ConnectorMustImplement(when = "the connector supports nested column evolution")
    default void modifyColumnComment(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumnPath path, String comment) {
        throw new DorisConnectorException("MODIFY COLUMN COMMENT not supported");
    }
}
