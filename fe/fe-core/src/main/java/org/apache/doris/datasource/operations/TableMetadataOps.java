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

package org.apache.doris.datasource.operations;

import org.apache.doris.catalog.Column;
import org.apache.doris.common.DdlException;

import java.util.List;

/**
 * This interface defines operations for managing table metadata, including:
 * - Setting or removing table properties
 * - Adding, deleting, and renaming columns
 * - Adding, deleting, and renaming nested fields
 * - Reordering top-level columns and nested struct fields
 * - Widening the type of int, float, and decimal fields
 * - Making required columns optional
 */
public interface TableMetadataOps {
    default void setTableProperty(String key, String value) throws DdlException {
        throw new UnsupportedOperationException("Set table property operation is not supported for this table type.");
    }

    default void addColumn(Column column) throws DdlException {
        throw new UnsupportedOperationException("Add column operation is not supported for this table type.");
    }

    default void addColumns(List<Column> columns) throws DdlException {
        throw new UnsupportedOperationException("Add columns operation is not supported for this table type.");
    }

    default void deleteColumn(String name) throws DdlException {
        throw new UnsupportedOperationException("Drop column operation is not supported for this table type.");
    }

    default void renameColumn(String oldName, String newName) throws DdlException {
        throw new UnsupportedOperationException("Rename column operation is not supported for this table type.");
    }

    default void updateColumn(Column column) throws DdlException {
        throw new UnsupportedOperationException("Update column operation is not supported for this table type.");
    }

    default void reorderColumn(List<String> newOrder) throws DdlException {
        throw new UnsupportedOperationException("Reorder column operation is not supported for this table type.");
    }
}
