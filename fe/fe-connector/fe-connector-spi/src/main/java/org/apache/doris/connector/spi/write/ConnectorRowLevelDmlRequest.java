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

import org.apache.doris.connector.spi.handle.WriteOperation;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

/** Connector-neutral facts needed to validate a row-level DML statement. */
public final class ConnectorRowLevelDmlRequest {
    private final WriteOperation operation;
    private final Set<String> updatedColumns;
    private final boolean containsUpdate;
    private final boolean containsDelete;

    public ConnectorRowLevelDmlRequest(WriteOperation operation, Set<String> updatedColumns,
            boolean containsUpdate, boolean containsDelete) {
        this.operation = operation;
        TreeSet<String> columns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        columns.addAll(updatedColumns);
        this.updatedColumns = Collections.unmodifiableSet(columns);
        this.containsUpdate = containsUpdate;
        this.containsDelete = containsDelete;
    }

    public WriteOperation getOperation() {
        return operation;
    }

    public Set<String> getUpdatedColumns() {
        return updatedColumns;
    }

    public boolean containsUpdate() {
        return containsUpdate;
    }

    public boolean containsDelete() {
        return containsDelete;
    }
}
