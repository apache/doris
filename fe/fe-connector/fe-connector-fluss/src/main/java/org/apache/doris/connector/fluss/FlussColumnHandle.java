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

package org.apache.doris.connector.fluss;

import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;

import java.util.Objects;

/**
 * A fluss column: its name and its position in the table's row type.
 *
 * <p>The position is what the scan side needs — fluss projects by field index
 * ({@code TableScan.project(int[])}) and its readers hand back positional rows — so carrying it here
 * keeps the projection from having to re-derive it from the schema.
 */
public class FlussColumnHandle implements ConnectorColumnHandle {

    private static final long serialVersionUID = 1L;

    private final String name;
    private final int fieldIndex;

    public FlussColumnHandle(String name, int fieldIndex) {
        this.name = Objects.requireNonNull(name, "name");
        this.fieldIndex = fieldIndex;
    }

    public String getName() {
        return name;
    }

    public int getFieldIndex() {
        return fieldIndex;
    }

    /** Identity is the name and the position: the same name at a different index is a different column. */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FlussColumnHandle)) {
            return false;
        }
        FlussColumnHandle that = (FlussColumnHandle) o;
        return fieldIndex == that.fieldIndex && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, fieldIndex);
    }

    @Override
    public String toString() {
        return name + "[" + fieldIndex + "]";
    }
}
