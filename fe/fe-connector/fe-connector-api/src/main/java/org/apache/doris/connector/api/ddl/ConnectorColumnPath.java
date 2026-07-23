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

package org.apache.doris.connector.api.ddl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The dotted column path targeted by an {@code ALTER TABLE ADD/DROP/RENAME/MODIFY COLUMN} clause,
 * carried neutrally across the SPI by the {@code ConnectorColumnPath} column-DDL overloads on
 * {@link org.apache.doris.connector.api.ConnectorTableOps}.
 *
 * <p>Faithful, lossless neutralization of the fe-core {@code org.apache.doris.analysis.ColumnPath}
 * (an ordered, non-empty list of identifier parts). A single-part path targets a top-level column;
 * a multi-part path ({@link #isNested()}) targets a nested struct field, or a collection pseudo-field
 * such as {@code arr.element} / {@code m.value}. The connector taking a compile-time dependency on
 * fe-core types is forbidden by the iron law, so the SPI passes this DTO instead.</p>
 */
public final class ConnectorColumnPath {

    private final List<String> parts;

    private ConnectorColumnPath(List<String> parts) {
        if (parts == null || parts.isEmpty()) {
            throw new IllegalArgumentException("column path is empty");
        }
        for (String part : parts) {
            if (part == null || part.isEmpty()) {
                throw new IllegalArgumentException("column path contains empty part");
            }
        }
        this.parts = Collections.unmodifiableList(new ArrayList<>(parts));
    }

    public static ConnectorColumnPath of(List<String> parts) {
        return new ConnectorColumnPath(parts);
    }

    public static ConnectorColumnPath of(String name) {
        return new ConnectorColumnPath(Collections.singletonList(name));
    }

    /** The ordered identifier parts. Never empty. */
    public List<String> getParts() {
        return parts;
    }

    /** Whether the path targets a nested field (more than one part) rather than a top-level column. */
    public boolean isNested() {
        return parts.size() > 1;
    }

    /** The top-level (first) part. */
    public String getTopLevelName() {
        return parts.get(0);
    }

    /** The leaf (last) part. */
    public String getLeafName() {
        return parts.get(parts.size() - 1);
    }

    /**
     * The parent path (all parts but the last). Only meaningful when {@link #isNested()}.
     *
     * @throws IllegalStateException if this is a top-level path.
     */
    public ConnectorColumnPath getParentPath() {
        if (!isNested()) {
            throw new IllegalStateException("top-level column path has no parent");
        }
        return new ConnectorColumnPath(parts.subList(0, parts.size() - 1));
    }

    /** The dotted string form, e.g. {@code "s.b"} / {@code "arr.element"}. */
    public String getFullPath() {
        return String.join(".", parts);
    }

    @Override
    public String toString() {
        return getFullPath();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorColumnPath)) {
            return false;
        }
        return parts.equals(((ConnectorColumnPath) o).parts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parts);
    }
}
