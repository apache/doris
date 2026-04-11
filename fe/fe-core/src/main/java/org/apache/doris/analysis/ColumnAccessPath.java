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

package org.apache.doris.analysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Java equivalent of the Thrift {@code TColumnAccessPath} struct.
 * Merges {@code TDataAccessPath} and {@code TMetaAccessPath} into a single class
 * since both are structurally identical ({@code List<String> path}).
 * The {@link ColumnAccessPathType} discriminates between data and metadata access.
 *
 * <p>This class is immutable and implements {@link Comparable} so it can be used
 * in sorted collections such as {@code TreeSet}.
 */
public final class ColumnAccessPath implements Comparable<ColumnAccessPath> {
    private final ColumnAccessPathType type;
    private final List<String> path;

    public ColumnAccessPath(ColumnAccessPathType type, List<String> path) {
        this.type = Objects.requireNonNull(type, "type must not be null");
        this.path = Collections.unmodifiableList(new ArrayList<>(
                Objects.requireNonNull(path, "path must not be null")));
    }

    /** Creates a DATA access path. */
    public static ColumnAccessPath data(List<String> path) {
        return new ColumnAccessPath(ColumnAccessPathType.DATA, path);
    }

    /** Creates a META access path. */
    public static ColumnAccessPath meta(List<String> path) {
        return new ColumnAccessPath(ColumnAccessPathType.META, path);
    }

    public ColumnAccessPathType getType() {
        return type;
    }

    public List<String> getPath() {
        return path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ColumnAccessPath)) {
            return false;
        }
        ColumnAccessPath that = (ColumnAccessPath) o;
        return type == that.type && Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, path);
    }

    @Override
    public int compareTo(ColumnAccessPath other) {
        int cmp = type.compareTo(other.type);
        if (cmp != 0) {
            return cmp;
        }
        int minLen = Math.min(path.size(), other.path.size());
        for (int i = 0; i < minLen; i++) {
            cmp = path.get(i).compareTo(other.path.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(path.size(), other.path.size());
    }

    @Override
    public String toString() {
        return type + ":" + String.join(".", path);
    }
}
