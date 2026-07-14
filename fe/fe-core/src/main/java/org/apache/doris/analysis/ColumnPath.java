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

import org.apache.doris.common.util.SqlUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a column path used by schema change statements.
 */
public class ColumnPath {
    private final ImmutableList<String> parts;

    private ColumnPath(List<String> parts) {
        Preconditions.checkArgument(parts != null && !parts.isEmpty(), "column path is empty");
        for (String part : parts) {
            Preconditions.checkArgument(part != null && !part.isEmpty(), "column path contains empty part");
        }
        this.parts = ImmutableList.copyOf(parts);
    }

    public static ColumnPath of(List<String> parts) {
        return new ColumnPath(parts);
    }

    public static ColumnPath of(String name) {
        return new ColumnPath(ImmutableList.of(name));
    }

    public static ColumnPath fromDotName(String name) {
        return new ColumnPath(Arrays.asList(name.split("\\.")));
    }

    public List<String> getParts() {
        return parts;
    }

    public boolean isNested() {
        return parts.size() > 1;
    }

    public String getTopLevelName() {
        return parts.get(0);
    }

    public String getLeafName() {
        return parts.get(parts.size() - 1);
    }

    public ColumnPath getParentPath() {
        Preconditions.checkState(isNested(), "top-level column path has no parent");
        return new ColumnPath(parts.subList(0, parts.size() - 1));
    }

    public String getParentPathString() {
        return getParentPath().getFullPath();
    }

    public String getFullPath() {
        return String.join(".", parts);
    }

    public String toSql() {
        return parts.stream().map(SqlUtils::getIdentSql).collect(Collectors.joining("."));
    }

    @Override
    public String toString() {
        return getFullPath();
    }
}
