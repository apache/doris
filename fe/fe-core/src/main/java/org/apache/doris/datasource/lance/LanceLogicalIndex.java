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

package org.apache.doris.datasource.lance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Immutable logical index metadata read from one Lance dataset snapshot. */
public final class LanceLogicalIndex {
    private final String name;
    private final List<String> columns;
    private final String indexType;
    private final String properties;

    public LanceLogicalIndex(String name, List<String> columns,
            String indexType, String properties) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        List<String> columnCopy = new ArrayList<>(
                Objects.requireNonNull(columns, "columns must not be null"));
        for (String column : columnCopy) {
            Objects.requireNonNull(column, "column must not be null");
        }
        this.columns = Collections.unmodifiableList(columnCopy);
        this.indexType = Objects.requireNonNull(indexType, "indexType must not be null");
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
    }

    public String getName() {
        return name;
    }

    public List<String> getColumns() {
        return columns;
    }

    public String getIndexType() {
        return indexType;
    }

    public String getProperties() {
        return properties;
    }
}
