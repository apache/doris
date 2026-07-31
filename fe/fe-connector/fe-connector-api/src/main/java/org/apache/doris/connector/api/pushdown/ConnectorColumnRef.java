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

package org.apache.doris.connector.api.pushdown;

import org.apache.doris.connector.api.ConnectorType;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A column reference expression.
 */
public final class ConnectorColumnRef implements ConnectorExpression {

    private static final long serialVersionUID = 1L;

    private final String columnName;
    private final ConnectorType type;

    public ConnectorColumnRef(String columnName, ConnectorType type) {
        this.columnName = Objects.requireNonNull(columnName, "columnName");
        this.type = Objects.requireNonNull(type, "type");
    }

    public String getColumnName() {
        return columnName;
    }

    public ConnectorType getType() {
        return type;
    }

    @Override
    public List<ConnectorExpression> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public String toString() {
        return columnName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorColumnRef)) {
            return false;
        }
        ConnectorColumnRef that = (ConnectorColumnRef) o;
        return columnName.equals(that.columnName) && type.equals(that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, type);
    }
}
