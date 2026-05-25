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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A single field in a {@link ConnectorPartitionSpec}.
 *
 * <p>The {@code transform} string follows Appendix B of the connector SPI RFC:
 * {@code identity / year / month / day / hour / bucket / truncate / list / range}.
 * Unlisted values are treated as {@code CUSTOM} and interpreted by the connector.</p>
 *
 * <p>{@code transformArgs} carries numeric parameters (e.g., {@code [16]} for
 * {@code bucket(16, col)} or {@code [10]} for {@code truncate(10, col)}).</p>
 */
public final class ConnectorPartitionField {

    private final String columnName;
    private final String transform;
    private final List<Integer> transformArgs;

    public ConnectorPartitionField(String columnName, String transform,
            List<Integer> transformArgs) {
        this.columnName = Objects.requireNonNull(columnName, "columnName");
        this.transform = Objects.requireNonNull(transform, "transform");
        this.transformArgs = transformArgs == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(transformArgs);
    }

    public String getColumnName() {
        return columnName;
    }

    public String getTransform() {
        return transform;
    }

    public List<Integer> getTransformArgs() {
        return transformArgs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorPartitionField)) {
            return false;
        }
        ConnectorPartitionField that = (ConnectorPartitionField) o;
        return columnName.equals(that.columnName)
                && transform.equals(that.transform)
                && transformArgs.equals(that.transformArgs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, transform, transformArgs);
    }

    @Override
    public String toString() {
        if (transformArgs.isEmpty()) {
            return transform + "(" + columnName + ")";
        }
        return transform + transformArgs + "(" + columnName + ")";
    }
}
