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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;

import com.google.common.collect.ImmutableList;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Properties
 */
public class Properties extends Expression implements LeafExpression {

    private final Map<String, String> keyValues;

    public Properties(Map<String, String> properties) {
        super(ImmutableList.of());
        this.keyValues = Objects.requireNonNull(properties, "properties can not be null");
    }

    public Map<String, String> getMap() {
        return keyValues;
    }

    @Override
    public boolean nullable() {
        return false;
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return MapType.SYSTEM_DEFAULT;
    }

    @Override
    public String computeToSql() {
        return getMap()
                .entrySet()
                .stream()
                .map(kv -> "'" + kv.getKey() + "' = '" + kv.getValue() + "'")
                .collect(Collectors.joining(", "));
    }

    @Override
    public String toString() {
        return "Properties(" + toSql() + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        Properties that = (Properties) o;
        return Objects.equals(keyValues, that.keyValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), keyValues);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitProperties(this, context);
    }
}
