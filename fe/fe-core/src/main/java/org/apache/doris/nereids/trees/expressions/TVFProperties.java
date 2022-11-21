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
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** TVFProperties */
public class TVFProperties extends Expression implements LeafExpression {
    private final Map<String, String> keyValues;

    public TVFProperties(Map<String, String> properties) {
        super();
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
        return MapType.INSTANCE;
    }

    @Override
    public String toSql() {
        return getMap()
                .entrySet()
                .stream()
                .map(kv -> "'" + kv.getKey() + "' = '" + kv.getValue() + "'")
                .collect(Collectors.joining(", "));
    }

    @Override
    public String toString() {
        return "TVFProperties(" + toSql() + ")";
    }
}
