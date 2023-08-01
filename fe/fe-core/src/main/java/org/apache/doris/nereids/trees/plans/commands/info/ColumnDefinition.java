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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VarcharType;

import java.util.Optional;
import java.util.Set;

/**
 * column definition
 * TODO: complex types will not work, we will support them later.
 */
public class ColumnDefinition {
    private final String name;
    private DataType type;
    private boolean isKey;
    private AggregateType aggType;
    private final boolean isNull;
    private final Optional<Expression> defaultValue;
    private final String comment;

    /**
     * constructor
     */
    public ColumnDefinition(String name, DataType type, boolean isKey, AggregateType aggType, boolean isNull,
            Optional<Expression> defaultValue, String comment) {
        this.name = name;
        this.type = type;
        this.isKey = isKey;
        this.aggType = aggType;
        this.isNull = isNull;
        this.defaultValue = defaultValue;
        this.comment = comment;
    }

    public String getName() {
        return name;
    }

    public DataType getType() {
        return type;
    }

    public AggregateType getAggType() {
        return aggType;
    }

    /**
     * validate column definition and analyze
     */
    public void validate(Set<String> keysSet) {
        if (type.isJsonType() || type.isStructType() || type.isArrayType() || type.isMapType()) {
            throw new AnalysisException(String.format("currently type %s is not supported for Nereids",
                    type.toString()));
        }
        if (type.isStringLikeType()) {
            if (type instanceof CharType && ((CharType) type).getLen() == -1) {
                type = new CharType(1);
            } else if (type instanceof VarcharType && ((VarcharType) type).getLen() == -1) {
                type = new VarcharType(VarcharType.MAX_VARCHAR_LENGTH);
            }
        }
        if (keysSet.contains(name)) {
            isKey = true;
            if (aggType != null) {
                throw new AnalysisException(String.format("Key column %s can not set aggregation type", name));
            }
        } else if (aggType == null) {
            aggType = AggregateType.NONE;
        }
    }

    public Column translateToCatalogStyle() {
        return new Column(name, type.toCatalogDataType(), isKey, aggType, isNull,
                false, null, comment, true,
                null, 0, "");
    }
}
