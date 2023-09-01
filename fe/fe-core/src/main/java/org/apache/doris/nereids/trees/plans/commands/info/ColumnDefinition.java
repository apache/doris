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
import org.apache.doris.catalog.KeysType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;
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
    private final boolean isNullable;
    private final Optional<Expression> defaultValue;
    private final String comment;
    private final boolean isVisible;

    public ColumnDefinition(String name, DataType type, boolean isKey, AggregateType aggType, boolean isNullable,
            Optional<Expression> defaultValue, String comment) {
        this(name, type, isKey, aggType, isNullable, defaultValue, comment, true);
    }

    /**
     * constructor
     */
    public ColumnDefinition(String name, DataType type, boolean isKey, AggregateType aggType, boolean isNullable,
            Optional<Expression> defaultValue, String comment, boolean isVisible) {
        this.name = name;
        this.type = type;
        this.isKey = isKey;
        this.aggType = aggType;
        this.isNullable = isNullable;
        this.defaultValue = defaultValue;
        this.comment = comment;
        this.isVisible = isVisible;
    }

    public ColumnDefinition(String name, DataType type, boolean isNullable) {
        this(name, type, false, null, isNullable, Optional.empty(), "");
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
    public void validate(Set<String> keysSet, boolean isEnableMergeOnWrite, KeysType keysType) {
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
            if (keysType.equals(KeysType.DUP_KEYS)) {
                aggType = AggregateType.NONE;
            } else if (keysType.equals(KeysType.UNIQUE_KEYS) && isEnableMergeOnWrite) {
                aggType = AggregateType.NONE;
            } else if (!keysType.equals(KeysType.AGG_KEYS)) {
                aggType = AggregateType.REPLACE;
            } else {
                throw new AnalysisException("should set aggregation type to non-key column when in aggregate key");
            }
        }
    }

    public Column translateToCatalogStyle() {
        return new Column(name, type.toCatalogDataType(), isKey, aggType, isNullable,
                false, defaultValue.map(ExpressionTrait::toSql).orElse(null), comment, isVisible,
                null, Column.COLUMN_UNIQUE_ID_INIT_VALUE, defaultValue.map(ExpressionTrait::toSql).orElse(null));
    }

    // hidden column
    public static ColumnDefinition newDeleteSignColumnDefinition() {
        return new ColumnDefinition(Column.DELETE_SIGN, TinyIntType.INSTANCE, false, null, false,
                Optional.of(new TinyIntLiteral(((byte) 0))), "doris delete flag hidden column", false);
    }

    public static ColumnDefinition newDeleteSignColumnDefinition(AggregateType aggregateType) {
        return new ColumnDefinition(Column.DELETE_SIGN, TinyIntType.INSTANCE, false, aggregateType, false,
                Optional.of(new TinyIntLiteral(((byte) 0))), "doris delete flag hidden column", false);
    }

    public static ColumnDefinition newSequenceColumnDefinition(DataType type) {
        return new ColumnDefinition(Column.SEQUENCE_COL, type, false, null, true,
                Optional.empty(), "sequence column hidden column", false);
    }

    public static ColumnDefinition newSequenceColumnDefinition(DataType type, AggregateType aggregateType) {
        return new ColumnDefinition(Column.SEQUENCE_COL, type, false, aggregateType, true,
                Optional.empty(), "sequence column hidden column", false);
    }

    public static ColumnDefinition newRowStoreColumnDefinition(AggregateType aggregateType) {
        return new ColumnDefinition(Column.ROW_STORE_COL, StringType.INSTANCE, false, aggregateType, false,
                Optional.of(new StringLiteral("")), "doris row store hidden column", false);
    }

    public static ColumnDefinition newVersionColumnDefinition(AggregateType aggregateType) {
        return new ColumnDefinition(Column.VERSION_COL, BigIntType.INSTANCE, false, aggregateType, false,
                Optional.of(new TinyIntLiteral(((byte) 0))), "doris version hidden column", false);
    }

}
