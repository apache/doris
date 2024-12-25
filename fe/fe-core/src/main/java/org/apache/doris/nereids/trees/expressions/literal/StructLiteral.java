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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * struct literal
 */
public class StructLiteral extends Literal {

    private final List<Literal> fields;

    public StructLiteral() {
        super(StructType.SYSTEM_DEFAULT);
        this.fields = ImmutableList.of();
    }

    public StructLiteral(List<Literal> fields) {
        this(fields, computeDataType(fields));
    }

    /**
     * create Struct Literal with fields and datatype
     */
    public StructLiteral(List<Literal> fields, DataType dataType) {
        super(dataType);
        this.fields = ImmutableList.copyOf(Objects.requireNonNull(fields, "fields should not be null"));
        Preconditions.checkArgument(dataType instanceof StructType,
                "dataType should be StructType, but we meet %s", dataType);
        Preconditions.checkArgument(fields.size() == ((StructType) dataType).getFields().size(),
                "fields size is not same with dataType size. %s vs %s",
                fields.size(), ((StructType) dataType).getFields().size());
    }

    @Override
    public List<Literal> getValue() {
        return fields;
    }

    @Override
    protected Expression uncheckedCastTo(DataType targetType) throws AnalysisException {
        if (this.dataType.equals(targetType)) {
            return this;
        } else if (targetType instanceof StructType) {
            // we should pass dataType to constructor because arguments maybe empty
            if (((StructType) targetType).getFields().size() != this.fields.size()) {
                return super.uncheckedCastTo(targetType);
            }
            ImmutableList.Builder<Literal> newLiterals = ImmutableList.builder();
            for (int i = 0; i < fields.size(); i++) {
                newLiterals.add((Literal) fields.get(i)
                        .uncheckedCastTo(((StructType) targetType).getFields().get(i).getDataType()));
            }
            return new StructLiteral(newLiterals.build(), targetType);
        } else {
            return super.uncheckedCastTo(targetType);
        }
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        try {
            return new org.apache.doris.analysis.StructLiteral(dataType.toCatalogDataType(),
                    fields.stream().map(Literal::toLegacyLiteral).toArray(LiteralExpr[]::new)
            );
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e);
        }
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
        StructLiteral that = (StructLiteral) o;
        return Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields);
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public String computeToSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("STRUCT(");
        for (int i = 0; i < fields.size(); i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append("'").append(((StructType) dataType).getFields().get(i).getName()).append("'");
            sb.append(":");
            sb.append(fields.get(i).toSql());
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitStructLiteral(this, context);
    }

    public static StructType constructStructType(List<DataType> fieldTypes) {
        ImmutableList.Builder<StructField> structFields = ImmutableList.builder();
        for (int i = 0; i < fieldTypes.size(); i++) {
            structFields.add(new StructField("col" + (i + 1), fieldTypes.get(i), true, ""));
        }
        return new StructType(structFields.build());
    }

    public static StructType computeDataType(List<? extends Expression> fields) {
        List<DataType> fieldTypes = fields.stream().map(ExpressionTrait::getDataType).collect(Collectors.toList());
        return constructStructType(fieldTypes);
    }
}
