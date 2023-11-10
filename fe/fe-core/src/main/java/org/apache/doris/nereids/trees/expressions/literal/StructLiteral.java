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
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;

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
        super(computeDataType(fields));
        this.fields = ImmutableList.copyOf(fields);
    }

    @Override
    public List<Literal> getValue() {
        return fields;
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        try {
            return new org.apache.doris.analysis.StructLiteral(
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
    public String toSql() {
        return "{" + fields.stream().map(Literal::toSql).collect(Collectors.joining(",")) + "}";
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitStructLiteral(this, context);
    }

    private static StructType computeDataType(List<Literal> fields) {
        ImmutableList.Builder<StructField> structFields = ImmutableList.builder();
        for (int i = 0; i < fields.size(); i++) {
            structFields.add(new StructField(String.valueOf(i + 1), fields.get(i).getDataType(), true, ""));
        }
        return new StructType(structFields.build());
    }
}
