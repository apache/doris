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

import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.FileType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** An independent FILE constant returned by validated constant STRUCT/JSON conversion on the BE. */
public final class FileLiteral extends Literal {
    private final List<Literal> fields;

    public FileLiteral(List<Literal> validatedFields) {
        super(FileType.INSTANCE);
        Preconditions.checkArgument(validatedFields.size() == 6, "FILE literal requires six children");
        Preconditions.checkArgument(validatedFields.get(0) instanceof StringLikeLiteral,
                "A non-NULL FILE literal requires uri");
        this.fields = ImmutableList.copyOf(validatedFields);
    }

    @Override
    public List<Literal> getValue() {
        return fields;
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return new org.apache.doris.analysis.FileLiteral(
                fields.stream().map(Literal::toLegacyLiteral).collect(Collectors.toList()));
    }

    @Override
    public String getStringValue() {
        return toLegacyLiteral().getStringValue();
    }

    @Override
    public String computeToSql() {
        return toLegacyLiteral().accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitFileLiteral(this, context);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof FileLiteral && fields.equals(((FileLiteral) other).fields);
    }

    @Override
    protected int computeHashCode() {
        // Expression identity, not SQL value hash semantics.
        return Objects.hash(dataType, fields);
    }
}
