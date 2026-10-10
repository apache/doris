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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FileResourceSnapshot;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.VolatileIdentity;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.FileType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** Resolve file metadata through a named resource at execution time. */
public class ToFile extends UniqueFunction
        implements BinaryExpression, ExplicitlyCastableSignature, PropagateNullable {
    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(FileType.INSTANCE).args(StringType.INSTANCE, StringType.INSTANCE));

    public ToFile(Expression resource, Expression uri) {
        this(VolatileIdentity.newVolatileIdentity(), ImmutableList.of(resource, uri));
    }

    private ToFile(VolatileIdentity identity, List<Expression> arguments) {
        super("to_file", identity, arguments);
    }

    private ToFile(UniqueFunctionParams params) {
        super(params);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        getResourceName();
        DataType uriType = getArgument(1).getDataType();
        if (!uriType.isStringLikeType() && !uriType.isNullType()) {
            throw new AnalysisException("TO_FILE uri must be string-like or NULL");
        }
        // URI syntax, schemes and resource/URI compatibility belong to the BE
        // filesystem. Even a literal URI must reach it unchanged.
    }

    public String getResourceName() {
        Expression resource = getArgument(0);
        if (!(resource instanceof StringLikeLiteral)) {
            throw new AnalysisException("TO_FILE resource must be a non-NULL string literal");
        }
        return ((StringLikeLiteral) resource).getStringValue();
    }

    @Override
    public void checkLegalityAfterRewrite() {
        // Bind the name and check USAGE without capturing properties in an
        // expression that can be retained by PREPARE.
        FileResourceSnapshot.checkResource(getResourceName(), ConnectContext.get());
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitToFile(this, context);
    }

    @Override
    public ToFile withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new ToFile(getFunctionParams(children));
    }

    @Override
    public ToFile withIgnoreUniqueId(boolean ignoreUniqueId) {
        return new ToFile(volatileIdentity.withIgnoreUniqueId(ignoreUniqueId), getArguments());
    }
}
