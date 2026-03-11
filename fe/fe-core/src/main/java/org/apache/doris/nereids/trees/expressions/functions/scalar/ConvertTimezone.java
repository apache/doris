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

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.functions.RewriteWhenAnalyze;
import org.apache.doris.nereids.trees.expressions.shape.TernaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.base.Preconditions;

import java.util.List;

/**
 * Snowflake-compatible function 'convert_timezone'.
 * CONVERT_TIMEZONE(source_tz, target_tz, timestamp) converts timestamp between timezones.
 * Snowflake parameter order: (source_tz, target_tz, timestamp)
 * Doris convert_tz parameter order: (timestamp, source_tz, target_tz)
 * Rewrites to convert_tz(child(2), child(0), child(1)) during analysis.
 */
public class ConvertTimezone extends ScalarFunction
        implements TernaryExpression, CustomSignature, AlwaysNullable, RewriteWhenAnalyze {

    /**
     * constructor with 3 arguments.
     */
    public ConvertTimezone(Expression arg0, Expression arg1, Expression arg2) {
        super("convert_timezone", arg0, arg1, arg2);
    }

    /** constructor for withChildren and reuse signature */
    private ConvertTimezone(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public FunctionSignature customSignature() {
        // Preserve the precision of the input timestamp argument.
        // If arg2 is already a DateTimeV2Type, use its precision for both the signature
        // type constraint and the return type. If arg2 is a string literal (VarcharType),
        // the framework will cast it to DateTimeV2Type.SYSTEM_DEFAULT (precision 6).
        DataType tsArgType = getArgumentType(2);
        DateTimeV2Type effectiveDtType = (tsArgType instanceof DateTimeV2Type)
                ? (DateTimeV2Type) tsArgType
                : DateTimeV2Type.SYSTEM_DEFAULT;
        return FunctionSignature.ret(effectiveDtType)
                .args(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, effectiveDtType);
    }

    /**
     * withChildren.
     */
    @Override
    public ConvertTimezone withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 3);
        return new ConvertTimezone(getFunctionParams(children));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitConvertTimezone(this, context);
    }

    @Override
    public Expression rewriteWhenAnalyze() {
        // CONVERT_TIMEZONE(src_tz, dst_tz, ts) => convert_tz(ts, src_tz, dst_tz)
        return new ConvertTz(child(2), child(0), child(1));
    }
}
