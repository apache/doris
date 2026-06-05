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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.Monotonic;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullLiteral;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.shape.TernaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.DateUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

/**
 * ScalarFunction 'convert_tz'.
 */
public class ConvertTz extends ScalarFunction
        implements TernaryExpression, ExplicitlyCastableSignature, PropagateNullLiteral, PropagateNullable, Monotonic {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(DateTimeV2Type.WILDCARD)
                    .args(DateTimeV2Type.WILDCARD, VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT)
    );

    /**
     * constructor with 3 arguments.
     */
    public ConvertTz(Expression arg0, Expression arg1, Expression arg2) {
        super("convert_tz", castDateTime(arg0), arg1, arg2);
    }

    /** constructor for withChildren and reuse signature */
    private ConvertTz(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    private static Expression castDateTime(Expression arg0) {
        try {
            return arg0 instanceof StringLikeLiteral ? new Cast(arg0, DateTimeV2Type.forTypeFromString(
                    ((StringLikeLiteral) arg0).getStringValue()), true) : arg0;
        } catch (Exception e) {
            return new NullLiteral(DateTimeV2Type.SYSTEM_DEFAULT);
        }
    }

    /**
     * withChildren.
     */
    @Override
    public ConvertTz withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 3);
        return new ConvertTz(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitConvertTz(this, context);
    }

    @Override
    public boolean isMonotonic(Literal lower, Literal upper) {
        if (!(child(1) instanceof StringLikeLiteral) || !(child(2) instanceof StringLikeLiteral)) {
            return false;
        }
        ZoneId fromZone = parseZoneId((StringLikeLiteral) child(1));
        ZoneId toZone = parseZoneId((StringLikeLiteral) child(2));
        if (fromZone == null || toZone == null) {
            return false;
        }
        if (toZone.getRules().isFixedOffset()) {
            return true;
        }
        if (lower == null || upper == null) {
            return false;
        }
        LocalDateTime lowerDateTime = toLocalDateTime(lower);
        LocalDateTime upperDateTime = toLocalDateTime(upper);
        if (lowerDateTime == null || upperDateTime == null) {
            return false;
        }
        if (lowerDateTime.equals(upperDateTime)) {
            return true;
        }
        if (upperDateTime.isBefore(lowerDateTime)) {
            return false;
        }
        /*
         * convert_tz can be treated as a composition of two mappings:
         *
         *   source local time x -> instant by from_tz -> target local time by to_tz.
         *
         * After PR #64029, the first mapping is monotonic non-decreasing. A spring gap in from_tz
         * flattens skipped local times to the transition instant, and a fall-back overlap uses the
         * pre-transition offset before jumping forward at the overlap end. Neither case makes the
         * instant move backward as x increases.
         *
         * The second mapping, instant -> to_tz local time, is also monotonic non-decreasing except
         * at a to_tz fall-back transition, where the displayed local time jumps backward. Therefore
         * convert_tz is non-monotonic for a partition only when the instant interval obtained from
         * interpreting the partition bounds in from_tz crosses a to_tz fall-back transition instant.
         *
         * Partition pruning folds both endpoints before deriving the function range, so a fall-back
         * instant inside (fromInstant(lower), fromInstant(upper)] disables the monotonic shortcut.
         */
        Instant lowerInstant = DateTimeLiteral.convertLocalToInstant(lowerDateTime, fromZone);
        Instant upperInstant = DateTimeLiteral.convertLocalToInstant(upperDateTime, fromZone);
        if (upperInstant.isBefore(lowerInstant)) {
            return false;
        }
        return !DateUtils.hasFallbackTransitionInInstantRange(toZone, lowerInstant, upperInstant);
    }

    @Override
    public boolean isPositive() {
        return true;
    }

    @Override
    public int getMonotonicFunctionChildIndex() {
        return 0;
    }

    @Override
    public Expression withConstantArgs(Expression literal) {
        return new ConvertTz(literal, child(1), child(2));
    }

    private ZoneId parseZoneId(StringLikeLiteral timeZone) {
        try {
            String standardizedTimeZone = TimeUtils.checkTimeZoneValidAndStandardize(timeZone.getStringValue());
            return ZoneId.of(standardizedTimeZone, TimeUtils.timeZoneAliasMap);
        } catch (DdlException | DateTimeException e) {
            return null;
        }
    }

    private LocalDateTime toLocalDateTime(Literal literal) {
        return literal instanceof DateLiteral ? ((DateLiteral) literal).toJavaDateType() : null;
    }
}
