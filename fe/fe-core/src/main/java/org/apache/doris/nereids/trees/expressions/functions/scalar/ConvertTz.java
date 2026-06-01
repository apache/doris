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
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.shape.TernaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.zone.ZoneOffsetTransition;
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
        if (fromZone.getRules().isFixedOffset() && toZone.getRules().isFixedOffset()) {
            return true;
        }
        if (lower == null || upper == null) {
            return false;
        }
        LocalDateTime lowerDateTime = toLocalDateTime(lower);
        LocalDateTime upperDateTime = toLocalDateTime(upper);
        if (lowerDateTime == null || upperDateTime == null || upperDateTime.isBefore(lowerDateTime)) {
            return false;
        }
        // Partition pruning folds both endpoints as inclusive values, so a transition touching either boundary
        // must disable the monotonic shortcut.
        if (hasTransitionInLocalRange(fromZone, lowerDateTime, upperDateTime)) {
            return false;
        }
        Instant lowerInstant = lowerDateTime.atZone(fromZone).toInstant();
        Instant upperInstant = upperDateTime.atZone(fromZone).toInstant();
        if (upperInstant.isBefore(lowerInstant)) {
            return false;
        }
        return !hasTransitionInInstantRange(toZone, lowerInstant, upperInstant);
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

    private boolean hasTransitionInLocalRange(ZoneId zoneId, LocalDateTime lower, LocalDateTime upper) {
        if (zoneId.getRules().isFixedOffset() || !lower.isBefore(upper)) {
            return false;
        }
        ZoneOffsetTransition transition = zoneId.getRules()
                .previousTransition(lower.atZone(zoneId).toInstant().plusNanos(1));
        if (transition == null) {
            transition = zoneId.getRules().nextTransition(lower.atZone(zoneId).toInstant());
        }
        while (transition != null) {
            LocalDateTime transitionBefore = transition.getDateTimeBefore();
            LocalDateTime transitionAfter = transition.getDateTimeAfter();
            LocalDateTime transitionStart = transitionBefore.isBefore(transitionAfter)
                    ? transitionBefore : transitionAfter;
            if (upper.isBefore(transitionStart)) {
                return false;
            }
            LocalDateTime transitionEnd = transitionBefore.isAfter(transitionAfter)
                    ? transitionBefore : transitionAfter;
            if (!transitionEnd.isBefore(lower) && !upper.isBefore(transitionStart)) {
                return true;
            }
            transition = zoneId.getRules().nextTransition(transition.getInstant());
        }
        return false;
    }

    private boolean hasTransitionInInstantRange(ZoneId zoneId, Instant lower, Instant upper) {
        if (zoneId.getRules().isFixedOffset()) {
            return false;
        }
        ZoneOffsetTransition transition = zoneId.getRules().previousTransition(lower.plusNanos(1));
        if (transition == null || transition.getInstant().isBefore(lower)) {
            transition = zoneId.getRules().nextTransition(lower);
        }
        return transition != null && !transition.getInstant().isAfter(upper);
    }
}
