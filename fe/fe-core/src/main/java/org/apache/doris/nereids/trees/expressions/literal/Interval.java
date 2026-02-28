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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.EnumUtils;

import java.util.List;
import java.util.Optional;

/**
 * Interval for timestamp calculation.
 */
public class Interval extends Expression implements UnaryExpression, AlwaysNotNullable {
    private final TimeUnit timeUnit;

    public Interval(Expression value, String desc) {
        this(value, TimeUnit.valueOf(desc.toUpperCase()));
    }

    public Interval(Expression value, TimeUnit timeUnit) {
        super(ImmutableList.of(value));
        this.timeUnit = timeUnit;
    }

    @Override
    public DataType getDataType() {
        return DateType.INSTANCE;
    }

    public Expression value() {
        return child();
    }

    public TimeUnit timeUnit() {
        return timeUnit;
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Interval(children.get(0), timeUnit);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitInterval(this, context);
    }

    @Override
    public String toDigest() {
        StringBuilder sb = new StringBuilder();
        sb.append("INTERVAL ");
        sb.append(value().toDigest());
        sb.append(" ").append(timeUnit);
        return sb.toString();
    }

    @Override
    protected boolean extraEquals(Expression that) {
        return that instanceof Interval && this.timeUnit().equals(((Interval) that).timeUnit());
    }

    /**
     * Supported time unit.
     */
    public enum TimeUnit {
        YEAR("YEAR", false, 800),
        YEAR_MONTH("YEAR_MONTH", false, 800),
        MONTH("MONTH", false, 700),
        QUARTER("QUARTER", false, 600), //TODO: need really support quarter
        WEEK("WEEK", false, 500),
        DAY("DAY", false, 400),
        DAY_HOUR("DAY_HOUR", false, 400),
        DAY_MINUTE("DAY_MINUTE", false, 400),
        DAY_MICROSECOND("DAY_MICROSECOND", false, 400),
        DAY_SECOND("DAY_SECOND", false, 400),
        HOUR("HOUR", true, 300),
        HOUR_MINUTE("HOUR_MINUTE", false, 300),
        HOUR_SECOND("HOUR_SECOND", false, 300),
        HOUR_MICROSECOND("HOUR_MICROSECOND", false, 300),
        MINUTE("MINUTE", true, 200),
        MINUTE_SECOND("MINUTE_SECOND", false, 200),
        MINUTE_MICROSECOND("MINUTE_MICROSECOND", false, 200),
        SECOND("SECOND", true, 100),
        SECOND_MICROSECOND("SECOND_MICROSECOND", true, 100);

        private final String description;
        private final boolean isDateTimeUnit;
        /**
         * Time unit level, second level is low, year level is high
         */
        private final int level;

        TimeUnit(String description, boolean isDateTimeUnit, int level) {
            this.description = description;
            this.isDateTimeUnit = isDateTimeUnit;
            this.level = level;
        }

        public boolean isDateTimeUnit() {
            return isDateTimeUnit;
        }

        public int getLevel() {
            return level;
        }

        @Override
        public String toString() {
            return description;
        }

        /**
         * Construct time unit by name
         */
        public static Optional<TimeUnit> of(String name) {
            return Optional.ofNullable(EnumUtils.getEnumIgnoreCase(TimeUnit.class, name));
        }
    }
}
