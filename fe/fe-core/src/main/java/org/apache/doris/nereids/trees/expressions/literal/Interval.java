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
import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateType;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.EnumUtils;

import java.util.Optional;

/**
 * Interval for timestamp calculation.
 */
public class Interval extends Expression implements LeafExpression, AlwaysNotNullable {
    private final Expression value;
    private final TimeUnit timeUnit;

    public Interval(Expression value, String desc) {
        super(ImmutableList.of());
        this.value = value;
        this.timeUnit = TimeUnit.valueOf(desc.toUpperCase());
    }

    @Override
    public DataType getDataType() {
        return DateType.INSTANCE;
    }

    public Expression value() {
        return value;
    }

    public TimeUnit timeUnit() {
        return timeUnit;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitInterval(this, context);
    }

    /**
     * Supported time unit.
     */
    public enum TimeUnit {
        YEAR("YEAR", false, 800),
        MONTH("MONTH", false, 700),
        QUARTER("QUARTER", false, 600),
        WEEK("WEEK", false, 500),
        DAY("DAY", false, 400),
        HOUR("HOUR", true, 300),
        MINUTE("MINUTE", true, 200),
        SECOND("SECOND", true, 100);

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
