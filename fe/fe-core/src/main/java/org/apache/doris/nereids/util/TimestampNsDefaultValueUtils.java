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

package org.apache.doris.nereids.util;

import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.DefaultValueExprDef;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.nereids.trees.expressions.literal.TimeStampNsLiteral;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.TimeStampNsType;

import java.time.LocalDateTime;
import java.util.Optional;

/** Utilities for expanding TIMESTAMP_NS default expressions. */
public final class TimestampNsDefaultValueUtils {

    private TimestampNsDefaultValueUtils() {
    }

    /**
     * Expand CURRENT_TIMESTAMP precision that DATETIMEV2 cannot represent into a TIMESTAMP_NS literal.
     * The normal function path is retained for precision 0 through 6.
     */
    public static Optional<TimeStampNsLiteral> currentTimestampLiteral(Column column) {
        if (!column.getType().isTimeStampNs()) {
            return Optional.empty();
        }
        DefaultValueExprDef expression = column.getDefaultValueExprDef();
        if (expression == null || !ColumnDef.DefaultValue.NOW.equalsIgnoreCase(expression.getExprName())
                || expression.getPrecision() == null
                || expression.getPrecision() <= DateTimeV2Type.MAX_SCALE) {
            return Optional.empty();
        }

        int precision = expression.getPrecision().intValue();
        int truncationFactor = (int) Math.pow(10, TimeStampNsType.SCALE - precision);
        LocalDateTime now = LocalDateTime.now(TimeUtils.getTimeZone().toZoneId());
        LocalDateTime truncated = now.withNano(now.getNano() / truncationFactor * truncationFactor);
        return Optional.of(TimeStampNsLiteral.fromJavaDateType(truncated));
    }
}
