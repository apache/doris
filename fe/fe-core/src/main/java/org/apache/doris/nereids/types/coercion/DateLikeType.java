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

package org.apache.doris.nereids.types.coercion;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * date like type.
 */
public abstract class DateLikeType extends PrimitiveType implements RangeScalable {

    protected LocalDate toLocalDate(double d) {
        // d = (year * 10000 + month * 100 + day) * 1000000L;
        int date = (int) (d / 1000000);
        int day = date % 100;
        int month = (date / 100) % 100;
        int year = date / 10000;
        return LocalDate.of(year, month, day);
    }

    protected LocalDateTime toLocalDateTime(double d) {
        // d = (year * 10000 + month * 100 + day) * 1000000L + time
        // time = (hour * 10000 + minute * 100 + second);
        int date = (int) (d / 1000000);
        int day = date % 100;
        int month = (date / 100) % 100;
        int year = date / 10000;
        int time = (int) (d % 1000000);
        int second = time % 100;
        int minute = (time / 100) % 100;
        int hour = time / 10000;
        return LocalDateTime.of(year, month, day, hour, minute, second);
    }

    /**
     * parse string to date like literal.
     */
    public DateLiteral fromString(String s) {
        if (this instanceof DateType) {
            DateTimeV2Literal l = new DateTimeV2Literal(DateTimeV2Type.MAX, s);
            return new DateLiteral(l.getYear(), l.getMonth(), l.getDay());
        } else if (this instanceof DateV2Type) {
            DateTimeV2Literal l = new DateTimeV2Literal(DateTimeV2Type.MAX, s);
            return new DateV2Literal(l.getYear(), l.getMonth(), l.getDay());
        } else if (this instanceof DateTimeType) {
            return new DateTimeLiteral(s);
        } else if (this instanceof DateTimeV2Type) {
            return new DateTimeV2Literal((DateTimeV2Type) this, s);
        } else {
            throw new AnalysisException("unknown date like type");
        }
    }
}
