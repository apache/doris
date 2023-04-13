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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

public class DateUtils {
    public static final DateLiteral[] DATE_LITERALS = new DateLiteral[] {
            new DateLiteral(1999, 5, 15),
            new DateLiteral(1965, 7, 21),
            new DateLiteral(1325, 4, 12)
    };
    public static final DateTimeLiteral[] DATETIME_LITERALS = new DateTimeLiteral[] {
            new DateTimeLiteral(1999, 5, 15, 13, 45, 35),
            new DateTimeLiteral(1965, 7, 21, 10, 4, 24),
            new DateTimeLiteral(1325, 4, 12, 20, 47, 56)
    };
    public static final DateV2Literal[] DATEV2_LITERALS = new DateV2Literal[] {
            new DateV2Literal(1999, 5, 15),
            new DateV2Literal(1965, 7, 21),
            new DateV2Literal(1325, 4, 12)
    };
    public static final DateTimeV2Literal[] DATETIMEV2_LITERALS = new DateTimeV2Literal[] {
            new DateTimeV2Literal(1999, 5, 15, 13, 45, 35, 45),
            new DateTimeV2Literal(1965, 7, 21, 10, 4, 24, 464),
            new DateTimeV2Literal(1325, 4, 12, 20, 47, 56, 435)
    };
    public static final IntegerLiteral[] INTEGER_LITERALS = new IntegerLiteral[] {
            new IntegerLiteral(1),
            new IntegerLiteral(3),
            new IntegerLiteral(5),
            new IntegerLiteral(15),
            new IntegerLiteral(30),
            new IntegerLiteral(55)
    };
    public static final VarcharLiteral[] DATE_TAGS = new VarcharLiteral[] {
            new VarcharLiteral("year"),
            new VarcharLiteral("month"),
            new VarcharLiteral("week"),
            new VarcharLiteral("day"),
            new VarcharLiteral("hour"),
            new VarcharLiteral("minute"),
            new VarcharLiteral("second")
    };
}
