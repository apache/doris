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

package org.apache.doris.nereids.trees.expressions.functions.executable;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.ExecFunction;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.UuidLiteral;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.UuidType;
import org.apache.doris.nereids.util.DateUtils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/** Deterministic UUID functions evaluated by FE constant folding. */
public class UuidArithmetic {
    private static final UuidLiteral ZERO = new UuidLiteral("00000000-0000-0000-0000-000000000000");

    @ExecFunction(name = "to_uuid_or_zero")
    public static Literal toUuidOrZero(Literal text) {
        return text instanceof NullLiteral ? new NullLiteral(UuidType.INSTANCE) : parseOrDefault(text, ZERO);
    }

    @ExecFunction(name = "to_uuid_or_null")
    public static Literal toUuidOrNull(Literal text) {
        return parseOrDefault(text, new NullLiteral(UuidType.INSTANCE));
    }

    @ExecFunction(name = "to_uuid_or_default")
    public static Literal toUuidOrDefault(Literal text) {
        return parseOrDefault(text, ZERO);
    }

    @ExecFunction(name = "to_uuid_or_default")
    public static Literal toUuidOrDefault(Literal text, Literal fallback) {
        return parseOrDefault(text, fallback);
    }

    private static Literal parseOrDefault(Literal text, Literal fallback) {
        if (text instanceof NullLiteral) {
            return fallback;
        }
        try {
            return new UuidLiteral(((StringLikeLiteral) text).getStringValue());
        } catch (AnalysisException e) {
            return fallback;
        }
    }

    @ExecFunction(name = "uuid_v7_to_datetime")
    public static Literal uuidV7ToDateTime(UuidLiteral uuid) {
        return uuidV7ToDateTime(uuid, DateUtils.getTimeZone());
    }

    @ExecFunction(name = "uuid_v7_to_datetime")
    public static Literal uuidV7ToDateTime(UuidLiteral uuid, StringLikeLiteral timezone) throws DdlException {
        String zone = TimeUtils.checkTimeZoneValidAndStandardize(timezone.getStringValue());
        return uuidV7ToDateTime(uuid, ZoneId.of(zone, TimeUtils.timeZoneAliasMap));
    }

    private static Literal uuidV7ToDateTime(UuidLiteral uuid, ZoneId zone) {
        long millis = uuid.getValue().version() == 7 ? uuid.getValue().getMostSignificantBits() >>> 16 : 0;
        LocalDateTime datetime = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), zone);
        if (datetime.getYear() > 9999 || datetime.getYear() < 0) {
            return new NullLiteral(DateTimeV2Type.of(3));
        }
        return DateTimeV2Literal.fromJavaDateType(datetime, 3);
    }
}
