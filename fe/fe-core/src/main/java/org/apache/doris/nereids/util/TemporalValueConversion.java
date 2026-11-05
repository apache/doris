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

import org.apache.doris.common.util.TimeZoneParser;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/** Timezone value conversion shared by literal CAST and executable SQL functions. */
public final class TemporalValueConversion {
    private TemporalValueConversion() {
    }

    /** Resolve conversion zones with the session policy shared by CAST and CONVERT_TZ. */
    public static LocalDateTime convertZone(LocalDateTime datetime, String fromTz, String toTz) {
        // These are conversion zones, not offsets embedded in DATETIME/TIMESTAMPTZ literals.
        // Keep aliases, offset syntax and range checks in the common parser.
        ZoneId fromZone = TimeZoneParser.parse(fromTz, TimeZoneParser.Usage.SESSION);
        ZoneId toZone = TimeZoneParser.parse(toTz, TimeZoneParser.Usage.SESSION);
        Instant instant = DateTimeLiteral.convertLocalToInstant(datetime, fromZone);
        return LocalDateTime.ofInstant(instant, toZone);
    }
}
