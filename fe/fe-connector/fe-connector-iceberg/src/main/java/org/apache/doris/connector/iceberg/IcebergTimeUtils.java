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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.ConnectorSession;

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Self-contained session time-zone resolution + datetime parsing for the iceberg connector (the connector
 * cannot import fe-core {@code TimeUtils}). Shared by {@link IcebergScanPlanProvider} (timestamptz literal
 * pushdown, T02) and {@link IcebergConnectorMetadata} ({@code FOR TIME AS OF} time-travel, T07).
 */
public final class IcebergTimeUtils {

    // Self-contained mirror of fe-core TimeUtils.timeZoneAliasMap (cannot import TimeUtils). Doris stores the
    // session time_zone un-canonicalized (e.g. SET time_zone='CST' keeps "CST"), and legacy resolves it via
    // ZoneId.of(tz, timeZoneAliasMap). Without these aliases a plain ZoneId.of("CST") throws (CST is a
    // SHORT_ID) and falls back to UTC, shifting a timestamp literal by hours -> wrong file pruning / wrong
    // time-travel snapshot. The full SHORT_IDS map is required (PST/EST resolve via SHORT_IDS), plus the four
    // Doris overrides (CST/PRC -> Asia/Shanghai, UTC/GMT -> UTC = TimeUtils DEFAULT/UTC_TIME_ZONE).
    private static final Map<String, String> TIME_ZONE_ALIAS_MAP;

    // Byte-parity with legacy TimeUtils.DATETIME_FORMAT (= DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
    // the formatter TimeUtils.timeStringToLong uses for a non-digital FOR TIME AS OF datetime string.
    private static final DateTimeFormatter DATETIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    static {
        Map<String, String> aliases = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        aliases.putAll(ZoneId.SHORT_IDS);
        aliases.put("CST", "Asia/Shanghai");
        aliases.put("PRC", "Asia/Shanghai");
        aliases.put("UTC", "UTC");
        aliases.put("GMT", "UTC");
        TIME_ZONE_ALIAS_MAP = Collections.unmodifiableMap(aliases);
    }

    private IcebergTimeUtils() {
    }

    /**
     * Resolves the session time zone through the Doris alias map (mirrors fe-core {@code TimeUtils.getTimeZone}),
     * so aliases like {@code CST}/{@code PRC}/{@code EST} match legacy instead of throwing; a
     * {@code null}/blank/genuinely-invalid id falls back to UTC.
     */
    static ZoneId resolveSessionZone(ConnectorSession session) {
        if (session == null) {
            return ZoneOffset.UTC;
        }
        String tz = session.getTimeZone();
        if (tz == null || tz.trim().isEmpty()) {
            return ZoneOffset.UTC;
        }
        try {
            return ZoneId.of(tz.trim(), TIME_ZONE_ALIAS_MAP);
        } catch (Exception e) {
            return ZoneOffset.UTC;
        }
    }

    /**
     * Parses a {@code FOR TIME AS OF} datetime string to epoch-millis in {@code zone}, byte-faithful to legacy
     * {@code TimeUtils.timeStringToLong(value, sessionTZ)} (parse {@code yyyy-MM-dd HH:mm:ss} as a local
     * date-time, then interpret it in the session zone). Legacy returned {@code -1} on a parse failure and the
     * caller ({@code IcebergUtils.getQuerySpecSnapshot}) turned that into a {@code DateTimeException}; we throw
     * it directly (fail loud — a parse error is a user mistake, not a not-found).
     */
    static long datetimeToMillis(String value, ZoneId zone) {
        try {
            return LocalDateTime.parse(value, DATETIME_FORMAT).atZone(zone).toInstant().toEpochMilli();
        } catch (DateTimeParseException e) {
            throw new DateTimeException("can't parse time: " + value);
        }
    }
}
