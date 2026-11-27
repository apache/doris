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

package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.mvcc.ConnectorTimeTravelSpec;

import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;

/** Timestamp parsing and snapshot retention for explicit Delta time travel. */
final class DeltaLakeTimeTravel {
    private static final DateTimeFormatter DATE_FORMAT = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
            .appendLiteral('-')
            .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NOT_NEGATIVE)
            .appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NOT_NEGATIVE)
            .toFormatter()
            .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter LOCAL_TIME_FORMAT = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
            .appendLiteral(':')
            .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
            .optionalEnd()
            .optionalEnd()
            .toFormatter()
            .withResolverStyle(ResolverStyle.STRICT);

    private DeltaLakeTimeTravel() {
    }

    static void checkRetention(Engine engine, Snapshot latest, Snapshot target, long nowMillis) {
        if (target.getVersion() == latest.getVersion()) {
            return;
        }
        long retentionMillis = DeltaKernelCompat.deletedFileRetentionMillis(latest.getTableProperties());
        long commitMillis = target.getTimestamp(engine);
        if (commitMillis < nowMillis - retentionMillis) {
            throw new DorisConnectorException("Cannot time travel beyond delta.deletedFileRetentionDuration: "
                    + target.getPath() + " version " + target.getVersion() + " committed at "
                    + Instant.ofEpochMilli(commitMillis) + ", retention " + retentionMillis + " ms");
        }
    }

    static long parseTimestampMillis(ConnectorSession session, ConnectorTimeTravelSpec spec) {
        String raw = spec.getStringValue();
        if (raw == null || raw.trim().isEmpty()) {
            throw invalidTimestamp(raw);
        }
        if (spec.isDigital()) {
            try {
                return Long.parseLong(raw);
            } catch (NumberFormatException e) {
                throw invalidTimestamp(raw);
            }
        }
        String value = raw.trim();
        try {
            return parseDateTime(value, DeltaLakePartitionValues.sessionZone(session)).toEpochMilli();
        } catch (DateTimeException | ArithmeticException e) {
            throw invalidTimestamp(raw);
        }
    }

    private static Instant parseDateTime(String value, ZoneId sessionZone) {
        int sep = findDateTimeSeparator(value);
        if (sep < 0) {
            return LocalDate.parse(value, DATE_FORMAT).atStartOfDay(sessionZone).toInstant();
        }
        LocalDate date = LocalDate.parse(value.substring(0, sep), DATE_FORMAT);
        String timePart = value.substring(sep + 1);
        int offsetStart = findOffsetStart(timePart);
        if (offsetStart < 0) {
            LocalTime time = LocalTime.parse(timePart, LOCAL_TIME_FORMAT);
            return ZonedDateTime.of(date, time, sessionZone).toInstant();
        }
        LocalTime time = LocalTime.parse(timePart.substring(0, offsetStart), LOCAL_TIME_FORMAT);
        String offsetPart = timePart.substring(offsetStart);
        if (offsetPart.startsWith(" ")) {
            offsetPart = offsetPart.substring(1);
        }
        if ("z".equals(offsetPart)) {
            offsetPart = "Z";
        }
        ZoneOffset offset = ZoneOffset.of(offsetPart);
        return OffsetDateTime.of(date, time, offset).toInstant();
    }

    private static int findDateTimeSeparator(String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == ' ' || c == 'T' || c == 't') {
                return i;
            }
        }
        return -1;
    }

    private static int findOffsetStart(String timePart) {
        for (int i = 0; i < timePart.length(); i++) {
            char c = timePart.charAt(i);
            if (c == 'Z' || c == 'z' || c == '+' || c == '-' || c == ' ') {
                return i;
            }
        }
        return -1;
    }

    private static DorisConnectorException invalidTimestamp(String value) {
        return new DorisConnectorException(
                "Delta Lake FOR TIME AS OF requires epoch milliseconds or an ISO-8601/SQL timestamp: " + value);
    }
}
