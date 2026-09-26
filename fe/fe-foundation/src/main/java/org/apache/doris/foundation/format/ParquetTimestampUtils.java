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

package org.apache.doris.foundation.format;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Locale;
import java.util.regex.Pattern;

/** Shared timestamp options for file scans and connector-owned Hive writes. */
public final class ParquetTimestampUtils {
    public static final String HIVE_TIME_ZONE = "hive.parquet.time-zone";
    private static final Pattern OFFSET = Pattern.compile("^[+-]?\\d{1,2}:\\d{2}$");

    private ParquetTimestampUtils() {
    }

    public static String parseHiveTimeZone(String value) {
        String timeZone = value.trim();
        if (timeZone.isEmpty()) {
            return "";
        }
        try {
            String upperCase = timeZone.toUpperCase(Locale.ROOT);
            boolean isUtcOrGmt = upperCase.equals("UTC") || upperCase.equals("GMT");
            if (!isUtcOrGmt && (ZoneId.SHORT_IDS.containsKey(upperCase)
                    || upperCase.equals("CST") || upperCase.equals("PRC"))) {
                throw new DateTimeException("Ambiguous short timezone aliases are not supported");
            }
            String prefix = "";
            String offset = timeZone;
            if ((timeZone.startsWith("UTC") || timeZone.startsWith("GMT")) && timeZone.length() > 3) {
                prefix = timeZone.substring(0, 3);
                offset = timeZone.substring(3);
            }
            if (OFFSET.matcher(offset).matches()) {
                boolean negative = offset.charAt(0) == '-';
                String[] parts = offset.replaceAll("[+-]", "").split(":");
                int hours = Integer.parseInt(parts[0]);
                int minutes = Integer.parseInt(parts[1]);
                int totalMinutes = hours * 60 + minutes;
                if (minutes > 59 || totalMinutes > (negative ? 12 : 14) * 60) {
                    throw new DateTimeException("Timezone offset is outside the supported range");
                }
                timeZone = prefix + (negative ? "-" : "+")
                        + String.format(Locale.ROOT, "%02d:%02d", hours, minutes);
            } else if (!timeZone.contains("/") && !isUtcOrGmt) {
                throw new DateTimeException("Unknown timezone");
            }
            return ZoneId.of(timeZone).getId();
        } catch (DateTimeException e) {
            throw new IllegalArgumentException("The parameter " + HIVE_TIME_ZONE
                    + " must be an IANA timezone or UTC offset in the range -12:00 to +14:00; "
                    + "short timezone aliases are not supported, value is " + value.trim(), e);
        }
    }
}
