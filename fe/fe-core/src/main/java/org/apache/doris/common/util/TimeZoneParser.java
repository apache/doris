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

package org.apache.doris.common.util;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Doris timezone syntax, independent of the Java timezone database's input grammar. */
public final class TimeZoneParser {
    public enum Usage {
        SESSION, DATETIME, TIMESTAMPTZ
    }

    private static final Pattern OFFSET = Pattern.compile("^([+-])([0-9]{1,2})(?::([0-9]{2}))?$");
    private static final Pattern TEXT_OFFSET = Pattern.compile(
            "^([+-])([0-9]{1,2})(?::?([0-9]{2}))?(?::([0-9]{2}))?$");
    private static final Map<String, String> REGION_NAMES;

    static {
        Map<String, String> names = new HashMap<>();
        for (String region : ZoneId.getAvailableZoneIds()) {
            names.put(region.toLowerCase(Locale.ROOT), region);
        }
        REGION_NAMES = Collections.unmodifiableMap(names);
    }

    private TimeZoneParser() {
    }

    public static String normalize(String value, Usage usage) {
        if (value == null || value.isEmpty()) {
            throw new DateTimeException("Empty timezone");
        }
        String name = findAlias(value);
        if (name != null) {
            return name;
        }
        String region = findRegion(value, usage);
        if (region != null) {
            return region;
        }
        String upper = value.toUpperCase(Locale.ROOT);
        // Java's standard UTC names are not present in SHORT_IDS.
        if (upper.equals("UT") || upper.equals("Z")) {
            return "UTC";
        }
        boolean prefixed = upper.startsWith("UTC") || upper.startsWith("GMT");
        String offset = prefixed ? value.substring(3) : value;
        Matcher matcher = (usage == Usage.SESSION || prefixed ? OFFSET : TEXT_OFFSET).matcher(offset);
        if (!matcher.matches()
                || (usage == Usage.SESSION && !prefixed && matcher.group(3) == null)) {
            throw new DateTimeException("Invalid timezone: " + value);
        }
        boolean negative = "-".equals(matcher.group(1));
        int hour = Integer.parseInt(matcher.group(2));
        int minute = matcher.group(3) == null ? 0 : Integer.parseInt(matcher.group(3));
        int second = !prefixed && usage != Usage.SESSION && matcher.group(4) != null
                ? Integer.parseInt(matcher.group(4)) : 0;
        // Prefixed offsets have the same contract as session offsets in BE.
        Usage policy = prefixed ? Usage.SESSION : usage;
        int limit = policy == Usage.SESSION ? (negative ? 12 : 14)
                : policy == Usage.DATETIME ? 14 : 18;
        if (hour > limit || minute >= 60 || second >= 60
                || (policy != Usage.TIMESTAMPTZ && hour == limit && minute != 0)
                || (policy == Usage.TIMESTAMPTZ && hour == limit && (minute != 0 || second != 0))
                || (policy == Usage.DATETIME
                        && (matcher.group(4) != null || (minute != 0 && minute != 30 && minute != 45)))) {
            throw new DateTimeException("Invalid timezone offset: " + value);
        }
        String normalized = String.format(Locale.ROOT, "%s%02d:%02d", negative ? "-" : "+", hour, minute);
        if (second != 0) {
            normalized += String.format(Locale.ROOT, ":%02d", second);
        }
        return prefixed ? upper.substring(0, 3) + normalized : normalized;
    }

    private static String findAlias(String value) {
        for (String alias : TimeUtils.timeZoneAliasMap.keySet()) {
            if (alias.equalsIgnoreCase(value)) {
                return TimeUtils.getCanonicalTimeZone(alias);
            }
        }
        return null;
    }

    private static String findRegion(String value, Usage usage) {
        String region = REGION_NAMES.get(value.toLowerCase(Locale.ROOT));
        if (usage == Usage.SESSION && !value.equals(region)) {
            return null;
        }
        return region;
    }

    public static ZoneId parse(String value, Usage usage) {
        return ZoneId.of(normalize(value, usage));
    }
}
