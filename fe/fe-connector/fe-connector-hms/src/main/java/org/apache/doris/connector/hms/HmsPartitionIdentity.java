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

package org.apache.doris.connector.hms;

import org.apache.hadoop.hive.common.FileUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

final class HmsPartitionIdentity {

    private HmsPartitionIdentity() {
    }

    static List<String> keysFromName(String partitionName) {
        return parseParts(partitionName, null, true);
    }

    static ParsedPartitionName parse(String partitionName, List<String> expectedKeys) {
        return new ParsedPartitionName(partitionName,
                Collections.unmodifiableList(parseParts(partitionName, expectedKeys, false)));
    }

    private static List<String> parseParts(
            String partitionName, List<String> expectedKeys, boolean returnKeys) {
        if (partitionName == null || partitionName.isEmpty()) {
            throw new IllegalArgumentException("partition name must not be empty");
        }
        List<String> parts = new ArrayList<>();
        int segmentStart = 0;
        int keyIndex = 0;
        while (segmentStart < partitionName.length()) {
            int segmentEnd = partitionName.indexOf('/', segmentStart);
            if (segmentEnd < 0) {
                segmentEnd = partitionName.length();
            }
            int separator = partitionName.indexOf('=', segmentStart);
            if (separator <= segmentStart || separator >= segmentEnd) {
                throw new IllegalArgumentException("invalid partition name: " + partitionName);
            }
            String key = FileUtils.unescapePathName(partitionName.substring(segmentStart, separator))
                    .toLowerCase(Locale.ROOT);
            if (expectedKeys != null
                    && (keyIndex >= expectedKeys.size() || !expectedKeys.get(keyIndex).equals(key))) {
                throw new IllegalArgumentException("inconsistent partition keys in request: " + partitionName);
            }
            parts.add(returnKeys ? key
                    : FileUtils.unescapePathName(partitionName.substring(separator + 1, segmentEnd)));
            keyIndex++;
            if (segmentEnd == partitionName.length()) {
                break;
            }
            segmentStart = segmentEnd + 1;
            if (segmentStart == partitionName.length()) {
                throw new IllegalArgumentException("invalid partition name: " + partitionName);
            }
        }
        if (expectedKeys != null && keyIndex != expectedKeys.size()) {
            throw new IllegalArgumentException("inconsistent partition keys in request: " + partitionName);
        }
        return parts;
    }

    static final class ParsedPartitionName {
        private final String name;
        private final List<String> values;

        private ParsedPartitionName(String name, List<String> values) {
            this.name = name;
            this.values = values;
        }

        String getName() {
            return name;
        }

        List<String> getValues() {
            return values;
        }
    }
}
