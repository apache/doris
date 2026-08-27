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

/** Canonical ordered partition values shared by cache keys and batch-result validation. */
final class HmsPartitionIdentity {

    private HmsPartitionIdentity() {
    }

    static List<String> fromName(String partitionName) {
        return parse(partitionName).getValues();
    }

    static ParsedPartitionName parse(String partitionName) {
        if (partitionName == null || partitionName.isEmpty()) {
            throw new IllegalArgumentException("partition name must not be empty");
        }
        List<String> keys = new ArrayList<>();
        List<String> values = new ArrayList<>();
        int segmentStart = 0;
        while (segmentStart < partitionName.length()) {
            int segmentEnd = partitionName.indexOf('/', segmentStart);
            if (segmentEnd < 0) {
                segmentEnd = partitionName.length();
            }
            int separator = partitionName.indexOf('=', segmentStart);
            if (separator <= segmentStart || separator >= segmentEnd) {
                throw new IllegalArgumentException("invalid partition name: " + partitionName);
            }
            keys.add(FileUtils.unescapePathName(partitionName.substring(segmentStart, separator))
                    .toLowerCase(Locale.ROOT));
            values.add(FileUtils.unescapePathName(partitionName.substring(separator + 1, segmentEnd)));
            if (segmentEnd == partitionName.length()) {
                break;
            }
            segmentStart = segmentEnd + 1;
            if (segmentStart == partitionName.length()) {
                throw new IllegalArgumentException("invalid partition name: " + partitionName);
            }
        }
        return new ParsedPartitionName(
                Collections.unmodifiableList(keys), Collections.unmodifiableList(values));
    }

    static final class ParsedPartitionName {
        private final List<String> keys;
        private final List<String> values;

        private ParsedPartitionName(List<String> keys, List<String> values) {
            this.keys = keys;
            this.values = values;
        }

        List<String> getKeys() {
            return keys;
        }

        List<String> getValues() {
            return values;
        }
    }
}
