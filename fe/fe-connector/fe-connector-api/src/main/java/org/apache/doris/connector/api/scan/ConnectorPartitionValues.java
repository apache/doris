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

package org.apache.doris.connector.api.scan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class ConnectorPartitionValues {

    public static final String HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__";
    public static final String NULL_PARTITION_VALUE = "\\N";

    private ConnectorPartitionValues() {
    }

    public static Normalized normalize(List<String> partitionValues) {
        if (partitionValues == null || partitionValues.isEmpty()) {
            return new Normalized(Collections.emptyList(), Collections.emptyList());
        }
        List<String> values = new ArrayList<>(partitionValues.size());
        List<Boolean> isNull = new ArrayList<>(partitionValues.size());
        for (String value : partitionValues) {
            boolean nullValue = isNullPartitionValue(value);
            values.add(normalizePartitionValue(value));
            isNull.add(nullValue);
        }
        return new Normalized(values, isNull);
    }

    public static boolean isNullPartitionValue(String value) {
        return value == null || HIVE_DEFAULT_PARTITION.equals(value)
                || NULL_PARTITION_VALUE.equals(value);
    }

    public static String normalizePartitionValue(String value) {
        return value == null || HIVE_DEFAULT_PARTITION.equals(value)
                ? NULL_PARTITION_VALUE : value;
    }

    public static final class Normalized {
        private final List<String> values;
        private final List<Boolean> isNull;

        private Normalized(List<String> values, List<Boolean> isNull) {
            this.values = values;
            this.isNull = isNull;
        }

        public List<String> getValues() {
            return values;
        }

        public List<Boolean> getIsNull() {
            return isNull;
        }
    }
}
