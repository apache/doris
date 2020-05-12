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

package org.apache.doris.catalog;

import java.util.Map;

public class DynamicPartitionProperty{
    public static final String TIME_UNIT = "dynamic_partition.time_unit";
    public static final String START = "dynamic_partition.start";
    public static final String END = "dynamic_partition.end";
    public static final String PREFIX = "dynamic_partition.prefix";
    public static final String BUCKETS = "dynamic_partition.buckets";
    public static final String ENABLE = "dynamic_partition.enable";

    private boolean exist;

    private boolean enable;
    private String timeUnit;
    private int start;
    private int end;
    private String prefix;
    private int buckets;

    DynamicPartitionProperty(Map<String ,String> properties) {
        if (properties != null && !properties.isEmpty()) {
            this.exist = true;
            this.enable = Boolean.parseBoolean(properties.get(ENABLE));
            this.timeUnit = properties.get(TIME_UNIT);
            // In order to compatible dynamic add partition version
            this.start = Integer.parseInt(properties.getOrDefault(START, String.valueOf(Integer.MIN_VALUE)));
            this.end = Integer.parseInt(properties.get(END));
            this.prefix = properties.get(PREFIX);
            this.buckets = Integer.parseInt(properties.get(BUCKETS));
        } else {
            this.exist = false;
        }
    }

    public boolean isExist() {
        return exist;
    }

    public String getTimeUnit() {
        return timeUnit;
    }

    public int getStart() {
        return start;
    }

    public int getEnd() {
        return end;
    }

    public String getPrefix() {
        return prefix;
    }

    public int getBuckets() {
        return buckets;
    }

    public boolean getEnable() {
        return enable;
    }

    @Override
    public String toString() {
        return ",\n\"" + ENABLE + "\" = \"" + enable + "\"" +
                ",\n\"" + TIME_UNIT + "\" = \"" + timeUnit + "\"" +
                ",\n\"" + START + "\" = \"" + start + "\"" +
                ",\n\"" + END + "\" = \"" + end + "\"" +
                ",\n\"" + PREFIX + "\" = \"" + prefix + "\"" +
                ",\n\"" + BUCKETS + "\" = \"" + buckets + "\"";
    }
}
