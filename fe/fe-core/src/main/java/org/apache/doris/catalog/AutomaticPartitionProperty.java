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

public class AutomaticPartitionProperty {
    public static final String AUTOMATIC_PARTITION_PROPERTY_PREFIX = "automatic_partition.";
    public static final String TIME_UNIT = "automatic_partition.time_unit";
    public static final String PREFIX = "automatic_partition.prefix";
    public static final String ENABLE = "automatic_partition.enable";
    public static final String NOT_SET_RESERVED_HISTORY_PERIODS = "NULL";

    private boolean exist;

    private boolean enable;
    private String timeUnit;

    private String prefix;

    public AutomaticPartitionProperty(Map<String, String> properties) {
        if (properties != null && !properties.isEmpty()) {
            this.exist = true;
            this.enable = Boolean.parseBoolean(properties.get(ENABLE));
            this.timeUnit = properties.get(TIME_UNIT);
            this.prefix = properties.get(PREFIX);
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

    public String getPrefix() {
        return prefix;
    }

    public boolean getEnable() {
        return enable;
    }

    public String getProperties() {
        String res = ",\n\"" + ENABLE + "\" = \"" + enable + "\""
                + ",\n\"" + TIME_UNIT + "\" = \"" + timeUnit + "\""
                + ",\n\"" + PREFIX + "\" = \"" + prefix + "\"";
        return res;
    }
}
