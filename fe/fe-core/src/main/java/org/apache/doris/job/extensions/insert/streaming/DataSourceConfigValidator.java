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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.nereids.trees.plans.commands.LoadCommand;

import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

public class DataSourceConfigValidator {
    private static final Set<String> ALLOW_SOURCE_KEYS = Sets.newHashSet(
            DataSourceConfigKeys.JDBC_URL,
            DataSourceConfigKeys.USER,
            DataSourceConfigKeys.PASSWORD,
            DataSourceConfigKeys.OFFSET,
            DataSourceConfigKeys.DRIVER_URL,
            DataSourceConfigKeys.DRIVER_CLASS,
            DataSourceConfigKeys.DATABASE,
            DataSourceConfigKeys.SCHEMA,
            DataSourceConfigKeys.INCLUDE_TABLES,
            DataSourceConfigKeys.EXCLUDE_TABLES,
            DataSourceConfigKeys.SNAPSHOT_SPLIT_SIZE,
            DataSourceConfigKeys.SNAPSHOT_PARALLELISM
    );

    public static void validateSource(Map<String, String> input) throws IllegalArgumentException {
        for (Map.Entry<String, String> entry : input.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            if (!ALLOW_SOURCE_KEYS.contains(key)) {
                throw new IllegalArgumentException("Unexpected key: '" + key + "'");
            }

            if (!isValidValue(key, value)) {
                throw new IllegalArgumentException("Invalid value for key '" + key + "': " + value);
            }
        }
    }

    public static void validateTarget(Map<String, String> input) throws IllegalArgumentException {
        for (Map.Entry<String, String> entry : input.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith(DataSourceConfigKeys.TABLE_PROPS_PREFIX)
                    && !key.startsWith(DataSourceConfigKeys.LOAD_PROPERTIES)) {
                throw new IllegalArgumentException("Not support target properties key " + key);
            }

            if (key.equals(DataSourceConfigKeys.LOAD_PROPERTIES + LoadCommand.MAX_FILTER_RATIO_PROPERTY)) {
                try {
                    Double.parseDouble(entry.getValue());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid value for key '" + key + "': " + entry.getValue());
                }
            }
        }
    }

    private static boolean isValidValue(String key, String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }

        if (key.equals(DataSourceConfigKeys.OFFSET)
                && !(value.equals(DataSourceConfigKeys.OFFSET_INITIAL)
                || value.equals(DataSourceConfigKeys.OFFSET_LATEST))) {
            return false;
        }
        return true;
    }

}
