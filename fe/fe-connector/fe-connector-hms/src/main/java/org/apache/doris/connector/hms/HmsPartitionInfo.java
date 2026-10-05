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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * HMS partition metadata DTO using connector-SPI types.
 */
public final class HmsPartitionInfo {

    private final List<String> values;
    private final String location;
    private final String inputFormat;
    private final String outputFormat;
    private final String serializationLib;
    private final Map<String, String> parameters;

    public HmsPartitionInfo(List<String> values, String location,
            String inputFormat, String outputFormat,
            String serializationLib, Map<String, String> parameters) {
        this.values = values == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(values);
        this.location = location;
        this.inputFormat = inputFormat;
        this.outputFormat = outputFormat;
        this.serializationLib = serializationLib;
        this.parameters = parameters == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(parameters);
    }

    /** Partition column values in declaration order. */
    public List<String> getValues() {
        return values;
    }

    public String getLocation() {
        return location;
    }

    public String getInputFormat() {
        return inputFormat;
    }

    public String getOutputFormat() {
        return outputFormat;
    }

    public String getSerializationLib() {
        return serializationLib;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HmsPartitionInfo)) {
            return false;
        }
        HmsPartitionInfo that = (HmsPartitionInfo) o;
        return Objects.equals(values, that.values)
                && Objects.equals(location, that.location);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values, location);
    }

    @Override
    public String toString() {
        return "HmsPartitionInfo{values=" + values
                + ", location=" + location + "}";
    }
}
