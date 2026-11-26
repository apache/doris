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

package org.apache.doris.connector.spi.write;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Describes how rows of one connector write must be distributed to sink writers. */
public final class ConnectorWriteDistribution {

    /** Distribution behavior understood by the engine. */
    public enum Mode {
        EXECUTION_ANY,
        GATHER,
        HASH,
        EXTERNAL_UNPARTITIONED,
        EXTERNAL_HASH
    }

    /** Whether one ownership key is stable on one writer or may scale across writers. */
    public enum WriterAssignment {
        IDENTITY,
        SKEWED
    }

    private final Mode mode;
    private final List<String> routeColumns;
    private final String partitionFunction;
    private final Map<String, String> partitionFunctionOptions;
    private final WriterAssignment writerAssignment;

    private ConnectorWriteDistribution(Mode mode, List<String> routeColumns,
            String partitionFunction, Map<String, String> partitionFunctionOptions,
            WriterAssignment writerAssignment) {
        this.mode = Objects.requireNonNull(mode, "mode must not be null");
        this.routeColumns = immutableList(routeColumns);
        this.partitionFunction = partitionFunction;
        this.partitionFunctionOptions = immutableMap(partitionFunctionOptions);
        this.writerAssignment = writerAssignment;
    }

    /** Creates a distribution mode that carries no routing metadata. */
    public static ConnectorWriteDistribution simple(Mode mode) {
        if (mode == Mode.HASH || mode == Mode.EXTERNAL_HASH) {
            throw new IllegalArgumentException(mode + " requires routing metadata");
        }
        return new ConnectorWriteDistribution(mode, Collections.emptyList(), null,
                Collections.emptyMap(), null);
    }

    /** Uses the engine's ordinary hash shuffle for the named columns. */
    public static ConnectorWriteDistribution hash(List<String> routeColumns) {
        return new ConnectorWriteDistribution(Mode.HASH, requireRouteColumns(routeColumns), null,
                Collections.emptyMap(), null);
    }

    /**
     * Uses an external writer partition function registered in BE. FE treats {@code partitionFunction} and its
     * options as opaque values and forwards them together with the resolved route expressions.
     */
    public static ConnectorWriteDistribution externalHash(List<String> routeColumns,
            String partitionFunction, Map<String, String> partitionFunctionOptions,
            WriterAssignment writerAssignment) {
        String function = Objects.requireNonNull(partitionFunction,
                "partitionFunction must not be null");
        if (function.isEmpty()) {
            throw new IllegalArgumentException("partitionFunction must not be empty");
        }
        return new ConnectorWriteDistribution(Mode.EXTERNAL_HASH,
                requireRouteColumns(routeColumns), function, partitionFunctionOptions,
                Objects.requireNonNull(writerAssignment, "writerAssignment must not be null"));
    }

    private static List<String> requireRouteColumns(List<String> columns) {
        List<String> result = immutableList(columns);
        if (result.isEmpty()) {
            throw new IllegalArgumentException("routeColumns must not be empty");
        }
        return result;
    }

    private static <T> List<T> immutableList(List<T> values) {
        return Collections.unmodifiableList(new ArrayList<>(
                Objects.requireNonNull(values, "values must not be null")));
    }

    private static Map<String, String> immutableMap(Map<String, String> values) {
        return Collections.unmodifiableMap(new LinkedHashMap<>(
                Objects.requireNonNull(values, "values must not be null")));
    }

    public Mode getMode() {
        return mode;
    }

    public List<String> getRouteColumns() {
        return routeColumns;
    }

    public String getPartitionFunction() {
        return partitionFunction;
    }

    public Map<String, String> getPartitionFunctionOptions() {
        return partitionFunctionOptions;
    }

    public WriterAssignment getWriterAssignment() {
        return writerAssignment;
    }
}
