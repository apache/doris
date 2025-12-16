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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * Unified partition specification for INSERT statements.
 * Supports multiple partition modes:
 * 1. Auto-detect: PARTITION (*) - partitions = null
 * 2. Dynamic partition by name: PARTITION (p1, p2) - partitionNames is
 * non-empty
 * 3. Static partition with values: PARTITION (col='val', ...) -
 * staticPartitionValues is non-empty
 * 4. No partition specified: all fields are empty/default
 */
public class InsertPartitionSpec {

    /** Static partition key-value pairs: col_name -> value expression */
    private final Map<String, Expression> staticPartitionValues;

    /** Dynamic partition names (e.g., PARTITION (p1, p2)) */
    private final List<String> partitionNames;

    /** Whether it's a temporary partition */
    private final boolean isTemporary;

    /** Whether it's auto-detect mode (PARTITION (*)) */
    private final boolean isAutoDetect;

    private InsertPartitionSpec(Map<String, Expression> staticPartitionValues,
            List<String> partitionNames, boolean isTemporary, boolean isAutoDetect) {
        this.staticPartitionValues = staticPartitionValues != null
                ? ImmutableMap.copyOf(staticPartitionValues)
                : ImmutableMap.of();
        this.partitionNames = partitionNames != null
                ? ImmutableList.copyOf(partitionNames)
                : ImmutableList.of();
        this.isTemporary = isTemporary;
        this.isAutoDetect = isAutoDetect;
    }

    /** No partition specified */
    public static InsertPartitionSpec none() {
        return new InsertPartitionSpec(null, null, false, false);
    }

    /** Auto-detect partition: PARTITION (*) */
    public static InsertPartitionSpec autoDetect(boolean isTemporary) {
        return new InsertPartitionSpec(null, null, isTemporary, true);
    }

    /** Dynamic partition by name: PARTITION (p1, p2) */
    public static InsertPartitionSpec dynamicPartition(List<String> partitionNames, boolean isTemporary) {
        return new InsertPartitionSpec(null, partitionNames, isTemporary, false);
    }

    /** Static partition with values: PARTITION (col='val', ...) */
    public static InsertPartitionSpec staticPartition(Map<String, Expression> staticValues, boolean isTemporary) {
        return new InsertPartitionSpec(staticValues, null, isTemporary, false);
    }

    /** Check if this is a static partition with key-value pairs */
    public boolean isStaticPartition() {
        return staticPartitionValues != null && !staticPartitionValues.isEmpty();
    }

    /** Check if this has dynamic partition names */
    public boolean hasDynamicPartitionNames() {
        return partitionNames != null && !partitionNames.isEmpty();
    }

    public Map<String, Expression> getStaticPartitionValues() {
        return staticPartitionValues;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    public boolean isAutoDetect() {
        return isAutoDetect;
    }
}
