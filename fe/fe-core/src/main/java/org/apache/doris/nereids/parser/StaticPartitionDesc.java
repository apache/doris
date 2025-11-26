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

import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * Static partition description for INSERT OVERWRITE ... PARTITION (col='val', ...)
 */
public class StaticPartitionDesc {
    private final Map<String, Expression> partitionKeyValues;  // Static partition key-value pairs
    private final boolean isTemporary;                          // Whether it's a temporary partition
    private final List<String> partitionNames;                  // Dynamic partition names (for compatibility)

    public StaticPartitionDesc(Map<String, Expression> partitionKeyValues, boolean isTemporary,
            List<String> partitionNames) {
        this.partitionKeyValues = partitionKeyValues != null
                ? ImmutableMap.copyOf(partitionKeyValues) : null;
        this.isTemporary = isTemporary;
        this.partitionNames = partitionNames;
    }

    /**
     * Check if this is a static partition overwrite
     */
    public boolean isStaticPartitionOverwrite() {
        return partitionKeyValues != null && !partitionKeyValues.isEmpty();
    }

    public Map<String, Expression> getPartitionKeyValues() {
        return partitionKeyValues;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }
}

