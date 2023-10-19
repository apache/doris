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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.SinglePartitionDesc;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * represent fixed range partition
 */
public class FixedRangePartition extends PartitionDefinition {
    private final String partitionName;
    private List<Expression> lowerBounds;
    private List<Expression> upperBounds;
    private ReplicaAllocation replicaAllocation = ReplicaAllocation.DEFAULT_ALLOCATION;

    public FixedRangePartition(String partitionName, List<Expression> lowerBounds, List<Expression> upperBounds) {
        this.partitionName = partitionName;
        this.lowerBounds = lowerBounds;
        this.upperBounds = upperBounds;
    }

    @Override
    public void validate(Map<String, String> properties) {
        super.validate(properties);
        final DataType type = partitionTypes.get(0);
        lowerBounds = lowerBounds.stream().map(e -> e.castTo(type)).collect(Collectors.toList());
        upperBounds = upperBounds.stream().map(e -> e.castTo(type)).collect(Collectors.toList());
    }

    public String getPartitionName() {
        return partitionName;
    }

    /**
     * translate to catalog objects.
     */
    public SinglePartitionDesc translateToCatalogStyle() {
        List<PartitionValue> lowerValues = lowerBounds.stream().map(this::toLegacyPartitionValueStmt)
                .collect(Collectors.toList());
        List<PartitionValue> upperValues = upperBounds.stream().map(this::toLegacyPartitionValueStmt)
                .collect(Collectors.toList());
        return new SinglePartitionDesc(false, partitionName,
                PartitionKeyDesc.createFixed(lowerValues, upperValues), replicaAllocation, Maps.newHashMap());
    }
}
