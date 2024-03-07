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

import org.apache.doris.analysis.MultiPartitionDesc;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * represent step partition
 */
public class StepPartition extends PartitionDefinition {
    private final List<Expression> fromExpression;
    private final List<Expression> toExpression;
    private final long unit;
    private final String unitString;

    public StepPartition(boolean ifNotExists, String partitionName, List<Expression> fromExpression,
            List<Expression> toExpression, long unit, String unitString) {
        super(ifNotExists, partitionName);
        this.fromExpression = fromExpression;
        this.toExpression = toExpression;
        this.unit = unit;
        this.unitString = unitString;
    }

    @Override
    public void validate(Map<String, String> properties) {
        super.validate(properties);
        if (fromExpression.stream().anyMatch(MaxValue.class::isInstance)
                || toExpression.stream().anyMatch(MaxValue.class::isInstance)) {
            throw new AnalysisException("MAXVALUE cannot be used in step partition");
        }
    }

    /**
     * translate to catalog objects.
     */
    public MultiPartitionDesc translateToCatalogStyle() {
        List<PartitionValue> fromValues = fromExpression.stream()
                .map(this::toLegacyPartitionValueStmt)
                .collect(Collectors.toList());
        List<PartitionValue> toValues = toExpression.stream()
                .map(this::toLegacyPartitionValueStmt)
                .collect(Collectors.toList());
        try {
            if (unitString == null) {
                return new MultiPartitionDesc(PartitionKeyDesc.createMultiFixed(fromValues, toValues, unit),
                        replicaAllocation, properties);
            }
            return new MultiPartitionDesc(PartitionKeyDesc.createMultiFixed(fromValues, toValues, unit, unitString),
                    replicaAllocation, properties);
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e.getCause());
        }
    }
}
