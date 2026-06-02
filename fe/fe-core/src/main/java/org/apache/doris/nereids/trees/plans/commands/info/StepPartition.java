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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
        validateBoundaryValueCount();
        for (Expression expression : fromExpression) {
            if (expression instanceof MaxValue) {
                throw new AnalysisException("MAXVALUE cannot be used in step partition");
            }
        }
        for (Expression expression : toExpression) {
            if (expression instanceof MaxValue) {
                throw new AnalysisException("MAXVALUE cannot be used in step partition");
            }
        }
    }

    /**
     * translate to catalog objects.
     */
    public MultiPartitionDesc translateToCatalogStyle() {
        validateBoundaryValueCount();
        List<PartitionValue> fromValues = new ArrayList<>();
        for (int i = 0; i < fromExpression.size(); i++) {
            Expression typedFrom = typedPartitionExpression(fromExpression.get(i), i);
            fromValues.add(toLegacyPartitionValueStmt(typedFrom));
        }
        List<PartitionValue> toValues = new ArrayList<>();
        for (int i = 0; i < toExpression.size(); i++) {
            Expression typedTo = typedPartitionExpression(toExpression.get(i), i);
            toValues.add(toLegacyPartitionValueStmt(typedTo));
        }
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

    public List<Expression> getFromExpression() {
        return fromExpression;
    }

    public List<Expression> getToExpression() {
        return toExpression;
    }

    private void validateBoundaryValueCount() {
        if (fromExpression.size() > partitionTypes.size() || toExpression.size() > partitionTypes.size()) {
            throw new AnalysisException("partition column number in multi partition clause must be one but start "
                    + "column size is " + fromExpression.size() + ", end column size is " + toExpression.size()
                    + ".");
        }
    }
}
