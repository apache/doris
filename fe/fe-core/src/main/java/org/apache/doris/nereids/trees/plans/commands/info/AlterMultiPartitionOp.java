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

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.MaxLiteral;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * AlterMultiPartitionOp
 */
public class AlterMultiPartitionOp extends AlterTableOp {
    private final List<Expression> fromExpression;
    private final List<Expression> toExpression;
    private final long unit;
    private final String unitString;
    private Map<String, String> properties;
    private boolean isTempPartition;
    private List<DataType> partitionTypes;

    /**
     * AlterMultiPartitionOp
     */
    public AlterMultiPartitionOp(List<Expression> fromExpression,
            List<Expression> toExpression, long unit, String unitString, Map<String, String> properties,
            boolean isTempPartition) {
        super(AlterOpType.ADD_PARTITION);
        this.fromExpression = fromExpression;
        this.toExpression = toExpression;
        this.unit = unit;
        this.unitString = unitString;
        this.properties = properties;
        this.isTempPartition = isTempPartition;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder("[");
        sb.append(getPartitionValuesStr(fromExpression)).append(", ").append(getPartitionValuesStr(toExpression));
        sb.append(")");
        return String.format("ADD PARTITIONS %s", sb);
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public boolean allowOpMTMV() {
        return false;
    }

    @Override
    public boolean needChangeMTMVState() {
        return false;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * getPartitionKeyDesc
     */
    public PartitionKeyDesc getPartitionKeyDesc() {
        validateBoundaryValueCount();
        List<PartitionValue> fromValues = new ArrayList<>();
        for (int i = 0; i < fromExpression.size(); i++) {
            Expression typedFrom = typedExpression(fromExpression.get(i), i);
            fromValues.add(toLegacyPartitionValue(typedFrom));
        }
        List<PartitionValue> toValues = new ArrayList<>();
        for (int i = 0; i < toExpression.size(); i++) {
            Expression typedTo = typedExpression(toExpression.get(i), i);
            toValues.add(toLegacyPartitionValue(typedTo));
        }
        PartitionKeyDesc partitionKeyDesc = (unitString == null
                ? PartitionKeyDesc.createMultiFixed(fromValues, toValues, unit)
                : PartitionKeyDesc.createMultiFixed(fromValues, toValues, unit, unitString));
        return partitionKeyDesc;
    }

    public void setPartitionTypes(List<DataType> partitionTypes) {
        this.partitionTypes = partitionTypes;
    }

    public boolean isTempPartition() {
        return isTempPartition;
    }

    private String getPartitionValuesStr(List<Expression> values) {
        StringBuilder sb = new StringBuilder("(");
        Joiner.on(", ").appendTo(sb, Lists.transform(values, v -> {
            if (v instanceof MaxLiteral) {
                return v.toSql();
            } else {
                return "'" + v.toSql() + "'";
            }
        })).append(")");
        return sb.toString();
    }

    private PartitionValue toLegacyPartitionValue(Expression typedExpression) {
        if (typedExpression.isLiteral()) {
            return new PartitionValue(((Literal) typedExpression).toLegacyLiteral(), typedExpression.isNullLiteral(),
                    typedExpression.isNullLiteral() ? null : ((Literal) typedExpression).getStringValue());
        } else if (typedExpression instanceof PartitionDefinition.MaxValue) {
            return PartitionValue.MAX_VALUE;
        }
        throw new AnalysisException("Unsupported partition value");
    }

    private void ensurePartitionTypesInitialized() {
        if (partitionTypes == null) {
            throw new AnalysisException("partitionTypes should be initialized before validating partition definition");
        }
    }

    private Expression typedExpression(Expression expression, int index) {
        ensurePartitionTypesInitialized();
        return PartitionDefinition.strictTypedPartitionExpression(expression, partitionTypes.get(index));
    }

    private void validateBoundaryValueCount() {
        ensurePartitionTypesInitialized();
        if (fromExpression.size() > partitionTypes.size() || toExpression.size() > partitionTypes.size()) {
            throw new AnalysisException("partition column number in multi partition clause must be one but start "
                    + "column size is " + fromExpression.size() + ", end column size is " + toExpression.size()
                    + ".");
        }
    }
}
