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
import org.apache.doris.analysis.AlterMultiPartitionClause;
import org.apache.doris.analysis.AlterTableClause;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.MaxLiteral;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    @Override
    public AlterTableClause translateToLegacyAlterClause() {
        List<PartitionValue> fromValues = fromExpression.stream()
                .map(this::toLegacyPartitionValue)
                .collect(Collectors.toList());
        List<PartitionValue> toValues = toExpression.stream()
                .map(this::toLegacyPartitionValue)
                .collect(Collectors.toList());
        PartitionKeyDesc partitionKeyDesc = (unitString == null
                ? PartitionKeyDesc.createMultiFixed(fromValues, toValues, unit)
                : PartitionKeyDesc.createMultiFixed(fromValues, toValues, unit, unitString));
        return new AlterMultiPartitionClause(partitionKeyDesc, properties, isTempPartition);
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

    private PartitionValue toLegacyPartitionValue(Expression e) {
        if (e.isLiteral()) {
            return new PartitionValue(((Literal) e).getStringValue(), e.isNullLiteral());
        } else if (e instanceof PartitionDefinition.MaxValue) {
            return PartitionValue.MAX_VALUE;
        }
        throw new AnalysisException("Unsupported partition value");
    }
}
