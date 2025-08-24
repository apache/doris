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
import org.apache.doris.analysis.AlterTableClause;
import org.apache.doris.analysis.DropMultiPartitionClause;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.MaxLiteral;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * DropMultiPartitionOp
 */
public class DropMultiPartitionOp extends AlterTableOp {
    private boolean ifExists;
    private final List<Expression> fromExpression;
    private final List<Expression> toExpression;
    private final String unit;
    private final String unitString;
    // true if this is to drop a temp partition
    private boolean isTempPartition;
    private boolean forceDrop;

    /**
     * DropMultiPartitionOp
     */
    public DropMultiPartitionOp(boolean ifExists, boolean forceDrop, List<Expression> fromExpression,
                                List<Expression> toExpression, String unit, String unitString,
                                boolean isTempPartition) {
        super(AlterOpType.DROP_PARTITION);
        this.ifExists = ifExists;
        this.fromExpression = fromExpression;
        this.toExpression = toExpression;
        this.unit = unit;
        this.unitString = unitString;
        this.forceDrop = forceDrop;
        this.isTempPartition = isTempPartition;
        this.needTableStable = false;
    }

    public boolean isSetIfExists() {
        return ifExists;
    }

    public boolean isTempPartition() {
        return isTempPartition;
    }

    public boolean isForceDrop() {
        return forceDrop;
    }

    @Override
    public void validate(ConnectContext ctx) throws UserException {
    }

    @Override
    public AlterTableClause translateToLegacyAlterClause() {
        List<PartitionValue> fromValues = fromExpression.stream()
                .map(this::toLegacyPartitionValue)
                .collect(Collectors.toList());
        List<PartitionValue> toValues = toExpression.stream()
                .map(this::toLegacyPartitionValue)
                .collect(Collectors.toList());
        PartitionKeyDesc partitionKeyDesc = (unit != null ? (unitString == null
                ? PartitionKeyDesc.createMultiFixed(fromValues, toValues, Long.parseLong(unit))
                : PartitionKeyDesc.createMultiFixed(fromValues, toValues, Long.parseLong(unit), unitString))
                : PartitionKeyDesc.createFixed(fromValues, toValues));
        return new DropMultiPartitionClause(ifExists, forceDrop, partitionKeyDesc, isTempPartition);
    }

    @Override
    public Map<String, String> getProperties() {
        return null;
    }

    @Override
    public boolean allowOpMTMV() {
        return false;
    }

    @Override
    public boolean needChangeMTMVState() {
        return false;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder("[");
        sb.append(getPartitionValuesStr(fromExpression)).append(", ").append(getPartitionValuesStr(toExpression));
        sb.append(")");
        return String.format("DROP PARTITION %s", sb);
    }

    @Override
    public String toString() {
        return toSql();
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
