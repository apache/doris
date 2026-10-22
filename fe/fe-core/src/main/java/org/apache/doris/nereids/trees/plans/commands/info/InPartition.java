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

import org.apache.doris.analysis.AllPartitionDesc;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.SinglePartitionDesc;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * represent in partition
 */
public class InPartition extends PartitionDefinition {
    private final List<List<Expression>> values;

    public InPartition(boolean ifNotExists, String partitionName, List<List<Expression>> values) {
        super(ifNotExists, partitionName);
        this.values = values;
    }

    @Override
    public void validate(Map<String, String> properties) {
        super.validate(properties);
        try {
            FeNameFormat.checkPartitionName(partitionName);
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e.getCause());
        }
        checkNoMaxValue();
    }

    /**
     * MAXVALUE is only meaningful as the open upper bound of a RANGE partition
     * ('VALUES LESS THAN (MAXVALUE)'). A LIST partition enumerates concrete values, so a
     * MAXVALUE key can never be matched on load and breaks partition serialization and
     * pruning afterwards. Reject it at DDL time; tables created by older versions keep
     * working (see the load/prune handling that skips MAXVALUE keys).
     */
    private void checkNoMaxValue() {
        if (DebugPointUtil.isEnable("FE.skipCheckMaxValueInListPartition")) {
            return;
        }
        for (List<Expression> item : values) {
            for (Expression value : item) {
                if (value instanceof PartitionDefinition.MaxValue) {
                    throw new AnalysisException(String.format(
                            "MAXVALUE is not allowed in LIST partition '%s', got VALUES IN (%s). "
                                    + "MAXVALUE can only be used in RANGE partition with "
                                    + "'VALUES LESS THAN (MAXVALUE)'. Please use explicit values or NULL instead.",
                            partitionName,
                            item.stream().map(InPartition::valueToSql).collect(Collectors.joining(", "))));
                }
            }
        }
    }

    private static String valueToSql(Expression value) {
        return value instanceof PartitionDefinition.MaxValue ? "MAXVALUE" : value.toSql();
    }

    @Override
    public String getPartitionName() {
        return partitionName;
    }

    @Override
    public AllPartitionDesc translateToCatalogStyle() {
        if (values.isEmpty()) {
            // add a empty list for default value process
            values.add(new ArrayList<>());
        }
        List<List<PartitionValue>> catalogValues = values.stream().map(l -> l.stream()
                .map(this::toLegacyPartitionValueStmt)
                .collect(Collectors.toList())).collect(Collectors.toList());
        return new SinglePartitionDesc(ifNotExists, partitionName,
                PartitionKeyDesc.createIn(catalogValues), replicaAllocation, properties,
                partitionDataProperty, isInMemory, tabletType, versionInfo, storagePolicy,
                isMutable);
    }

    public List<List<Expression>> getValues() {
        return values;
    }
}
