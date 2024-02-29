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
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * represent less than partition
 */
public class LessThanPartition extends PartitionDefinition {
    private final List<Expression> values;

    public LessThanPartition(boolean ifNotExists, String partitionName, List<Expression> values) {
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
    }

    public String getPartitionName() {
        return partitionName;
    }

    /**
     * translate to catalog objects.
     */
    public SinglePartitionDesc translateToCatalogStyle() {
        if (values.get(0) instanceof MaxValue) {
            return new SinglePartitionDesc(ifNotExists, partitionName,
                    PartitionKeyDesc.createMaxKeyDesc(), replicaAllocation, properties,
                    partitionDataProperty, isInMemory, tabletType, versionInfo, storagePolicy,
                    isMutable);
        }
        List<PartitionValue> partitionValues =
                values.stream().map(this::toLegacyPartitionValueStmt).collect(Collectors.toList());
        return new SinglePartitionDesc(ifNotExists, partitionName,
                PartitionKeyDesc.createLessThan(partitionValues), replicaAllocation, properties,
                partitionDataProperty, isInMemory, tabletType, versionInfo, storagePolicy,
                isMutable);
    }
}
