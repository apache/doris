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

import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.SinglePartitionDesc;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.stream.Collectors;

/**
 * represent less than partition
 */
public class LessThanPartition extends PartitionDefinition {
    private final String partitionName;
    private final List<Expression> values;
    private final boolean isMaxValue;
    
    public LessThanPartition(String partitionName, List<Expression> values, boolean isMaxValue) {
        this.partitionName = partitionName;
        this.values = values;
        this.isMaxValue = isMaxValue;
    }

    /**
     * translate to catalog objects.
     */
    public SinglePartitionDesc translateToCatalogStyle() {
        if (isMaxValue) {
            return new SinglePartitionDesc(false, partitionName, PartitionKeyDesc.createMaxKeyDesc(),
                    Maps.newHashMap());
        }
        List<PartitionValue> partitionValues = values.stream().map(e -> new PartitionValue(e.toSql()))
                .collect(Collectors.toList());
        return new SinglePartitionDesc(false, partitionName,
                PartitionKeyDesc.createLessThan(partitionValues), Maps.newHashMap());
    }
}
