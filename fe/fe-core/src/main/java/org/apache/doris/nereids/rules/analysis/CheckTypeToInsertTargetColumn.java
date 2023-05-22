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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.stream.Collectors;

/**
 * adjust output for insert target type
 */
public class CheckTypeToInsertTargetColumn extends OneAnalysisRuleFactory {
    @Override
    public Rule build() {
        return logicalOlapTableSink().then(sink -> {
            List<DataType> insertTargetTypes = sink.getCols().stream()
                    .filter(Column::isVisible)
                    .map(col -> DataType.fromCatalogType(col.getType()))
                    .collect(Collectors.toList());
            List<Slot> outputs = sink.child().getOutput();
            checkNeedCast(insertTargetTypes, outputs);
            return sink;
        }).toRule(RuleType.CHECK_TYPE_TO_INSERT_TARGET_COLUMN);
    }

    private void checkNeedCast(List<DataType> targetType, List<Slot> slots) {
        Preconditions.checkArgument(targetType.size() == slots.size(),
                String.format("insert target table contains %d slots, but source table contains %d slots",
                        targetType.size(), slots.size()));
        for (int i = 0; i < targetType.size(); i++) {
            TypeCoercionUtils.checkCanCastTo(slots.get(i).getDataType(), targetType.get(i));
        }
    }
}
