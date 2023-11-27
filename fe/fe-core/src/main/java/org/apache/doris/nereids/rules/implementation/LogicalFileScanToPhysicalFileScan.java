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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHash.StorageBucketHashType;
import org.apache.doris.nereids.properties.DistributionSpecStorageAny;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.logical.LogicalHudiScan;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Implementation rule that convert logical FileScan to physical FileScan.
 */
public class LogicalFileScanToPhysicalFileScan extends OneImplementationRuleFactory {
    @Override
    public Rule build() {
        return logicalFileScan().when(plan -> !(plan instanceof LogicalHudiScan)).then(fileScan ->
            new PhysicalFileScan(
                    fileScan.getRelationId(),
                    fileScan.getTable(),
                    fileScan.getQualifier(),
                    convertDistribution(fileScan),
                    Optional.empty(),
                    fileScan.getLogicalProperties(),
                    fileScan.getSelectedPartitions(),
                    fileScan.getTableSample(),
                    fileScan.getTableSnapshot())
        ).toRule(RuleType.LOGICAL_FILE_SCAN_TO_PHYSICAL_FILE_SCAN_RULE);
    }

    private DistributionSpec convertDistribution(LogicalFileScan fileScan) {
        TableIf table = fileScan.getTable();
        if (!(table instanceof HMSExternalTable)) {
            return DistributionSpecStorageAny.INSTANCE;
        }

        HMSExternalTable hmsExternalTable = (HMSExternalTable) table;
        DistributionInfo distributionInfo = hmsExternalTable.getDefaultDistributionInfo();
        if (distributionInfo instanceof HashDistributionInfo) {
            HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
            List<Slot> output = fileScan.getOutput();
            List<ExprId> hashColumns = Lists.newArrayList();
            for (Slot slot : output) {
                for (Column column : hashDistributionInfo.getDistributionColumns()) {
                    if (((SlotReference) slot).getColumn().get().equals(column)) {
                        hashColumns.add(slot.getExprId());
                    }
                }
            }
            StorageBucketHashType function = StorageBucketHashType.STORAGE_BUCKET_CRC32;
            if (hmsExternalTable.isBucketedTable()) {
                function = StorageBucketHashType.STORAGE_BUCKET_SPARK_MURMUR32;
            }
            return new DistributionSpecHash(hashColumns, DistributionSpecHash.ShuffleType.NATURAL,
                fileScan.getTable().getId(), -1, Collections.emptySet(), function);
        }

        return DistributionSpecStorageAny.INSTANCE;
    }
}
