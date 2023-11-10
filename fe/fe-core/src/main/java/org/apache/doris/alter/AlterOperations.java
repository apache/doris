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

package org.apache.doris.alter;

import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.ModifyTablePropertiesClause;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.PropertyAnalyzer;

import com.google.common.base.Joiner;

import java.util.EnumSet;
import java.util.List;

/*
 * AlterOperations contains a set alter operations generated from a AlterStmt's alter clause.
 * This class is mainly used to integrate these operation types and check whether they have conflicts.
 */
public class AlterOperations {
    private EnumSet<AlterOpType> currentOps = EnumSet.noneOf(AlterOpType.class);

    public AlterOperations() {
    }

    public EnumSet<AlterOpType> getCurrentOps() {
        return currentOps;
    }

    // check the conflicts of the given list of alter clauses
    public void checkConflict(List<AlterClause> alterClauses) throws DdlException {
        for (AlterClause alterClause : alterClauses) {
            checkOp(alterClause.getOpType());
        }
    }

    // some operations take up disk space. so we need to check the disk capacity before processing.
    // return true if we see these kind of operations.
    public boolean needCheckCapacity() {
        for (AlterOpType currentOp : currentOps) {
            if (currentOp.needCheckCapacity()) {
                return true;
            }
        }
        return false;
    }

    public boolean hasPartitionOp() {
        return currentOps.contains(AlterOpType.ADD_PARTITION)
                || currentOps.contains(AlterOpType.DROP_PARTITION)
                || currentOps.contains(AlterOpType.REPLACE_PARTITION)
                || currentOps.contains(AlterOpType.MODIFY_PARTITION);
    }

    public boolean checkTableStoragePolicy(List<AlterClause> alterClauses) {
        return alterClauses.stream().filter(clause ->
            clause instanceof ModifyTablePropertiesClause
        ).anyMatch(clause -> clause.getProperties().containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_POLICY));
    }

    public String getTableStoragePolicy(List<AlterClause> alterClauses) {
        return alterClauses.stream().filter(clause ->
            clause instanceof ModifyTablePropertiesClause
        ).map(c -> ((ModifyTablePropertiesClause) c).getStoragePolicy()).findFirst().orElse("");
    }

    public boolean checkIsBeingSynced(List<AlterClause> alterClauses) {
        return alterClauses.stream().filter(clause ->
            clause instanceof ModifyTablePropertiesClause
        ).anyMatch(clause -> clause.getProperties().containsKey(PropertyAnalyzer.PROPERTIES_IS_BEING_SYNCED));
    }

    public boolean checkMinLoadReplicaNum(List<AlterClause> alterClauses) {
        return alterClauses.stream().filter(clause ->
            clause instanceof ModifyTablePropertiesClause
        ).anyMatch(clause -> clause.getProperties().containsKey(PropertyAnalyzer.PROPERTIES_MIN_LOAD_REPLICA_NUM));
    }

    public boolean checkBinlogConfigChange(List<AlterClause> alterClauses) {
        return alterClauses.stream().filter(clause ->
            clause instanceof ModifyTablePropertiesClause
        ).anyMatch(clause -> clause.getProperties().containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE)
            || clause.getProperties().containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_TTL_SECONDS)
            || clause.getProperties().containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_BYTES)
            || clause.getProperties().containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_HISTORY_NUMS));
    }

    public boolean isBeingSynced(List<AlterClause> alterClauses) {
        return alterClauses.stream().filter(clause ->
            clause instanceof ModifyTablePropertiesClause
        ).map(c -> ((ModifyTablePropertiesClause) c).isBeingSynced()).findFirst().orElse(false);
    }

    // MODIFY_TABLE_PROPERTY is also processed by SchemaChangeHandler
    public boolean hasSchemaChangeOp() {
        return currentOps.contains(AlterOpType.SCHEMA_CHANGE) || currentOps.contains(AlterOpType.MODIFY_TABLE_PROPERTY);
    }

    public boolean hasRollupOp() {
        return currentOps.contains(AlterOpType.ADD_ROLLUP) || currentOps.contains(AlterOpType.DROP_ROLLUP);
    }

    public boolean hasRenameOp() {
        return currentOps.contains(AlterOpType.RENAME);
    }

    public boolean hasReplaceTableOp() {
        return currentOps.contains(AlterOpType.REPLACE_TABLE);
    }

    public boolean hasModifyBucketNumOp() {
        return currentOps.contains(AlterOpType.MODIFY_DISTRIBUTION);
    }

    public boolean contains(AlterOpType op) {
        return currentOps.contains(op);
    }

    // throw exception if the given operation has conflict with current operations.,
    private void checkOp(AlterOpType opType) throws DdlException {
        if (currentOps.isEmpty()) {
            currentOps.add(opType);
            return;
        }

        for (AlterOpType currentOp : currentOps) {
            if (!AlterOpType.COMPATIBILITY_MATRIX[currentOp.ordinal()][opType.ordinal()]) {
                throw new DdlException("Alter operation " + opType + " conflicts with operation " + currentOp);
            }
        }

        currentOps.add(opType);
    }

    public boolean hasEnableFeatureOP() {
        return currentOps.contains(AlterOpType.ENABLE_FEATURE);
    }

    @Override
    public String toString() {
        return Joiner.on(", ").join(currentOps);
    }


}
