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

package org.apache.doris.datasource.iceberg.action;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.ArgumentParsers;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergMetadataOps;
import org.apache.doris.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.expressions.Expression;

import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.PartitionStatsHandler;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Computes and registers Iceberg partition statistics for a selected snapshot. */
public class IcebergComputePartitionStatsAction extends BaseIcebergAction {
    private static final String SNAPSHOT_ID = "snapshot_id";
    private final ExecutionAuthenticator authenticator;

    public IcebergComputePartitionStatsAction(Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo, Optional<Expression> whereCondition,
            IcebergMetadataOps metadataOps) {
        super(IcebergExecuteActionFactory.COMPUTE_PARTITION_STATS, properties, partitionNamesInfo,
                whereCondition, metadataOps);
        this.authenticator = metadataOps.getExecutionAuthenticator();
    }

    @Override
    protected void registerIcebergArguments() {
        namedArguments.registerOptionalArgument(SNAPSHOT_ID,
                "Snapshot ID to compute partition statistics for (defaults to the current snapshot)",
                null, ArgumentParsers.longRange(SNAPSHOT_ID, Long.MIN_VALUE, Long.MAX_VALUE));
    }

    @Override
    protected void validateIcebergAction() throws UserException {
        validateNoPartitions();
        validateNoWhereCondition();
    }

    @Override
    protected List<Column> getResultSchema() {
        return Collections.singletonList(new Column("partition_statistics_file", Type.STRING, true,
                "Path of the partition statistics file"));
    }

    @Override
    protected List<List<String>> executeAction(TableIf table) throws UserException {
        IcebergExternalTable dorisTable = (IcebergExternalTable) table;
        // Let the command retry a catalog-generation fence before any statistics file is written.
        Table icebergTable = getWritableIcebergTable(table);
        try {
            Long requestedSnapshotId = namedArguments.getLong(SNAPSHOT_ID);
            Snapshot snapshot;
            if (requestedSnapshotId != null) {
                snapshot = icebergTable.snapshot(requestedSnapshotId);
                if (snapshot == null) {
                    throw new UserException("Snapshot not found: " + requestedSnapshotId);
                }
            } else {
                snapshot = icebergTable.currentSnapshot();
                if (snapshot == null) {
                    return Collections.emptyList();
                }
            }

            // Keep the selected snapshot even if the table head changes during computation.
            long snapshotId = snapshot.snapshotId();
            Table statisticsTable = new IcebergPartitionStatsTable(icebergTable, authenticator);
            PartitionStatisticsFile file = PartitionStatsHandler.computeAndWriteStatsFile(statisticsTable, snapshotId);
            if (file == null) {
                return Collections.emptyList();
            }

            // Follow the SDK commit contract, including reuse of an existing statistics file.
            icebergTable.updatePartitionStatistics().setPartitionStatistics(file).commit();
            Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache(dorisTable);
            return Collections.singletonList(Collections.singletonList(file.path()));
        } catch (Exception e) {
            // The commit may have succeeded: do not remove files on failure or unknown commit state.
            throw new UserException("Failed to compute partition statistics: " + e.getMessage(), e);
        }
    }

    @Override
    public String getDescription() {
        return "Compute and register Iceberg partition statistics";
    }
}
