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

import com.google.common.collect.Table;
import com.google.gson.annotations.SerializedName;
import org.apache.doris.catalog.*;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class CooldownJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(CooldownJob.class);

    @SerializedName(value = "dbId")
    protected long dbId;
    @SerializedName(value = "tableId")
    protected long tableId;
    @SerializedName(value = "partitionId")
    protected long partitionId;
    @SerializedName(value = "indexId")
    protected long indexId;
    @SerializedName(value = "tabletId")
    protected long tabletId;
    @SerializedName(value = "replicaId")
    protected long replicaId;
    @SerializedName(value = "replicaId")
    protected long replicaId;

    public CooldownJob(long jobId, JobType jobType, long dbId, long tableId, String tableName, long timeoutMs) {
        super(jobId, jobType, dbId, tableId, tableName, timeoutMs);
    }

    protected CooldownJob(JobType type) {
        super(type);
    }

    @Override
    protected void runPendingJob() throws AlterCancelException {

    }

    @Override
    protected void runWaitingTxnJob() throws AlterCancelException {

    }

    @Override
    protected void runRunningJob() throws AlterCancelException {

    }

    @Override
    protected boolean cancelImpl(String errMsg) {
        return false;
    }

    @Override
    protected void getInfo(List<List<Comparable>> infos) {

    }

    @Override
    public void replay(AlterJobV2 replayedJob) {
        try {
            CooldownJob replayedCooldownJob = (CooldownJob) replayedJob;
            switch (replayedJob.jobState) {
                case PENDING:
                    replayCreateJob(replayedCooldownJob);
                    break;
                case WAITING_TXN:
                    replayPendingJob(replayedSchemaChangeJob);
                    break;
                case FINISHED:
                    replayRunningJob(replayedSchemaChangeJob);
                    break;
                case CANCELLED:
                    replayCancelled(replayedSchemaChangeJob);
                    break;
                default:
                    break;
            }
        } catch (MetaNotFoundException e) {
            LOG.warn("[INCONSISTENT META] replay schema change job failed {}", replayedJob.getJobId(), e);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this, AlterJobV2.class);
        Text.writeString(out, json);
    }

    /**
     * Replay job in PENDING state.
     * Should replay all changes before this job's state transfer to PENDING.
     * These changes should be same as changes in CooldownHandler.createJob()
     */
    private void replayCreateJob(CooldownJob replayedJob) throws MetaNotFoundException {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(tableId, TableIf.TableType.OLAP);
        olapTable.writeLock();
        try {
            TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
            for (Table.Cell<Long, Long, MaterializedIndex> cell : partitionIndexMap.cellSet()) {
                long partitionId = cell.getRowKey();
                long shadowIndexId = cell.getColumnKey();
                MaterializedIndex shadowIndex = cell.getValue();

                TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(partitionId).getStorageMedium();
                TabletMeta shadowTabletMeta = new TabletMeta(dbId, tableId, partitionId, shadowIndexId,
                    indexSchemaVersionAndHashMap.get(shadowIndexId).schemaHash, medium);

                for (Tablet shadownTablet : shadowIndex.getTablets()) {
                    invertedIndex.addTablet(shadownTablet.getId(), shadowTabletMeta);
                    for (Replica shadowReplica : shadownTablet.getReplicas()) {
                        invertedIndex.addReplica(shadownTablet.getId(), shadowReplica);
                    }
                }
            }

            // set table state
            olapTable.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        } finally {
            olapTable.writeUnlock();
        }

        this.watershedTxnId = replayedJob.watershedTxnId;
        jobState = JobState.WAITING_TXN;
        LOG.info("replay pending send cooldown conf job: {}, table id: {}", jobId, tableId);
    }

}
