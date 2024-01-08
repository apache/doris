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

package org.apache.doris.task;

import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.Config;
import org.apache.doris.thrift.TAlterMaterializedViewParam;
import org.apache.doris.thrift.TAlterTabletReqV2;
import org.apache.doris.thrift.TAlterTabletType;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/*
 * This task is used for alter table process, such as rollup and schema change
 * The task will do data transformation from base replica to new replica.
 * The new replica should be created before.
 * The new replica can be a rollup replica, or a shadow replica of schema change.
 */
public class AlterReplicaTask extends AgentTask {
    private long baseTabletId;
    private long newReplicaId;
    private int baseSchemaHash;
    private int newSchemaHash;
    private long version;
    private long jobId;
    private AlterJobV2.JobType jobType;

    private Map<String, Expr> defineExprs;
    private Expr whereClause;
    private DescriptorTable descTable;
    private List<Column> baseSchemaColumns;

    /**
     * AlterReplicaTask constructor.
     *
     */
    public AlterReplicaTask(long backendId, long dbId, long tableId, long partitionId, long rollupIndexId,
            long baseIndexId, long rollupTabletId, long baseTabletId, long newReplicaId, int newSchemaHash,
            int baseSchemaHash, long version, long jobId, AlterJobV2.JobType jobType, Map<String, Expr> defineExprs,
            DescriptorTable descTable, List<Column> baseSchemaColumns, Expr whereClause) {
        super(null, backendId, TTaskType.ALTER, dbId, tableId, partitionId, rollupIndexId, rollupTabletId);

        this.baseTabletId = baseTabletId;
        this.newReplicaId = newReplicaId;

        this.newSchemaHash = newSchemaHash;
        this.baseSchemaHash = baseSchemaHash;

        this.version = version;
        this.jobId = jobId;

        this.jobType = jobType;
        this.defineExprs = defineExprs;
        this.whereClause = whereClause;
        this.descTable = descTable;
        this.baseSchemaColumns = baseSchemaColumns;
    }

    public long getBaseTabletId() {
        return baseTabletId;
    }

    public long getNewReplicaId() {
        return newReplicaId;
    }

    public int getNewSchemaHash() {
        return newSchemaHash;
    }

    public int getBaseSchemaHash() {
        return baseSchemaHash;
    }

    public long getVersion() {
        return version;
    }

    public long getJobId() {
        return jobId;
    }

    public AlterJobV2.JobType getJobType() {
        return jobType;
    }

    public TAlterTabletReqV2 toThrift() {
        TAlterTabletReqV2 req = new TAlterTabletReqV2(baseTabletId, signature, baseSchemaHash, newSchemaHash);
        req.setAlterVersion(version);
        req.setBeExecVersion(Config.be_exec_version);
        switch (jobType) {
            case ROLLUP:
                req.setAlterTabletType(TAlterTabletType.ROLLUP);
                break;
            case SCHEMA_CHANGE:
                req.setAlterTabletType(TAlterTabletType.SCHEMA_CHANGE);
                break;
            default:
                break;
        }
        if (defineExprs != null) {
            for (Map.Entry<String, Expr> entry : defineExprs.entrySet()) {
                List<SlotRef> slots = Lists.newArrayList();
                entry.getValue().collect(SlotRef.class, slots);
                TAlterMaterializedViewParam mvParam = new TAlterMaterializedViewParam(entry.getKey());
                mvParam.setMvExpr(entry.getValue().treeToThrift());
                req.addToMaterializedViewParams(mvParam);
            }
        }
        if (whereClause != null) {
            TAlterMaterializedViewParam mvParam = new TAlterMaterializedViewParam(Column.WHERE_SIGN);
            mvParam.setMvExpr(whereClause.treeToThrift());
            req.addToMaterializedViewParams(mvParam);
        }
        req.setDescTbl(descTable.toThrift());

        if (baseSchemaColumns != null) {
            List<TColumn> columns = new ArrayList<TColumn>();
            for (Column column : baseSchemaColumns) {
                columns.add(column.toThrift());
            }
            req.setColumns(columns);
        }
        return req;
    }
}
