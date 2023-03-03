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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Index;
import org.apache.doris.thrift.TAlterInvertedIndexReq;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TOlapTableIndex;
import org.apache.doris.thrift.TTaskType;

import java.util.ArrayList;
import java.util.List;

/*
 * This task is used for alter table process, such as rollup and schema change
 * The task will do data transformation from base replica to new replica.
 * The new replica should be created before.
 * The new replica can be a rollup replica, or a shadow replica of schema change.
 */
public class AlterInvertedIndexTask extends AgentTask {
    private long tabletId;
    private long version;
    private long jobId;
    private AlterJobV2.JobType jobType;
    private int schemaHash;
    private boolean isDropOp = false;
    private List<Index> alterInvertedIndexes;
    List<Index> indexes;
    private List<Column> schemaColumns;
    private long expiration;

    public AlterInvertedIndexTask(long backendId, long dbId, long tableId,
            long partitionId, long indexId, long version,
            long tabletId, int schemaHash,
            long jobId, AlterJobV2.JobType jobType,
            boolean isDropOp, List<Index> alterInvertedIndexes,
            List<Index> indexes, List<Column> schemaColumns, long expiration) {
        super(null, backendId, TTaskType.ALTER_INVERTED_INDEX, dbId, tableId, partitionId, indexId, tabletId);
        this.tabletId = tabletId;
        this.version = version;
        this.jobId = jobId;
        this.jobType = jobType;
        this.schemaHash = schemaHash;
        this.isDropOp = isDropOp;
        this.alterInvertedIndexes = alterInvertedIndexes;
        this.indexes = indexes;
        this.schemaColumns = schemaColumns;
        this.expiration = expiration;
    }

    public long getTabletId() {
        return tabletId;
    }

    public long getVersion() {
        return version;
    }

    public long getJobId() {
        return jobId;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public AlterJobV2.JobType getJobType() {
        return jobType;
    }

    public List<Index> getAlterInvertedIndexes() {
        return alterInvertedIndexes;
    }

    public TAlterInvertedIndexReq toThrift() {
        TAlterInvertedIndexReq req = new TAlterInvertedIndexReq();
        req.setTabletId(tabletId);
        req.setAlterVersion(version);
        req.setSchemaHash(schemaHash);
        req.setIsDropOp(isDropOp);
        req.setJobId(jobId);
        req.setExpiration(expiration);

        if (!alterInvertedIndexes.isEmpty()) {
            List<TOlapTableIndex> tIndexes = new ArrayList<>();
            for (Index index : alterInvertedIndexes) {
                tIndexes.add(index.toThrift());
            }
            req.setAlterInvertedIndexes(tIndexes);
        }

        if (indexes != null && !indexes.isEmpty()) {
            List<TOlapTableIndex> tIndexes = new ArrayList<>();
            for (Index index : indexes) {
                tIndexes.add(index.toThrift());
            }
            req.setIndexes(tIndexes);
        }

        if (schemaColumns != null) {
            List<TColumn> columns = new ArrayList<TColumn>();
            for (Column column : schemaColumns) {
                columns.add(column.toThrift());
            }
            req.setColumns(columns);
        }
        return req;
    }
}
