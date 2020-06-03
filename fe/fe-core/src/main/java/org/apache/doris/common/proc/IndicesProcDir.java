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

package org.apache.doris.common.proc;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.TimeUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
 * SHOW PROC /dbs/dbId/tableId/partitions/partitionId
 * show index's detail info within a partition
 */
public class IndicesProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("IndexId").add("IndexName").add("State").add("LastConsistencyCheckTime")
            .build();

    private Database db;
    private OlapTable olapTable;
    private Partition partition;

    public IndicesProcDir(Database db, OlapTable olapTable, Partition partition) {
        this.db = db;
        this.olapTable = olapTable;
        this.partition = partition;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(partition);

        BaseProcResult result = new BaseProcResult();
        // get info
        List<List<Comparable>> indexInfos = new ArrayList<List<Comparable>>();
        olapTable.readLock();
        try {
            result.setNames(TITLE_NAMES);
            for (MaterializedIndex materializedIndex : partition.getMaterializedIndices(IndexExtState.ALL)) {
                List<Comparable> indexInfo = new ArrayList<Comparable>();
                indexInfo.add(materializedIndex.getId());
                indexInfo.add(olapTable.getIndexNameById(materializedIndex.getId()));
                indexInfo.add(materializedIndex.getState());
                indexInfo.add(TimeUtils.longToTimeString(materializedIndex.getLastCheckTime()));

                indexInfos.add(indexInfo);
            }

        } finally {
            olapTable.readUnlock();
        }

        // sort by index id
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0);
        Collections.sort(indexInfos, comparator);

        // set result
        for (List<Comparable> info : indexInfos) {
            List<String> row = new ArrayList<String>(info.size());
            for (Comparable comparable : info) {
                row.add(comparable.toString());
            }
            result.addRow(row);
        }
        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String indexIdStr) throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(partition);
        if (Strings.isNullOrEmpty(indexIdStr)) {
            throw new AnalysisException("Index id is null");
        }
        
        long indexId;
        try {
            indexId = Long.valueOf(indexIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid index id format: " + indexIdStr);
        }
        
        olapTable.readLock();
        try {
            MaterializedIndex materializedIndex = partition.getIndex(indexId);
            if (materializedIndex == null) {
                throw new AnalysisException("Index[" + indexId + "] does not exist.");
            }
            return new TabletsProcDir(olapTable, materializedIndex);
        } finally {
            olapTable.readUnlock();
        }
    }

}
