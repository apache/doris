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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.EsTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.external.elasticsearch.EsShardPartitions;
import org.apache.doris.external.elasticsearch.EsShardRouting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public class EsShardProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("ShardId").add("Host").add("IsPrimary")
            .build();

    private Database db;
    private EsTable esTable;
    private String indexName;
    
    public EsShardProcDir(Database db, EsTable esTable, String indexName) {
        this.db = db;
        this.esTable = esTable;
        this.indexName = indexName;
    }
    
    @Override
    public ProcResult fetchResult() {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(esTable);
        Preconditions.checkNotNull(indexName);

        List<List<Comparable>> shardInfos = new ArrayList<List<Comparable>>();
        esTable.readLock();
        try {
            // get infos
            EsShardPartitions esShardPartitions = esTable.getEsTablePartitions().getEsShardPartitions(indexName);
            for (int shardId : esShardPartitions.getShardRoutings().keySet()) {
                List<EsShardRouting> shardRoutings = esShardPartitions.getShardRoutings().get(shardId);
                if (shardRoutings != null && shardRoutings.size() > 0) {
                    for (EsShardRouting esShardRouting : shardRoutings) {
                        List<Comparable> shardInfo = new ArrayList<Comparable>();
                        shardInfo.add(shardId);
                        shardInfo.add(esShardRouting.getAddress().toString());
                        shardInfo.add(esShardRouting.isPrimary());
                        shardInfos.add(shardInfo);
                    }
                } else {
                    List<Comparable> shardInfo = new ArrayList<Comparable>();
                    shardInfo.add(shardId);
                    shardInfo.add("");
                    shardInfo.add(false);
                    shardInfos.add(shardInfo);
                }
            }
        } finally {
            esTable.readUnlock();
        }

        // sort by tabletId, replicaId
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0, 1);
        Collections.sort(shardInfos, comparator);

        // set result
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        for (int i = 0; i < shardInfos.size(); i++) {
            List<Comparable> info = shardInfos.get(i);
            List<String> row = new ArrayList<String>(info.size());
            for (int j = 0; j < info.size(); j++) {
                row.add(info.get(j).toString());
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
    public ProcNodeInterface lookup(String shardIdStr) throws AnalysisException {
        return null;
    }

}
