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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StatisticProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("DbId").add("DbName").add("TableNum").add("PartitionNum")
            .add("IndexNum").add("TabletNum").add("ReplicaNum")
            .build();
    private Env env;

    private ForkJoinPool taskPool = new ForkJoinPool();

    public StatisticProcNode(Env env) {
        Preconditions.checkNotNull(env);
        this.env = env;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        List<DBStatistic> statistics = taskPool.submit(() ->
                env.getInternalCatalog().getDbIds().parallelStream()
                    // skip information_schema database
                    .flatMap(id -> Stream.of(id == 0 ? null : env.getCatalogMgr().getDbNullable(id)))
                    .filter(Objects::nonNull).map(DBStatistic::new)
                    // sort by dbName
                    .sorted(Comparator.comparing(db -> db.db.getFullName())).collect(Collectors.toList())
        ).join();

        List<List<String>> rows = new ArrayList<>(statistics.size() + 1);
        for (DBStatistic statistic : statistics) {
            rows.add(statistic.toRow());
        }
        rows.add(statistics.stream().reduce(new DBStatistic(), DBStatistic::reduce).toRow());

        return new BaseProcResult(TITLE_NAMES, rows);
    }

    static class DBStatistic {
        boolean summary;
        DatabaseIf<TableIf> db;
        int dbNum;
        int tableNum;
        int partitionNum;
        int indexNum;
        int tabletNum;
        int replicaNum;

        DBStatistic() {
            this.summary = true;
        }

        DBStatistic(DatabaseIf db) {
            Preconditions.checkNotNull(db);
            this.summary = false;
            this.db = db;
            this.dbNum = 1;

            this.db.getTables().stream().filter(Objects::nonNull).forEach(t -> {
                ++tableNum;
                if (t.isManagedTable()) {
                    OlapTable olapTable = (OlapTable) t;
                    olapTable.readLock();
                    try {
                        for (Partition partition : olapTable.getAllPartitions()) {
                            ++partitionNum;
                            for (MaterializedIndex materializedIndex : partition.getMaterializedIndices(
                                    IndexExtState.VISIBLE)) {
                                ++indexNum;
                                List<Tablet> tablets = materializedIndex.getTablets();
                                for (int i = 0; i < tablets.size(); ++i) {
                                    Tablet tablet = tablets.get(i);
                                    ++tabletNum;
                                    replicaNum += tablet.getReplicas().size();
                                } // end for tablets
                            } // end for indices
                        } // end for partitions
                    } finally {
                        olapTable.readUnlock();
                    }
                }
            });
        }

        DBStatistic reduce(DBStatistic other) {
            if (this.summary) {
                this.dbNum += other.dbNum;
                this.tableNum += other.tableNum;
                this.partitionNum += other.partitionNum;
                this.indexNum += other.indexNum;
                this.tabletNum += other.tabletNum;
                this.replicaNum += other.replicaNum;
                return this;
            } else if (other.summary) {
                return other.reduce(this);
            } else {
                return new DBStatistic().reduce(this).reduce(other);
            }
        }

        List<String> toRow() {
            List<Object> row = new ArrayList<>(TITLE_NAMES.size());
            if (summary) {
                row.add("Total");
                row.add(dbNum);
            } else {
                row.add(db.getId());
                row.add(db.getFullName());
            }
            row.add(tableNum);
            row.add(partitionNum);
            row.add(indexNum);
            row.add(tabletNum);
            row.add(replicaNum);
            return row.stream().map(String::valueOf).collect(Collectors.toList());
        }
    }
}
