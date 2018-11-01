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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.ListComparator;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class StatisticProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("DbId").add("DbName").add("TableNum").add("PartitionNum")
            .add("IndexNum").add("TabletNum").add("ReplicaNum").add("IncompleteTabletNum")
            .add("InconsistentTabletNum")
            .build();

    private Catalog catalog;

    // db id -> set(tablet id)
    Multimap<Long, Long> incompleteTabletIds;
    // db id -> set(tablet id)
    Multimap<Long, Long> inconsistentTabletIds;

    public StatisticProcDir(Catalog catalog) {
        this.catalog = catalog;
        incompleteTabletIds = HashMultimap.create();
        inconsistentTabletIds = HashMultimap.create();
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(catalog);

        BaseProcResult result = new BaseProcResult();

        result.setNames(TITLE_NAMES);
        List<Long> dbIds = catalog.getDbIds();
        if (dbIds == null || dbIds.isEmpty()) {
            // empty
            return result;
        }

        // get alive backends
        Set<Long> aliveBackendIds = Sets.newHashSet(Catalog.getCurrentSystemInfo().getBackendIds(true));

        int totalDbNum = 0;
        int totalTableNum = 0;
        int totalPartitionNum = 0;
        int totalIndexNum = 0;
        int totalTabletNum = 0;
        int totalReplicaNum = 0;

        incompleteTabletIds.clear();
        inconsistentTabletIds.clear();
        List<List<Comparable>> lines = new ArrayList<List<Comparable>>();
        for (Long dbId : dbIds) {
            if (dbId == 0) {
                // skip information_schema database
                continue;
            }
            Database db = catalog.getDb(dbId);
            if (db == null) {
                continue;
            }

            ++totalDbNum;
            db.readLock();
            try {
                int dbTableNum = 0;
                int dbPartitionNum = 0;
                int dbIndexNum = 0;
                int dbTabletNum = 0;
                int dbReplicaNum = 0;

                for (Table table : db.getTables()) {
                    if (table.getType() != TableType.OLAP) {
                        continue;
                    }

                    ++dbTableNum;
                    OlapTable olapTable = (OlapTable) table;

                    for (Partition partition : olapTable.getPartitions()) {
                        short replicationNum = olapTable.getPartitionInfo().getReplicationNum(partition.getId());
                        ++dbPartitionNum;
                        for (MaterializedIndex materializedIndex : partition.getMaterializedIndices()) {
                            ++dbIndexNum;
                            for (Tablet tablet : materializedIndex.getTablets()) {
                                int onlineReplicaNum = 0;
                                ++dbTabletNum;
                                List<Replica> replicas = tablet.getReplicas();
                                dbReplicaNum += replicas.size();

                                for (Replica replica : tablet.getReplicas()) {
                                    ReplicaState state = replica.getState();
                                    if (state != ReplicaState.NORMAL && state != ReplicaState.SCHEMA_CHANGE) {
                                        continue;
                                    }
                                    if (!aliveBackendIds.contains(replica.getBackendId())) {
                                        continue;
                                    }
                                    ++onlineReplicaNum;
                                }

                                if (onlineReplicaNum < replicationNum) {
                                    incompleteTabletIds.put(dbId, tablet.getId());
                                }

                                if (!tablet.isConsistent()) {
                                    inconsistentTabletIds.put(dbId, tablet.getId());
                                }
                            } // end for tablets
                        } // end for indices
                    } // end for partitions
                } // end for tables

                List<Comparable> oneLine = new ArrayList<Comparable>(TITLE_NAMES.size());
                oneLine.add(dbId);
                oneLine.add(db.getFullName());
                oneLine.add(dbTableNum);
                oneLine.add(dbPartitionNum);
                oneLine.add(dbIndexNum);
                oneLine.add(dbTabletNum);
                oneLine.add(dbReplicaNum);
                oneLine.add(incompleteTabletIds.get(dbId).size());
                oneLine.add(inconsistentTabletIds.get(dbId).size());

                lines.add(oneLine);

                totalTableNum += dbTableNum;
                totalPartitionNum += dbPartitionNum;
                totalIndexNum += dbIndexNum;
                totalTabletNum += dbTabletNum;
                totalReplicaNum += dbReplicaNum;
            } finally {
                db.readUnlock();
            }
        } // end for dbs

        // sort by dbName
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(1);
        Collections.sort(lines, comparator);

        // add sum line after sort
        List<Comparable> finalLine = new ArrayList<Comparable>(TITLE_NAMES.size());
        finalLine.add("Total");
        finalLine.add(totalDbNum);
        finalLine.add(totalTableNum);
        finalLine.add(totalPartitionNum);
        finalLine.add(totalIndexNum);
        finalLine.add(totalTabletNum);
        finalLine.add(totalReplicaNum);
        finalLine.add(incompleteTabletIds.size());
        finalLine.add(inconsistentTabletIds.size());
        lines.add(finalLine);

        // add result
        for (List<Comparable> line : lines) {
            List<String> row = new ArrayList<String>(line.size());
            for (Comparable comparable : line) {
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
    public ProcNodeInterface lookup(String dbIdStr) throws AnalysisException {
        long dbId = -1L;
        try {
            dbId = Long.valueOf(dbIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid db id format: " + dbIdStr);
        }

        return new IncompleteTabletsProcNode(incompleteTabletIds.get(dbId), inconsistentTabletIds.get(dbId));
    }

    // used to metrics
    public static List<Integer> getStatistic() {
        int totalDbNum = 0;
        int totalTableNum = 0;
        int totalPartitionNum = 0;
        int totalIndexNum = 0;
        int totalTabletNum = 0;
        int totalReplicaNum = 0;
        int totalIncompleteTabletNum = 0;
        int totalInconsistentTabletNum = 0;

        List<Integer> result = Lists.newArrayList(0, 0, 0, 0, 0, 0, 0, 0);

        List<Long> dbIds = Catalog.getInstance().getDbIds();
        if (dbIds == null || dbIds.isEmpty()) {
            // empty
            return result;
        }

        // get alive backends
        Set<Long> aliveBackendIds = Sets.newHashSet(Catalog.getCurrentSystemInfo().getBackendIds(true));
        for (Long dbId : dbIds) {
            if (dbId == 0) {
                // skip information_schema database
                continue;
            }
            Database db = Catalog.getInstance().getDb(dbId);
            if (db == null) {
                continue;
            }

            ++totalDbNum;
            db.readLock();
            try {
                for (Table table : db.getTables()) {
                    if (table.getType() != TableType.OLAP) {
                        continue;
                    }

                    ++totalTableNum;
                    OlapTable olapTable = (OlapTable) table;

                    for (Partition partition : olapTable.getPartitions()) {
                        short replicationNum = olapTable.getPartitionInfo().getReplicationNum(partition.getId());
                        ++totalPartitionNum;
                        for (MaterializedIndex materializedIndex : partition.getMaterializedIndices()) {
                            ++totalIndexNum;
                            for (Tablet tablet : materializedIndex.getTablets()) {
                                int onlineReplicaNum = 0;
                                ++totalTabletNum;
                                List<Replica> replicas = tablet.getReplicas();
                                totalReplicaNum += replicas.size();

                                for (Replica replica : tablet.getReplicas()) {
                                    ReplicaState state = replica.getState();
                                    if (state != ReplicaState.NORMAL && state != ReplicaState.SCHEMA_CHANGE) {
                                        continue;
                                    }
                                    if (!aliveBackendIds.contains(replica.getBackendId())) {
                                        continue;
                                    }
                                    ++onlineReplicaNum;
                                }

                                if (onlineReplicaNum < replicationNum) {
                                    ++totalIncompleteTabletNum;
                                }

                                if (!tablet.isConsistent()) {
                                    ++totalInconsistentTabletNum;
                                }
                            } // end for tablets
                        } // end for indices
                    } // end for partitions
                } // end for tables
            } finally {
                db.readUnlock();
            }
        } // end for dbs

        result.add(totalDbNum);
        result.add(totalTableNum);
        result.add(totalPartitionNum);
        result.add(totalIndexNum);
        result.add(totalTabletNum);
        result.add(totalReplicaNum);
        result.add(totalIncompleteTabletNum);
        result.add(totalInconsistentTabletNum);

        return result;
    }
}
