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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class PartitionName {
    private final String tableName;
    private String newTableName;
    private String partitionName;
    private String newPartitionName;

    public PartitionName(String tableName, String newTableName,
                         String partitionName, String newPartitionName) {
        this.tableName = Strings.nullToEmpty(tableName);
        this.newTableName = Strings.nullToEmpty(newTableName);
        this.partitionName = Strings.nullToEmpty(partitionName);
        this.newPartitionName = Strings.nullToEmpty(newPartitionName);
    }

    /*
     * OK:
     * 1. t1                ==> t1    AS t1
     * 2. t1    AS t2       ==> t1    AS t2
     * 3. t1.p1             ==> t1.p1 AS t1.p1
     * 4. t1.p1 AS t2.p1    ==> t1.p1 AS t2.p1
     *
     * ERR:
     * 1. t1    AS t1.p1
     * 2. t1.p1 AS t1
     * 3. t1.p1 AS t1.p2
     */
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (Strings.isNullOrEmpty(tableName)) {
            throw new AnalysisException("Table name is null");
        }

        if (!newTableName.isEmpty()) {
            // ERR 1/2
            if ((partitionName.isEmpty() && !newPartitionName.isEmpty())
                    || (newPartitionName.isEmpty() && !partitionName.isEmpty())) {
                throw new AnalysisException("Partition name is missing");
            }

            // ERR 3
            if (!partitionName.equalsIgnoreCase(newPartitionName)) {
                throw new AnalysisException("Do not support rename partition name");
            }
        }

        if (newTableName.isEmpty()) {
            newTableName = tableName;
            newPartitionName = partitionName;
        }
    }

    public String getTableName() {
        return tableName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public String getNewTableName() {
        return newTableName;
    }

    public String getNewPartitionName() {
        return newPartitionName;
    }

    public boolean onlyTable() {
        return partitionName.isEmpty();
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("`").append(tableName).append("`");
        if (!partitionName.isEmpty()) {
            sb.append(".`").append(partitionName).append("`");
        }

        sb.append(" AS ").append("`").append(newTableName).append("`");
        if (!partitionName.isEmpty()) {
            sb.append(".`").append(newPartitionName).append("`");
        }

        return sb.toString();
    }

    private static void checkIntersectInternal(List<Pair<String, String>> pairs) throws AnalysisException {
        Map<String, Set<String>> tablePartitionMap = Maps.newHashMap();
        Pair<String, String> duplicatedObj = null;
        for (Pair<String, String> pair : pairs) {
            String tbl = pair.first;
            String partition = pair.second;
            if (pair.second.isEmpty()) {
                // only table
                if (tablePartitionMap.containsKey(tbl)) {
                    duplicatedObj = pair;
                    break;
                } else {
                    Set<String> emptySet = Sets.newHashSet();
                    tablePartitionMap.put(tbl, emptySet);
                }
            } else {
                Set<String> partitions = tablePartitionMap.get(tbl);
                if (partitions != null) {
                    if (partitions.isEmpty()) {
                        duplicatedObj = pair;
                        break;
                    } else {
                        if (partitions.contains(partition)) {
                            duplicatedObj = pair;
                            break;
                        } else {
                            partitions.add(partition);
                        }
                    }
                } else {
                    partitions = Sets.newHashSet();
                    partitions.add(partition);
                    tablePartitionMap.put(tbl, partitions);
                }
            }
        } // end for

        if (duplicatedObj != null) {
            throw new AnalysisException("Duplicated restore object: " + duplicatedObj);
        }
    }

    public static void checkIntersect(List<PartitionName> partitionNames)
            throws AnalysisException {
        List<Pair<String, String>> oldPairs = Lists.newArrayList();
        List<Pair<String, String>> newPairs = Lists.newArrayList();
        Map<String, String> tableRenameMap = Maps.newHashMap();
        for (PartitionName partitionName : partitionNames) {
            Pair<String, String> oldPair = Pair.of(partitionName.getTableName(),
                                                                    partitionName.getPartitionName());
            oldPairs.add(oldPair);
            Pair<String, String> newPair = Pair.of(partitionName.getNewTableName(),
                                                                    partitionName.getNewPartitionName());
            newPairs.add(newPair);

            if (tableRenameMap.containsKey(partitionName.getTableName())) {
                if (!tableRenameMap.get(partitionName.getTableName()).equals(partitionName.getNewTableName())) {
                    throw new AnalysisException("Can not rename table again: " + partitionName.getTableName());
                }
            } else {
                tableRenameMap.put(partitionName.getTableName(), partitionName.getNewTableName());
            }
        }

        checkIntersectInternal(oldPairs);
        checkIntersectInternal(newPairs);
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof TableName) {
            return toString().equals(other.toString());
        }
        return false;
    }

}
