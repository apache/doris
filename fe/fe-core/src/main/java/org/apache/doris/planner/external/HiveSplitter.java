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

package org.apache.doris.planner.external;

import org.apache.doris.analysis.Expr;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.datasource.hive.HivePartition;
import org.apache.doris.planner.ColumnRange;
import org.apache.doris.planner.ListPartitionPrunerV2;
import org.apache.doris.planner.Split;
import org.apache.doris.planner.Splitter;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HiveSplitter implements Splitter {

    private static final Logger LOG = LogManager.getLogger(HiveSplitter.class);

    private HMSExternalTable hmsTable;
    private Map<String, ColumnRange> columnNameToRange;
    private int totalPartitionNum = 0;
    private int readPartitionNum = 0;

    public HiveSplitter(HMSExternalTable hmsTable, Map<String, ColumnRange> columnNameToRange) {
        this.hmsTable = hmsTable;
        this.columnNameToRange = columnNameToRange;
    }

    @Override
    public List<Split> getSplits(List<Expr> exprs) throws UserException {
        long start = System.currentTimeMillis();
        try {
            HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                    .getMetaStoreCache((HMSExternalCatalog) hmsTable.getCatalog());
            // 1. get ListPartitionItems from cache
            HiveMetaStoreCache.HivePartitionValues hivePartitionValues = null;
            List<Type> partitionColumnTypes = hmsTable.getPartitionColumnTypes();
            if (!partitionColumnTypes.isEmpty()) {
                hivePartitionValues = cache.getPartitionValues(hmsTable.getDbName(), hmsTable.getName(),
                    partitionColumnTypes);
            }

            List<Split> allFiles = Lists.newArrayList();
            if (hivePartitionValues != null) {
                // 2. prune partitions by expr
                Map<Long, PartitionItem> idToPartitionItem = hivePartitionValues.getIdToPartitionItem();
                this.totalPartitionNum = idToPartitionItem.size();
                ListPartitionPrunerV2 pruner = new ListPartitionPrunerV2(idToPartitionItem,
                        hmsTable.getPartitionColumns(), columnNameToRange,
                        hivePartitionValues.getUidToPartitionRange(),
                        hivePartitionValues.getRangeToId(),
                        hivePartitionValues.getSingleColumnRangeMap(),
                        true);
                Collection<Long> filteredPartitionIds = pruner.prune();
                this.readPartitionNum = filteredPartitionIds.size();
                LOG.debug("hive partition fetch and prune for table {}.{} cost: {} ms",
                        hmsTable.getDbName(), hmsTable.getName(), (System.currentTimeMillis() - start));

                // 3. get partitions from cache
                List<List<String>> partitionValuesList = Lists.newArrayListWithCapacity(filteredPartitionIds.size());
                for (Long id : filteredPartitionIds) {
                    ListPartitionItem listPartitionItem = (ListPartitionItem) idToPartitionItem.get(id);
                    partitionValuesList.add(listPartitionItem.getItems().get(0).getPartitionValuesAsStringList());
                }
                List<HivePartition> partitions = cache.getAllPartitions(hmsTable.getDbName(), hmsTable.getName(),
                        partitionValuesList);
                // 4. get all files of partitions
                getFileSplitByPartitions(cache, partitions, allFiles);
            } else {
                // unpartitioned table, create a dummy partition to save location and inputformat,
                // so that we can unify the interface.
                HivePartition dummyPartition = new HivePartition(hmsTable.getRemoteTable().getSd().getInputFormat(),
                        hmsTable.getRemoteTable().getSd().getLocation(), null);
                getFileSplitByPartitions(cache, Lists.newArrayList(dummyPartition), allFiles);
                this.totalPartitionNum = 1;
                this.readPartitionNum = 1;
            }
            LOG.debug("get #{} files for table: {}.{}, cost: {} ms",
                    allFiles.size(), hmsTable.getDbName(), hmsTable.getName(), (System.currentTimeMillis() - start));
            return allFiles;
        } catch (Throwable t) {
            LOG.warn("get file split failed for table: {}", hmsTable.getName(), t);
            throw new UserException(
                "get file split failed for table: " + hmsTable.getName() + ", err: " + Util.getRootCauseMessage(t),
                t);
        }
    }

    private void getFileSplitByPartitions(HiveMetaStoreCache cache, List<HivePartition> partitions,
                                          List<Split> allFiles) {
        List<InputSplit> files = cache.getFilesByPartitions(partitions);
        if (LOG.isDebugEnabled()) {
            LOG.debug("get #{} files from #{} partitions: {}", files.size(), partitions.size(),
                    Joiner.on(",")
                    .join(files.stream().limit(10).map(f -> ((FileSplit) f).getPath())
                        .collect(Collectors.toList())));
        }
        allFiles.addAll(files.stream().map(file -> {
            FileSplit fs = (FileSplit) file;
            org.apache.doris.planner.external.FileSplit split = new org.apache.doris.planner.external.FileSplit();
            split.setPath(fs.getPath());
            split.setStart(fs.getStart());
            // file size of orc files is not correct get by FileSplit.getLength(),
            // broker reader needs correct file size
            if (fs instanceof OrcSplit) {
                split.setLength(((OrcSplit) fs).getFileLength());
            } else {
                split.setLength(fs.getLength());
            }
            return split;
        }).collect(Collectors.toList()));
    }

    public int getTotalPartitionNum() {
        return totalPartitionNum;
    }

    public int getReadPartitionNum() {
        return readPartitionNum;
    }
}
