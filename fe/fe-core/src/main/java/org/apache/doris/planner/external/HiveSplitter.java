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
import org.apache.doris.external.hive.util.HiveUtil;
import org.apache.doris.planner.ColumnRange;
import org.apache.doris.planner.ListPartitionPrunerV2;
import org.apache.doris.planner.Split;
import org.apache.doris.planner.Splitter;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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
            Map<String, String> properties = hmsTable.getCatalog().getCatalogProperty().getProperties();
            boolean useSelfSplitter = true;
            if (properties.containsKey(HMSExternalCatalog.ENABLE_SELF_SPLITTER)
                    && properties.get(HMSExternalCatalog.ENABLE_SELF_SPLITTER).equalsIgnoreCase("false")) {
                LOG.debug("Using self splitter for hmsTable {}", hmsTable.getName());
                useSelfSplitter = false;
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
                getFileSplitByPartitions(cache, partitions, allFiles, useSelfSplitter);
            } else {
                // unpartitioned table, create a dummy partition to save location and inputformat,
                // so that we can unify the interface.
                HivePartition dummyPartition = new HivePartition(hmsTable.getRemoteTable().getSd().getInputFormat(),
                        hmsTable.getRemoteTable().getSd().getLocation(), null);
                getFileSplitByPartitions(cache, Lists.newArrayList(dummyPartition), allFiles, useSelfSplitter);
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
                                          List<Split> allFiles, boolean useSelfSplitter) throws IOException {
        for (HiveMetaStoreCache.FileCacheValue fileCacheValue :
                cache.getFilesByPartitions(partitions, useSelfSplitter)) {
            if (fileCacheValue.getSplits() != null) {
                allFiles.addAll(fileCacheValue.getSplits());
            }
            if (fileCacheValue.getFiles() != null) {
                boolean isSplittable = fileCacheValue.isSplittable();
                for (HiveMetaStoreCache.HiveFileStatus status : fileCacheValue.getFiles()) {
                    allFiles.addAll(splitFile(status, isSplittable, fileCacheValue.getPartitionValues()));
                }
            }
        }
    }

    private List<Split> splitFile(HiveMetaStoreCache.HiveFileStatus status,
                                  boolean splittable, List<String> partitionValues) throws IOException {
        List<Split> result = Lists.newArrayList();
        if (!splittable) {
            LOG.debug("Path {} is not splittable.", status.getPath());
            BlockLocation block = status.getBlockLocations()[0];
            result.add(new FileSplit(status.getPath(), 0, status.getLength(),
                    status.getLength(), block.getHosts(), partitionValues));
            return result;
        }
        long splitSize = ConnectContext.get().getSessionVariable().getFileSplitSize();
        if (splitSize <= 0) {
            splitSize = status.getBlockSize();
        }
        // Min split size is DEFAULT_SPLIT_SIZE(128MB).
        splitSize = splitSize > DEFAULT_SPLIT_SIZE ? splitSize : DEFAULT_SPLIT_SIZE;
        BlockLocation[] blockLocations = status.getBlockLocations();
        long length = status.getLength();
        long bytesRemaining;
        for (bytesRemaining = length; (double) bytesRemaining / (double) splitSize > 1.1D;
                bytesRemaining -= splitSize) {
            int location = getBlockIndex(blockLocations, length - bytesRemaining);
            result.add(new FileSplit(status.getPath(), length - bytesRemaining,
                    splitSize, length, blockLocations[location].getHosts(), partitionValues));
        }
        if (bytesRemaining != 0L) {
            int location = getBlockIndex(blockLocations, length - bytesRemaining);
            result.add(new FileSplit(status.getPath(), length - bytesRemaining,
                    bytesRemaining, length, blockLocations[location].getHosts(), partitionValues));
        }

        LOG.debug("Path {} includes {} splits.", status.getPath(), result.size());
        return result;
    }

    public int getTotalPartitionNum() {
        return totalPartitionNum;
    }

    public int getReadPartitionNum() {
        return readPartitionNum;
    }

    // Get File Status by using FileSystem API.
    public static HiveMetaStoreCache.FileCacheValue getFileCache(Path path, InputFormat<?, ?> inputFormat,
                                                                 JobConf jobConf,
                                                                 List<String> partitionValues) throws IOException {
        FileSystem fs = path.getFileSystem(jobConf);
        boolean splittable = HiveUtil.isSplittable(inputFormat, fs, path);
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(path, false);
        HiveMetaStoreCache.FileCacheValue result = new HiveMetaStoreCache.FileCacheValue();
        result.setSplittable(splittable);
        while (locatedFileStatusRemoteIterator.hasNext()) {
            result.addFile(locatedFileStatusRemoteIterator.next());
        }
        result.setPartitionValues(partitionValues);
        return result;
    }

    private static int getBlockIndex(BlockLocation[] blkLocations, long offset) {
        for (int i = 0; i < blkLocations.length; ++i) {
            if (blkLocations[i].getOffset() <= offset
                    && offset < blkLocations[i].getOffset() + blkLocations[i].getLength()) {
                return i;
            }
        }
        BlockLocation last = blkLocations[blkLocations.length - 1];
        long fileLength = last.getOffset() + last.getLength() - 1L;
        throw new IllegalArgumentException(String.format("Offset %d is outside of file (0..%d)", offset, fileLength));
    }
}
