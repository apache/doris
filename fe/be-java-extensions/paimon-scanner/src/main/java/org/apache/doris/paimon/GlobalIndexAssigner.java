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

package org.apache.doris.paimon;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.crosspartition.BucketAssigner;
import org.apache.paimon.crosspartition.ExistingProcessor;
import org.apache.paimon.crosspartition.IndexBootstrap;
import org.apache.paimon.crosspartition.KeyPartPartitionKeyExtractor;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.PartitionKeyExtractor;
import org.apache.paimon.table.sink.RowPartitionAllPrimaryKeyExtractor;
import org.apache.paimon.utils.IDMapping;
import org.apache.paimon.utils.PositiveIntInt;
import org.apache.paimon.utils.ProjectToRowFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Assigns buckets for a key-dynamic table with a process-local global-key index.
 *
 * <p>Doris gathers key-dynamic writes into one writer, so a single in-memory index can preserve
 * Paimon's cross-partition merge semantics without a native state backend.
 */
final class GlobalIndexAssigner implements AutoCloseable {
    private final FileStoreTable table;
    // TODO: After resolving rocksdbjni allocator compatibility with the Doris BE jemalloc hook,
    // use a RocksDB-backed on-disk index to bound Java heap usage for large tables.
    private final Map<BinaryRow, PositiveIntInt> keyIndex = new HashMap<>();

    private int bucketIndex;
    private int targetBucketRowNumber;
    private int assignId;
    private int numAssigners;
    private boolean bootstrapping;
    private BiConsumer<InternalRow, Integer> collector;
    private PartitionKeyExtractor<InternalRow> extractor;
    private PartitionKeyExtractor<InternalRow> bootstrapExtractor;
    private IDMapping<BinaryRow> partitionMapping;
    private BucketAssigner bucketAssigner;
    private ExistingProcessor existingProcessor;

    GlobalIndexAssigner(FileStoreTable table) {
        this.table = table;
    }

    void open(
            int numAssigners,
            int assignId,
            BiConsumer<InternalRow, Integer> collector) {
        this.numAssigners = numAssigners;
        this.assignId = assignId;
        this.collector = collector;

        CoreOptions coreOptions = table.coreOptions();
        this.bucketIndex = IndexBootstrap.bootstrapType(table.schema()).getFieldCount() - 1;
        this.targetBucketRowNumber =
                checkedTargetBucketRowNumber(coreOptions.dynamicBucketTargetRowNum());
        this.extractor = new RowPartitionAllPrimaryKeyExtractor(table.schema());
        this.bootstrapExtractor = new KeyPartPartitionKeyExtractor(table.schema());
        this.partitionMapping = new IDMapping<>(BinaryRow::copy);
        this.bucketAssigner = new BucketAssigner();
        this.existingProcessor =
                ExistingProcessor.create(
                        coreOptions.mergeEngine(),
                        new ProjectToRowFunction(table.rowType(), table.partitionKeys()),
                        bucketAssigner,
                        this::collect);
        this.bootstrapping = true;
    }

    static int checkedTargetBucketRowNumber(long targetBucketRowNumber) {
        if (targetBucketRowNumber <= 0 || targetBucketRowNumber > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Paimon dynamic-bucket.target-row-num must be between 1 and "
                            + Integer.MAX_VALUE + ", actual=" + targetBucketRowNumber);
        }
        return (int) targetBucketRowNumber;
    }

    void bootstrapKey(InternalRow value) {
        if (!bootstrapping) {
            throw new IllegalStateException("Paimon global index bootstrap has finished");
        }

        BinaryRow partition = bootstrapExtractor.partition(value);
        BinaryRow key = bootstrapExtractor.trimmedPrimaryKey(value);
        int partitionId = partitionMapping.index(partition);
        int bucket = value.getInt(bucketIndex);
        bucketAssigner.bootstrapBucket(partition, bucket);
        PositiveIntInt previous =
                keyIndex.putIfAbsent(key.copy(), new PositiveIntInt(partitionId, bucket));
        if (previous != null) {
            throw new IllegalStateException(
                    "Duplicate primary key found while bootstrapping a key-dynamic Paimon table; "
                            + "the table only supports a single writer");
        }
    }

    void finishBootstrap() {
        bootstrapping = false;
    }

    void processInput(InternalRow value) throws Exception {
        if (bootstrapping) {
            throw new IllegalStateException("Paimon global index bootstrap is not finished");
        }

        BinaryRow partition = extractor.partition(value);
        BinaryRow key = extractor.trimmedPrimaryKey(value);
        int partitionId = partitionMapping.index(partition);
        PositiveIntInt partitionBucket = keyIndex.get(key);
        if (partitionBucket == null) {
            processNewRecord(partition, partitionId, key, value);
            return;
        }

        int previousPartitionId = partitionBucket.i1();
        int previousBucket = partitionBucket.i2();
        if (previousPartitionId == partitionId) {
            collect(value, previousBucket);
            return;
        }

        BinaryRow previousPartition = partitionMapping.get(previousPartitionId);
        if (existingProcessor.processExists(value, previousPartition, previousBucket)) {
            processNewRecord(partition, partitionId, key, value);
        }
    }

    private void processNewRecord(
            BinaryRow partition, int partitionId, BinaryRow key, InternalRow value) {
        int bucket =
                bucketAssigner.assignBucket(
                        partition, this::isAssignedBucket, targetBucketRowNumber);
        keyIndex.put(key.copy(), new PositiveIntInt(partitionId, bucket));
        collect(value, bucket);
    }

    private boolean isAssignedBucket(int bucket) {
        return Math.abs(bucket % numAssigners) == assignId;
    }

    private void collect(InternalRow value, int bucket) {
        collector.accept(value, bucket);
    }

    @Override
    public void close() {
        keyIndex.clear();
        collector = null;
        extractor = null;
        bootstrapExtractor = null;
        partitionMapping = null;
        bucketAssigner = null;
        existingProcessor = null;
    }
}
