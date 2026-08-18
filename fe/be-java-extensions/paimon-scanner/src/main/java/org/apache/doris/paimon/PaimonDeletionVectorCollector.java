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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Accumulates deleted row positions for an APPEND-ONLY paimon table and turns them into
 * deletion-vector index files at commit time.
 *
 * <p>An append-only table has no key to cancel a row against, so a delete is recorded as a POSITION:
 * which data file the row lives in, and its ordinal within that file. Those positions become a
 * {@link DeletionVector} per data file, which readers apply to skip the deleted rows.
 *
 * <p>Positions are grouped by {@code (partition, bucket)} because that is the granularity of both a
 * paimon {@link CommitMessage} and a deletion-vector index file. One
 * {@link BaseAppendDeleteFileMaintainer} is built per group at commit time.
 *
 * <p><b>Why the maintainer rather than a raw index-file writer:</b> the maintainer reads the deletion
 * vectors already present in the snapshot and MERGES the new positions into them. Writing index files
 * directly would silently discard earlier deletes, so the second {@code DELETE} against a table would
 * resurrect the rows removed by the first.
 */
public class PaimonDeletionVectorCollector {

    /** Deleted positions per data file, grouped by the (partition, bucket) that owns the file. */
    private final Map<Pair<BinaryRow, Integer>, Map<String, List<Long>>> pending = new LinkedHashMap<>();

    private final FileStoreTable table;

    public PaimonDeletionVectorCollector(FileStoreTable table) {
        this.table = table;
    }

    /**
     * Records one deleted row.
     *
     * @param partition    the row's partition (an empty {@link BinaryRow} for an unpartitioned table)
     * @param bucket       the row's bucket
     * @param dataFileName the data file's NAME (not its full path — that is what a deletion vector keys on)
     * @param rowPosition  the row's zero-based ordinal within that data file
     */
    public void add(BinaryRow partition, int bucket, String dataFileName, long rowPosition) {
        pending.computeIfAbsent(Pair.of(partition, bucket), k -> new LinkedHashMap<>())
                .computeIfAbsent(dataFileName, k -> new ArrayList<>())
                .add(rowPosition);
    }

    /** Whether any deletion was recorded (an empty collector must not produce a commit message). */
    public boolean isEmpty() {
        return pending.isEmpty();
    }

    /**
     * Builds the deletion-vector index files and wraps them into commit messages — one per
     * {@code (partition, bucket)} group.
     *
     * <p>The maintainer returns both ADD and DELETE index entries: rewriting a bucket's deletion vector
     * replaces its previous index file, so the old one must be retired in the SAME commit or it would be
     * applied on top of the new one.
     */
    public List<CommitMessage> persist() throws Exception {
        if (pending.isEmpty()) {
            return Collections.emptyList();
        }
        IndexFileHandler indexFileHandler = table.store().newIndexFileHandler();
        // Read existing vectors as of the latest snapshot so this delete is additive. A null snapshot
        // (never-committed table) is valid and means "no existing vectors".
        org.apache.paimon.Snapshot snapshot = table.snapshotManager().latestSnapshot();
        boolean unaware = table.bucketMode() == BucketMode.BUCKET_UNAWARE;

        List<CommitMessage> messages = new ArrayList<>(pending.size());
        for (Map.Entry<Pair<BinaryRow, Integer>, Map<String, List<Long>>> group : pending.entrySet()) {
            BinaryRow partition = group.getKey().getLeft();
            int bucket = group.getKey().getRight();

            BaseAppendDeleteFileMaintainer maintainer = unaware
                    ? BaseAppendDeleteFileMaintainer.forUnawareAppend(indexFileHandler, snapshot, partition)
                    : BaseAppendDeleteFileMaintainer.forBucketedAppend(
                            indexFileHandler, snapshot, partition, bucket);

            for (Map.Entry<String, List<Long>> perFile : group.getValue().entrySet()) {
                DeletionVector vector = new BitmapDeletionVector();
                for (long position : perFile.getValue()) {
                    vector.delete(position);
                }
                maintainer.notifyNewDeletionVector(perFile.getKey(), vector);
            }

            List<IndexFileMeta> added = new ArrayList<>();
            List<IndexFileMeta> deleted = new ArrayList<>();
            for (IndexManifestEntry entry : maintainer.persist()) {
                (entry.kind() == FileKind.ADD ? added : deleted).add(entry.indexFile());
            }
            if (added.isEmpty() && deleted.isEmpty()) {
                continue;
            }
            messages.add(new CommitMessageImpl(
                    maintainer.getPartition(),
                    maintainer.getBucket(),
                    null,
                    // A deletion carries no data files — only index files change.
                    new DataIncrement(Collections.emptyList(), Collections.emptyList(),
                            Collections.emptyList(), added, deleted),
                    CompactIncrement.emptyIncrement()));
        }
        pending.clear();
        return messages;
    }
}
