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

package org.apache.doris.connector.paimon;

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.ExpireSnapshots;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SimpleFileReader;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Minimal offline {@link Table} double for unit tests. Only the metadata read calls that
 * {@link PaimonConnectorMetadata} actually exercises — {@link #rowType()},
 * {@link #partitionKeys()}, {@link #primaryKeys()}, {@link #options()} — return controlled
 * values; every other method throws {@link UnsupportedOperationException}.
 *
 * <p>Throwing on the rest is deliberate: it documents that the metadata read path must touch
 * nothing else, and a future change that starts depending on (say) {@code newReadBuilder()} in
 * the read-only metadata path would blow up loudly in the test instead of silently passing.
 *
 * <p>P5-T08 promoted {@link #options()} out of the throwing set: the partition-listing path
 * reads the {@code partition.legacy-name} option, so {@code options()} now returns a
 * configurable map (default empty, settable via {@link #setOptions(Map)}). Every other method
 * keeps the fail-loud contract.
 */
final class FakePaimonTable implements Table {

    private final String name;
    private final RowType rowType;
    private final List<String> partitionKeys;
    private final List<String> primaryKeys;
    private Map<String, String> options = Collections.emptyMap();

    /**
     * The dynamic options passed to the most recent {@link #copy(Map)} call, or {@code null} if
     * {@code copy} was never invoked. Lets the scan tests assert the snapshot pin was applied via
     * {@code Table.copy(scanOptions)} rather than scanning the un-pinned table.
     */
    Map<String, String> lastCopyOptions;
    /** The table returned by {@link #copy(Map)}; defaults to {@code this} when unset. */
    Table copyResult;
    /** The FileIO returned by {@link #fileIO()}; {@code null} (the legacy throw) unless set. */
    FileIO fileIO;

    FakePaimonTable(String name, RowType rowType,
            List<String> partitionKeys, List<String> primaryKeys) {
        this.name = name;
        this.rowType = rowType;
        this.partitionKeys = partitionKeys;
        this.primaryKeys = primaryKeys;
    }

    /** Configures the value returned by {@link #options()}. */
    void setOptions(Map<String, String> options) {
        this.options = options;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public List<String> partitionKeys() {
        return partitionKeys;
    }

    @Override
    public List<String> primaryKeys() {
        return primaryKeys;
    }

    // ---- everything below is outside the metadata read path: fail loud if ever called ----

    @Override
    public Map<String, String> options() {
        return options;
    }

    @Override
    public Optional<String> comment() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Statistics> statistics() {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileIO fileIO() {
        // Settable so FIX-REST-VENDED tests can inject a non-REST FileIO double (the positive
        // RESTTokenFileIO path needs a live REST stack, covered by the fe-core bridge test + E2E).
        return fileIO;
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        // Records the scan-pin options the scan path layers on via Table.copy(scanOptions). Returns
        // a configurable result table (defaults to this) so the test can prove the COPIED table —
        // not the un-pinned original — is what gets planned/serialized.
        this.lastCopyOptions = dynamicOptions;
        return copyResult != null ? copyResult : this;
    }

    @Override
    public Optional<Snapshot> latestSnapshot() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SimpleFileReader<ManifestFileMeta> manifestListReader() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SimpleFileReader<ManifestEntry> manifestFileReader() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SimpleFileReader<IndexManifestEntry> indexManifestFileReader() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollbackTo(long snapshotId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTag(String tagName, long fromSnapshotId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTag(String tagName, long fromSnapshotId, Duration timeRetained) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTag(String tagName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTag(String tagName, Duration timeRetained) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTag(String tagName, String targetTagName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replaceTag(String tagName, Long fromSnapshotId, Duration timeRetained) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteTag(String tagName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollbackTo(String tagName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createBranch(String branchName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createBranch(String branchName, String tagName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteBranch(String branchName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void fastForward(String branchName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExpireSnapshots newExpireSnapshots() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExpireSnapshots newExpireChangelog() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ReadBuilder newReadBuilder() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BatchWriteBuilder newBatchWriteBuilder() {
        throw new UnsupportedOperationException();
    }

    @Override
    public StreamWriteBuilder newStreamWriteBuilder() {
        throw new UnsupportedOperationException();
    }
}
