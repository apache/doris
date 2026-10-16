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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.hms.HmsAcidConstants;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.Location;

import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList.RangeResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Pure directory-name ACID state resolution for transactional Hive tables.
 *
 * <p>Plugin-side port of fe-core {@code AcidUtil.getAcidState}. It resolves, for one partition
 * directory, the "best" base plus the working set of delta / delete-delta directories under a snapshot
 * ({@code ValidTxnList} + {@code ValidWriteIdList}) and returns the surviving <b>data</b> files (base +
 * non-delete deltas) plus the delete-delta descriptors. The BE later applies row deletes from the
 * delete deltas.</p>
 *
 * <p>The fe-core version drags in {@code FileCacheValue}/{@code LocationPath}/{@code AcidInfo}/
 * {@code HivePartition}; those all drop out at the plugin boundary because the plugin lists files with the
 * engine-injected Doris {@link FileSystem} and emits {@link HiveScanRange} directly. Only the pure name-parsing
 * plus the {@code hive-common} {@code Valid*} algorithm ports.</p>
 *
 * <p>Ref: hive/ql/src/java/org/apache/hadoop/hive/ql/io/AcidUtils.java#getAcidState (the fe-core copy
 * exists because hive3 cannot read hive4 transaction tables and using hive4 directly is problematic).</p>
 */
public final class HiveAcidUtil {
    private static final Logger LOG = LogManager.getLogger(HiveAcidUtil.class);

    private static final String HIVE_TRANSACTIONAL_ORC_BUCKET_PREFIX = "bucket_";
    private static final String DELTA_SIDE_FILE_SUFFIX = "_flush_length";

    private HiveAcidUtil() {
    }

    /** Resolved ACID state for one partition: surviving data files + delete-delta descriptors. */
    public static final class AcidState {
        private final List<FileEntry> dataFiles;
        private final List<DeleteDelta> deleteDeltas;

        AcidState(List<FileEntry> dataFiles, List<DeleteDelta> deleteDeltas) {
            this.dataFiles = dataFiles;
            this.deleteDeltas = deleteDeltas;
        }

        /** Base + non-delete delta bucket files that survive the snapshot; each becomes a scan split. */
        public List<FileEntry> getDataFiles() {
            return dataFiles;
        }

        /** Delete-delta directories (with the bucket file names inside) the BE must subtract. */
        public List<DeleteDelta> getDeleteDeltas() {
            return deleteDeltas;
        }
    }

    /** A single delete-delta directory plus the (filtered) delete file names inside it. */
    public static final class DeleteDelta {
        private final String directoryLocation;
        private final List<String> fileNames;

        DeleteDelta(String directoryLocation, List<String> fileNames) {
            this.directoryLocation = directoryLocation;
            this.fileNames = fileNames;
        }

        public String getDirectoryLocation() {
            return directoryLocation;
        }

        public List<String> getFileNames() {
            return fileNames;
        }
    }

    private static final class ParsedBase {
        private final long writeId;
        private final long visibilityId;

        ParsedBase(long writeId, long visibilityId) {
            this.writeId = writeId;
            this.visibilityId = visibilityId;
        }
    }

    private static ParsedBase parseBase(String name) {
        //format1 : base_writeId
        //format2 : base_writeId_visibilityId  detail: https://issues.apache.org/jira/browse/HIVE-20823
        name = name.substring("base_".length());
        int index = name.indexOf("_v");
        if (index == -1) {
            return new ParsedBase(Long.parseLong(name), 0);
        }
        return new ParsedBase(
                Long.parseLong(name.substring(0, index)),
                Long.parseLong(name.substring(index + 2)));
    }

    private static final class ParsedDelta implements Comparable<ParsedDelta> {
        private final long min;
        private final long max;
        private final String path;
        private final int statementId;
        private final boolean deleteDelta;
        private final long visibilityId;

        ParsedDelta(long min, long max, String path, int statementId,
                boolean deleteDelta, long visibilityId) {
            this.min = min;
            this.max = max;
            this.path = path;
            this.statementId = statementId;
            this.deleteDelta = deleteDelta;
            this.visibilityId = visibilityId;
        }

        /*
         * Smaller minWID orders first;
         * If minWID is the same, larger maxWID orders first;
         * Otherwise, sort by stmtID; files w/o stmtID orders first.
         *
         * Compactions (Major/Minor) merge deltas/bases but delete of old files
         * happens in a different process; thus it's possible to have bases/deltas with
         * overlapping writeId boundaries.  The sort order helps figure out the "best" set of files
         * to use to get data.
         * This sorts "wider" delta before "narrower" i.e. delta_5_20 sorts before delta_5_10 (and delta_11_20)
         */
        @Override
        public int compareTo(ParsedDelta other) {
            return min != other.min ? Long.compare(min, other.min) :
                    other.max != max ? Long.compare(other.max, max) :
                            statementId != other.statementId
                                    ? Integer.compare(statementId, other.statementId) :
                                    path.compareTo(other.path);
        }
    }

    private static boolean isValidMetaDataFile(FileSystem fileSystem, String baseDir)
            throws IOException {
        String fileLocation = baseDir + "_metadata_acid";
        try {
            return fileSystem.exists(Location.of(fileLocation));
        } catch (IOException e) {
            return false;
        }
    }

    private static boolean isValidBase(FileSystem fileSystem, String baseDir,
            ParsedBase base, ValidWriteIdList writeIdList) throws IOException {
        if (base.writeId == Long.MIN_VALUE) {
            //Ref: https://issues.apache.org/jira/browse/HIVE-13369
            //such base is created by 1st compaction in case of non-acid to acid table conversion.(you
            //will get dir: `base_-9223372036854775808`)
            //By definition there are no open txns with id < 1.
            //After this: https://issues.apache.org/jira/browse/HIVE-18192, txns(global transaction ID) => writeId.
            return true;
        }

        // hive 4 : just check "_v" suffix, before hive 4 : check `_metadata_acid` file in baseDir.
        if ((base.visibilityId > 0) || isValidMetaDataFile(fileSystem, baseDir)) {
            return writeIdList.isValidBase(base.writeId);
        }

        // if here, it's a result of IOW
        return writeIdList.isWriteIdValid(base.writeId);
    }

    private static ParsedDelta parseDelta(String fileName, String deltaPrefix, String path) {
        // format1: delta_min_max_statementId_visibilityId, delete_delta_min_max_statementId_visibilityId
        //     _visibilityId maybe not exists.
        //     detail: https://issues.apache.org/jira/browse/HIVE-20823
        // format2: delta_min_max_visibilityId, delete_delta_min_visibilityId
        //     when minor compaction runs, we collapse per statement delta files inside a single
        //     transaction so we no longer need a statementId in the file name

        long visibilityId = 0;
        int visibilityIdx = fileName.indexOf("_v");
        if (visibilityIdx != -1) {
            visibilityId = Long.parseLong(fileName.substring(visibilityIdx + 2));
            fileName = fileName.substring(0, visibilityIdx);
        }

        boolean deleteDelta = deltaPrefix.equals("delete_delta_");

        String rest = fileName.substring(deltaPrefix.length());
        int split = rest.indexOf('_');
        int split2 = rest.indexOf('_', split + 1);
        long min = Long.parseLong(rest.substring(0, split));

        if (split2 == -1) {
            long max = Long.parseLong(rest.substring(split + 1));
            return new ParsedDelta(min, max, path, -1, deleteDelta, visibilityId);
        }

        long max = Long.parseLong(rest.substring(split + 1, split2));
        int statementId = Integer.parseInt(rest.substring(split2 + 1));
        return new ParsedDelta(min, max, path, statementId, deleteDelta, visibilityId);
    }

    private interface FileFilter {
        boolean accept(String fileName);
    }

    private static final class FullAcidFileFilter implements FileFilter {
        @Override
        public boolean accept(String fileName) {
            return fileName.startsWith(HIVE_TRANSACTIONAL_ORC_BUCKET_PREFIX)
                    && !fileName.endsWith(DELTA_SIDE_FILE_SUFFIX);
        }
    }

    private static final class InsertOnlyFileFilter implements FileFilter {
        @Override
        public boolean accept(String fileName) {
            return true;
        }
    }

    /**
     * Lists the immediate children (files <b>and</b> directories) of {@code dir}, non-recursively, via the
     * engine-injected Doris {@link FileSystem#list} — the literal listing that mirrors the old
     * {@code FileSystem.listStatus}.
     *
     * <p><b>Literal, not glob:</b> uses {@code fs.list(loc)}, never {@code fs.listFiles(loc)}. The per-scheme
     * filesystems ({@code DFSFileSystem}, {@code S3CompatibleFileSystem}) override {@code listFiles} with a
     * glob-aware branch that would treat a location containing {@code [}/{@code *}/{@code ?} as a pattern; a hive
     * partition/delta location can legitimately contain those characters, and the old {@code listStatus} never
     * glob-expanded. Mirrors {@code HiveFileListingCache.listFromFileSystem}.</p>
     */
    private static List<FileEntry> listEntries(FileSystem fs, String dir) throws IOException {
        List<FileEntry> entries = new ArrayList<>();
        try (FileIterator it = fs.list(Location.of(dir))) {
            while (it.hasNext()) {
                entries.add(it.next());
            }
        }
        return entries;
    }

    /**
     * Lists the immediate <b>file</b> children of {@code dir} (directories excluded).
     *
     * <p>Mirrors fe-core {@code globList(fs, dir, false)} on a bare directory path: with no wildcard the
     * glob has a {@code null} pattern, so it returns files only and skips any nested directory. The literal
     * {@link #listEntries} returns both, so the directory entries are filtered out here.</p>
     */
    private static List<FileEntry> listFiles(FileSystem fs, String dir) throws IOException {
        List<FileEntry> files = new ArrayList<>();
        for (FileEntry entry : listEntries(fs, dir)) {
            if (!entry.isDirectory()) {
                files.add(entry);
            }
        }
        return files;
    }

    /**
     * Resolves the ACID state of one partition directory under the given snapshot.
     *
     * @param fs            engine-injected Doris file system for the partition location
     * @param partitionPath the partition directory (e.g. {@code hdfs://.../data_id=200103})
     * @param txnValidIds   the two serialized {@code Valid*} lists keyed by {@link HmsAcidConstants}
     * @param isFullAcid    full-ACID (bucket_ filter + delete deltas) vs insert-only (accept-all)
     * @return surviving data files + delete-delta descriptors for the partition
     */
    public static AcidState getAcidState(FileSystem fs, String partitionPath,
            Map<String, String> txnValidIds, boolean isFullAcid) throws IOException {
        // Ref: https://issues.apache.org/jira/browse/HIVE-18192
        // Readers should use the combination of ValidTxnList and ValidWriteIdList(Table) for snapshot isolation.
        // ValidReadTxnList implements ValidTxnList
        // ValidReaderWriteIdList implements ValidWriteIdList
        ValidTxnList validTxnList;
        if (txnValidIds.containsKey(HmsAcidConstants.VALID_TXNS_KEY)) {
            validTxnList = new ValidReadTxnList();
            validTxnList.readFromString(txnValidIds.get(HmsAcidConstants.VALID_TXNS_KEY));
        } else {
            throw new RuntimeException("Miss ValidTxnList");
        }

        ValidWriteIdList validWriteIdList;
        if (txnValidIds.containsKey(HmsAcidConstants.VALID_WRITEIDS_KEY)) {
            validWriteIdList = new ValidReaderWriteIdList();
            validWriteIdList.readFromString(txnValidIds.get(HmsAcidConstants.VALID_WRITEIDS_KEY));
        } else {
            throw new RuntimeException("Miss ValidWriteIdList");
        }

        //hdfs://xxxxx/user/hive/warehouse/username/data_id=200103
        // List all files and folders, without recursion.
        List<FileEntry> partitionEntries = listEntries(fs, partitionPath);

        String oldestBase = null;
        long oldestBaseWriteId = Long.MAX_VALUE;
        String bestBasePath = null;
        long bestBaseWriteId = 0;
        boolean haveOriginalFiles = false;
        List<ParsedDelta> workingDeltas = new ArrayList<>();

        for (FileEntry entry : partitionEntries) {
            if (entry.isDirectory()) {
                String dirName = entry.name(); //dirName: base_xxx,delta_xxx,...
                String dirPath = partitionPath + "/" + dirName;

                if (dirName.startsWith("base_")) {
                    ParsedBase base = parseBase(dirName);
                    if (!validTxnList.isTxnValid(base.visibilityId)) {
                        //checks visibilityTxnId to see if it is committed in current snapshot.
                        continue;
                    }

                    long writeId = base.writeId;
                    if (oldestBaseWriteId > writeId) {
                        oldestBase = dirPath;
                        oldestBaseWriteId = writeId;
                    }

                    if (((bestBasePath == null) || (bestBaseWriteId < writeId))
                            && isValidBase(fs, dirPath, base, validWriteIdList)) {
                        //IOW will generator a base_N/ directory: https://issues.apache.org/jira/browse/HIVE-14988
                        //So maybe need consider: https://issues.apache.org/jira/browse/HIVE-25777

                        bestBasePath = dirPath;
                        bestBaseWriteId = writeId;
                    }
                } else if (dirName.startsWith("delta_") || dirName.startsWith("delete_delta_")) {
                    String deltaPrefix = dirName.startsWith("delta_") ? "delta_" : "delete_delta_";
                    ParsedDelta delta = parseDelta(dirName, deltaPrefix, dirPath);

                    if (!validTxnList.isTxnValid(delta.visibilityId)) {
                        continue;
                    }

                    // No need check (validWriteIdList.isWriteIdRangeAborted(min,max) != RangeResponse.ALL)
                    // It is a subset of (validWriteIdList.isWriteIdRangeValid(min, max) != RangeResponse.NONE)
                    if (validWriteIdList.isWriteIdRangeValid(delta.min, delta.max) != RangeResponse.NONE) {
                        workingDeltas.add(delta);
                    }
                } else {
                    //Sometimes hive will generate temporary directories(`.hive-staging_hive_xxx` ),
                    // which do not need to be read.
                    LOG.warn("Read Hive Acid Table ignore the contents of this folder:" + dirName);
                }
            } else {
                haveOriginalFiles = true;
            }
        }

        if (bestBasePath == null && haveOriginalFiles) {
            // ALTER TABLE nonAcidTbl SET TBLPROPERTIES ('transactional'='true');
            throw new UnsupportedOperationException("For no acid table convert to acid, please COMPACT 'major'.");
        }

        if ((oldestBase != null) && (bestBasePath == null)) {
            /*
             * If here, it means there was a base_x (> 1 perhaps) but none were suitable for given
             * {@link writeIdList}.  Note that 'original' files are logically a base_Long.MIN_VALUE and thus
             * cannot have any data for an open txn.  We could check {@link deltas} has files to cover
             * [1,n] w/o gaps but this would almost never happen...
             *
             * We only throw for base_x produced by Compactor since that base erases all history and
             * cannot be used for a client that has a snapshot in which something inside this base is
             * open.  (Nor can we ignore this base of course)  But base_x which is a result of IOW,
             * contains all history so we treat it just like delta wrt visibility.  Imagine, IOW which
             * aborts. It creates a base_x, which can and should just be ignored.*/
            long[] exceptions = validWriteIdList.getInvalidWriteIds();
            String minOpenWriteId = ((exceptions != null)
                    && (exceptions.length > 0)) ? String.valueOf(exceptions[0]) : "x";
            throw new IOException(
                    String.format("Not enough history available for ({},{}). Oldest available base: {}",
                            validWriteIdList.getHighWatermark(), minOpenWriteId, oldestBase));
        }

        workingDeltas.sort(null);

        List<ParsedDelta> deltas = new ArrayList<>();
        long current = bestBaseWriteId;
        int lastStatementId = -1;
        ParsedDelta prev = null;
        // find need read delta/delete_delta file.
        for (ParsedDelta next : workingDeltas) {
            if (next.max > current) {
                if (validWriteIdList.isWriteIdRangeValid(current + 1, next.max) != RangeResponse.NONE) {
                    deltas.add(next);
                    current = next.max;
                    lastStatementId = next.statementId;
                    prev = next;
                }
            } else if ((next.max == current) && (lastStatementId >= 0)) {
                //make sure to get all deltas within a single transaction;  multi-statement txn
                //generate multiple delta files with the same txnId range
                //of course, if maxWriteId has already been minor compacted,
                // all per statement deltas are obsolete

                deltas.add(next);
                prev = next;
            } else if ((prev != null)
                    && (next.max == prev.max)
                    && (next.min == prev.min)
                    && (next.statementId == prev.statementId)) {
                // The 'next' parsedDelta may have everything equal to the 'prev' parsedDelta, except
                // the path. This may happen when we have split update and we have two types of delta
                // directories- 'delta_x_y' and 'delete_delta_x_y' for the SAME txn range.

                // Also note that any delete_deltas in between a given delta_x_y range would be made
                // obsolete. For example, a delta_30_50 would make delete_delta_40_40 obsolete.
                // This is valid because minor compaction always compacts the normal deltas and the delete
                // deltas for the same range. That is, if we had 3 directories, delta_30_30,
                // delete_delta_40_40 and delta_50_50, then running minor compaction would produce
                // delta_30_50 and delete_delta_30_50.
                deltas.add(next);
                prev = next;
            }
        }

        List<FileEntry> dataFiles = new ArrayList<>();
        List<DeleteDelta> deleteDeltas = new ArrayList<>();

        FileFilter fileFilter = isFullAcid ? new FullAcidFileFilter() : new InsertOnlyFileFilter();

        // delta directories
        for (ParsedDelta delta : deltas) {
            String location = delta.path;
            List<FileEntry> entries = listFiles(fs, location);
            if (delta.deleteDelta) {
                List<String> deleteDeltaFileNames = new ArrayList<>();
                for (FileEntry entry : entries) {
                    String name = entry.name();
                    if (fileFilter.accept(name)) {
                        deleteDeltaFileNames.add(name);
                    }
                }
                deleteDeltas.add(new DeleteDelta(location, deleteDeltaFileNames));
                continue;
            }
            for (FileEntry entry : entries) {
                if (fileFilter.accept(entry.name())) {
                    dataFiles.add(entry);
                }
            }
        }

        // base
        if (bestBasePath != null) {
            List<FileEntry> entries = listFiles(fs, bestBasePath);
            for (FileEntry entry : entries) {
                if (fileFilter.accept(entry.name())) {
                    dataFiles.add(entry);
                }
            }
        }

        if (!isFullAcid && !deleteDeltas.isEmpty()) {
            throw new RuntimeException("No Hive Full Acid Table have delete_delta_* Dir.");
        }
        return new AcidState(dataFiles, deleteDeltas);
    }
}
