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

package org.apache.doris.datasource.hive;

import org.apache.doris.backup.Status;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.hive.AcidInfo.DeleteDeltaInfo;
import org.apache.doris.datasource.hive.HiveMetaStoreCache.FileCacheValue;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.FileSystem;
import org.apache.doris.fs.remote.RemoteFile;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
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
import java.util.stream.Collectors;

public class AcidUtil {
    private static final Logger LOG = LogManager.getLogger(AcidUtil.class);

    public static final String VALID_TXNS_KEY = "hive.txn.valid.txns";
    public static final String VALID_WRITEIDS_KEY = "hive.txn.valid.writeids";

    private static final String HIVE_TRANSACTIONAL_ORC_BUCKET_PREFIX = "bucket_";
    private static final String DELTA_SIDE_FILE_SUFFIX = "_flush_length";

    // An `_orc_acid_version` file is written to each base/delta/delete_delta dir written by a full acid write
    // or compaction.  This is the primary mechanism for versioning acid data.
    // Each individual ORC file written stores the current version as a user property in ORC footer. All data files
    // produced by Acid write should have this (starting with Hive 3.0), including those written by compactor.This is
    // more for sanity checking in case someone moved the files around or something like that.
    // In hive, methods for getting/reading the version from files were moved to test which is the only place they are
    // used (after HIVE-23506), in order to keep devs out of temptation, since they access the FileSystem which
    // is expensive.
    // After `HIVE-23825: Create a flag to turn off _orc_acid_version file creation`, introduce variables to
    // control whether to generate `_orc_acid_version` file. So don't need check this file exist.
    private static final String HIVE_ORC_ACID_VERSION_FILE = "_orc_acid_version";

    @Getter
    @ToString
    private static class ParsedBase {
        private final long writeId;
        private final long visibilityId;

        public ParsedBase(long writeId, long visibilityId) {
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

    @Getter
    @ToString
    @EqualsAndHashCode
    private static class ParsedDelta implements Comparable<ParsedDelta> {
        private final long min;
        private final long max;
        private final String path;
        private final int statementId;
        private final boolean deleteDelta;
        private final long visibilityId;

        public ParsedDelta(long min, long max, @NonNull String path, int statementId,
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
        Status status = fileSystem.exists(fileLocation);
        if (status != Status.OK) {
            return false;
        }
        //In order to save the cost of reading the file content, we only check whether the file exists.
        // File Contents: {"thisFileVersion":"0","dataFormat":"compacted"}
        //
        // Map<String, String> metadata;
        // try (var in = read(fileLocation)) {
        //     metadata = new ObjectMapper().readValue(in, new TypeReference<>() {});
        // }
        // catch (IOException e) {
        //     throw new IOException(String.format("Failed to read %s: %s", fileLocation, e.getMessage()), e);
        // }
        //
        // String version = metadata.get("thisFileVersion");
        // if (!"0".equals(version)) {
        //     throw new IOException("Unexpected ACID metadata version: " + version);
        // }
        //
        // String format = metadata.get("dataFormat");
        // if (!"compacted".equals(format)) {
        //     throw new IOException("Unexpected value for ACID dataFormat: " + format);
        // }
        return true;
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

        // String fileName = fileName.substring(name.lastIndexOf('/') + 1);
        // checkArgument(fileName.startsWith(deltaPrefix), "File does not start with '%s': %s", deltaPrefix, path);

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

    public interface FileFilter {
        public boolean accept(String fileName);
    }

    public static final class  FullAcidFileFilter implements FileFilter {
        @Override
        public boolean accept(String fileName) {
            return fileName.startsWith(HIVE_TRANSACTIONAL_ORC_BUCKET_PREFIX)
                    && !fileName.endsWith(DELTA_SIDE_FILE_SUFFIX);
        }
    }

    public static final class InsertOnlyFileFilter implements FileFilter {
        @Override
        public boolean accept(String fileName) {
            return true;
        }
    }

    //Since the hive3 library cannot read the hive4 transaction table normally, and there are many problems
    // when using the Hive 4 library directly, this method is implemented.
    //Ref: hive/ql/src/java/org/apache/hadoop/hive/ql/io/AcidUtils.java#getAcidState
    public static FileCacheValue getAcidState(FileSystem fileSystem, HivePartition partition,
            Map<String, String> txnValidIds, Map<StorageProperties.Type, StorageProperties> storagePropertiesMap,
                                              boolean isFullAcid) throws Exception {

        // Ref: https://issues.apache.org/jira/browse/HIVE-18192
        // Readers should use the combination of ValidTxnList and ValidWriteIdList(Table) for snapshot isolation.
        // ValidReadTxnList implements ValidTxnList
        // ValidReaderWriteIdList implements ValidWriteIdList
        ValidTxnList validTxnList = null;
        if (txnValidIds.containsKey(VALID_TXNS_KEY)) {
            validTxnList = new ValidReadTxnList();
            validTxnList.readFromString(
                    txnValidIds.get(VALID_TXNS_KEY)
            );
        } else {
            throw new RuntimeException("Miss ValidTxnList");
        }

        ValidWriteIdList validWriteIdList = null;
        if (txnValidIds.containsKey(VALID_WRITEIDS_KEY)) {
            validWriteIdList = new ValidReaderWriteIdList();
            validWriteIdList.readFromString(
                    txnValidIds.get(VALID_WRITEIDS_KEY)
            );
        } else {
            throw new RuntimeException("Miss ValidWriteIdList");
        }

        String partitionPath = partition.getPath();
        //hdfs://xxxxx/user/hive/warehouse/username/data_id=200103

        List<RemoteFile> lsPartitionPath = new ArrayList<>();
        Status status = fileSystem.globList(partitionPath + "/*", lsPartitionPath);
        // List all files and folders, without recursion.
        // FileStatus[] lsPartitionPath = null;
        // fileSystem.listFileStatuses(partitionPath,lsPartitionPath);
        if (status != Status.OK) {
            throw new IOException(status.toString());
        }

        String oldestBase = null;
        long oldestBaseWriteId = Long.MAX_VALUE;
        String bestBasePath = null;
        long bestBaseWriteId = 0;
        boolean haveOriginalFiles = false;
        List<ParsedDelta> workingDeltas = new ArrayList<>();

        for (RemoteFile remotePath : lsPartitionPath) {
            if (remotePath.isDirectory()) {
                String dirName = remotePath.getName(); //dirName: base_xxx,delta_xxx,...
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
                            && isValidBase(fileSystem, dirPath, base, validWriteIdList)) {
                        //IOW will generator a base_N/ directory: https://issues.apache.org/jira/browse/HIVE-14988
                        //So maybe need consider: https://issues.apache.org/jira/browse/HIVE-25777

                        bestBasePath =  dirPath;
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

        FileCacheValue fileCacheValue = new FileCacheValue();
        List<DeleteDeltaInfo> deleteDeltas = new ArrayList<>();

        FileFilter fileFilter = isFullAcid ? new FullAcidFileFilter() : new InsertOnlyFileFilter();

        // delta directories
        for (ParsedDelta delta : deltas) {
            String location = delta.getPath();

            List<RemoteFile> remoteFiles = new ArrayList<>();
            status = fileSystem.listFiles(location, false, remoteFiles);
            if (status.ok()) {
                if (delta.isDeleteDelta()) {
                    List<String> deleteDeltaFileNames = remoteFiles.stream()
                            .map(RemoteFile::getName).filter(fileFilter::accept)
                            .collect(Collectors.toList());
                    deleteDeltas.add(new DeleteDeltaInfo(location, deleteDeltaFileNames));
                    continue;
                }
                remoteFiles.stream().filter(f -> fileFilter.accept(f.getName())).forEach(file -> {
                    LocationPath path = LocationPath.of(file.getPath().toString(), storagePropertiesMap);
                    fileCacheValue.addFile(file, path);
                });
            } else {
                throw new RuntimeException(status.getErrMsg());
            }
        }

        // base
        if (bestBasePath != null) {
            List<RemoteFile> remoteFiles = new ArrayList<>();
            status = fileSystem.listFiles(bestBasePath, false, remoteFiles);
            if (status.ok()) {
                remoteFiles.stream().filter(f -> fileFilter.accept(f.getName()))
                        .forEach(file -> {
                            LocationPath path = LocationPath.of(file.getPath().toString(), storagePropertiesMap);
                            fileCacheValue.addFile(file, path);
                        });
            } else {
                throw new RuntimeException(status.getErrMsg());
            }
        }

        if (isFullAcid) {
            fileCacheValue.setAcidInfo(new AcidInfo(partition.getPath(), deleteDeltas));
        } else if (!deleteDeltas.isEmpty()) {
            throw new RuntimeException("No Hive Full Acid Table have delete_delta_* Dir.");
        }
        return fileCacheValue;
    }
}
