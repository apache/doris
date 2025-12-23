/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.metrics.ScanMetricsUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.PartitionMap;
import org.apache.iceberg.util.PartitionSet;
import org.apache.iceberg.util.Tasks;

/**
 * An index of {@link DeleteFile delete files} by sequence number.
 *
 * <p>Use {@link #builderFor(FileIO, Iterable)} to construct an index, and {@link #forDataFile(long,
 * DataFile)} or {@link #forEntry(ManifestEntry)} to get the delete files to apply to a given data
 * file.
 * 
 * Copied from https://github.com/apache/iceberg/blob/apache-iceberg-1.9.1/core/src/main/java/org/apache/iceberg/DeleteFileIndex.java
 * Change DeleteFileIndex and some methods to public.
 */
public class DeleteFileIndex {
  private static final DeleteFile[] EMPTY_DELETES = new DeleteFile[0];

  private final EqualityDeletes globalDeletes;
  private final PartitionMap<EqualityDeletes> eqDeletesByPartition;
  private final PartitionMap<PositionDeletes> posDeletesByPartition;
  private final Map<String, PositionDeletes> posDeletesByPath;
  private final Map<String, DeleteFile> dvByPath;
  private final boolean hasEqDeletes;
  private final boolean hasPosDeletes;
  private final boolean isEmpty;

  private DeleteFileIndex(
      EqualityDeletes globalDeletes,
      PartitionMap<EqualityDeletes> eqDeletesByPartition,
      PartitionMap<PositionDeletes> posDeletesByPartition,
      Map<String, PositionDeletes> posDeletesByPath,
      Map<String, DeleteFile> dvByPath) {
    this.globalDeletes = globalDeletes;
    this.eqDeletesByPartition = eqDeletesByPartition;
    this.posDeletesByPartition = posDeletesByPartition;
    this.posDeletesByPath = posDeletesByPath;
    this.dvByPath = dvByPath;
    this.hasEqDeletes = globalDeletes != null || eqDeletesByPartition != null;
    this.hasPosDeletes =
        posDeletesByPartition != null || posDeletesByPath != null || dvByPath != null;
    this.isEmpty = !hasEqDeletes && !hasPosDeletes;
  }

  public boolean isEmpty() {
    return isEmpty;
  }

  public boolean hasEqualityDeletes() {
    return hasEqDeletes;
  }

  public boolean hasPositionDeletes() {
    return hasPosDeletes;
  }

  public Iterable<DeleteFile> referencedDeleteFiles() {
    Iterable<DeleteFile> deleteFiles = Collections.emptyList();

    if (globalDeletes != null) {
      deleteFiles = Iterables.concat(deleteFiles, globalDeletes.referencedDeleteFiles());
    }

    if (eqDeletesByPartition != null) {
      for (EqualityDeletes deletes : eqDeletesByPartition.values()) {
        deleteFiles = Iterables.concat(deleteFiles, deletes.referencedDeleteFiles());
      }
    }

    if (posDeletesByPartition != null) {
      for (PositionDeletes deletes : posDeletesByPartition.values()) {
        deleteFiles = Iterables.concat(deleteFiles, deletes.referencedDeleteFiles());
      }
    }

    if (posDeletesByPath != null) {
      for (PositionDeletes deletes : posDeletesByPath.values()) {
        deleteFiles = Iterables.concat(deleteFiles, deletes.referencedDeleteFiles());
      }
    }

    if (dvByPath != null) {
      deleteFiles = Iterables.concat(deleteFiles, dvByPath.values());
    }

    return deleteFiles;
  }

  DeleteFile[] forEntry(ManifestEntry<DataFile> entry) {
    return forDataFile(entry.dataSequenceNumber(), entry.file());
  }

  public DeleteFile[] forDataFile(DataFile file) {
    return forDataFile(file.dataSequenceNumber(), file);
  }

  public DeleteFile[] forDataFile(long sequenceNumber, DataFile file) {
    if (isEmpty) {
      return EMPTY_DELETES;
    }

    DeleteFile[] global = findGlobalDeletes(sequenceNumber, file);
    DeleteFile[] eqPartition = findEqPartitionDeletes(sequenceNumber, file);
    DeleteFile dv = findDV(sequenceNumber, file);
    if (dv != null && global == null && eqPartition == null) {
      return new DeleteFile[] {dv};
    } else if (dv != null) {
      return concat(global, eqPartition, new DeleteFile[] {dv});
    } else {
      DeleteFile[] posPartition = findPosPartitionDeletes(sequenceNumber, file);
      DeleteFile[] posPath = findPathDeletes(sequenceNumber, file);
      return concat(global, eqPartition, posPartition, posPath);
    }
  }

  private DeleteFile[] findGlobalDeletes(long seq, DataFile dataFile) {
    return globalDeletes == null ? EMPTY_DELETES : globalDeletes.filter(seq, dataFile);
  }

  private DeleteFile[] findPosPartitionDeletes(long seq, DataFile dataFile) {
    if (posDeletesByPartition == null) {
      return EMPTY_DELETES;
    }

    PositionDeletes deletes = posDeletesByPartition.get(dataFile.specId(), dataFile.partition());
    return deletes == null ? EMPTY_DELETES : deletes.filter(seq);
  }

  private DeleteFile[] findEqPartitionDeletes(long seq, DataFile dataFile) {
    if (eqDeletesByPartition == null) {
      return EMPTY_DELETES;
    }

    EqualityDeletes deletes = eqDeletesByPartition.get(dataFile.specId(), dataFile.partition());
    return deletes == null ? EMPTY_DELETES : deletes.filter(seq, dataFile);
  }

  @SuppressWarnings("CollectionUndefinedEquality")
  private DeleteFile[] findPathDeletes(long seq, DataFile dataFile) {
    if (posDeletesByPath == null) {
      return EMPTY_DELETES;
    }

    PositionDeletes deletes = posDeletesByPath.get(dataFile.location());
    return deletes == null ? EMPTY_DELETES : deletes.filter(seq);
  }

  private DeleteFile findDV(long seq, DataFile dataFile) {
    if (dvByPath == null) {
      return null;
    }

    DeleteFile dv = dvByPath.get(dataFile.location());
    if (dv != null) {
      ValidationException.check(
          dv.dataSequenceNumber() >= seq,
          "DV data sequence number (%s) must be greater than or equal to data file sequence number (%s)",
          dv.dataSequenceNumber(),
          seq);
    }
    return dv;
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private static boolean canContainEqDeletesForFile(
      DataFile dataFile, EqualityDeleteFile deleteFile) {
    Map<Integer, ByteBuffer> dataLowers = dataFile.lowerBounds();
    Map<Integer, ByteBuffer> dataUppers = dataFile.upperBounds();

    // whether to check data ranges or to assume that the ranges match
    // if upper/lower bounds are missing, null counts may still be used to determine delete files
    // can be skipped
    boolean checkRanges =
        dataLowers != null && dataUppers != null && deleteFile.hasLowerAndUpperBounds();

    Map<Integer, Long> dataNullCounts = dataFile.nullValueCounts();
    Map<Integer, Long> dataValueCounts = dataFile.valueCounts();
    Map<Integer, Long> deleteNullCounts = deleteFile.nullValueCounts();
    Map<Integer, Long> deleteValueCounts = deleteFile.valueCounts();

    for (Types.NestedField field : deleteFile.equalityFields()) {
      if (!field.type().isPrimitiveType()) {
        // stats are not kept for nested types. assume that the delete file may match
        continue;
      }

      if (containsNull(dataNullCounts, field) && containsNull(deleteNullCounts, field)) {
        // the data has null values and null has been deleted, so the deletes must be applied
        continue;
      }

      if (allNull(dataNullCounts, dataValueCounts, field) && allNonNull(deleteNullCounts, field)) {
        // the data file contains only null values for this field, but there are no deletes for null
        // values
        return false;
      }

      if (allNull(deleteNullCounts, deleteValueCounts, field)
          && allNonNull(dataNullCounts, field)) {
        // the delete file removes only null rows with null for this field, but there are no data
        // rows with null
        return false;
      }

      if (!checkRanges) {
        // some upper and lower bounds are missing, assume they match
        continue;
      }

      int id = field.fieldId();
      ByteBuffer dataLower = dataLowers.get(id);
      ByteBuffer dataUpper = dataUppers.get(id);
      Object deleteLower = deleteFile.lowerBound(id);
      Object deleteUpper = deleteFile.upperBound(id);
      if (dataLower == null || dataUpper == null || deleteLower == null || deleteUpper == null) {
        // at least one bound is not known, assume the delete file may match
        continue;
      }

      if (!rangesOverlap(field, dataLower, dataUpper, deleteLower, deleteUpper)) {
        // no values overlap between the data file and the deletes
        return false;
      }
    }

    return true;
  }

  private static <T> boolean rangesOverlap(
      Types.NestedField field,
      ByteBuffer dataLowerBuf,
      ByteBuffer dataUpperBuf,
      T deleteLower,
      T deleteUpper) {
    Type.PrimitiveType type = field.type().asPrimitiveType();
    Comparator<T> comparator = Comparators.forType(type);

    T dataLower = Conversions.fromByteBuffer(type, dataLowerBuf);
    if (comparator.compare(dataLower, deleteUpper) > 0) {
      return false;
    }

    T dataUpper = Conversions.fromByteBuffer(type, dataUpperBuf);
    if (comparator.compare(deleteLower, dataUpper) > 0) {
      return false;
    }

    return true;
  }

  private static boolean allNonNull(Map<Integer, Long> nullValueCounts, Types.NestedField field) {
    if (field.isRequired()) {
      return true;
    }

    if (nullValueCounts == null) {
      return false;
    }

    Long nullValueCount = nullValueCounts.get(field.fieldId());
    if (nullValueCount == null) {
      return false;
    }

    return nullValueCount <= 0;
  }

  private static boolean allNull(
      Map<Integer, Long> nullValueCounts, Map<Integer, Long> valueCounts, Types.NestedField field) {
    if (field.isRequired()) {
      return false;
    }

    if (nullValueCounts == null || valueCounts == null) {
      return false;
    }

    Long nullValueCount = nullValueCounts.get(field.fieldId());
    Long valueCount = valueCounts.get(field.fieldId());
    if (nullValueCount == null || valueCount == null) {
      return false;
    }

    return nullValueCount.equals(valueCount);
  }

  private static boolean containsNull(Map<Integer, Long> nullValueCounts, Types.NestedField field) {
    if (field.isRequired()) {
      return false;
    }

    if (nullValueCounts == null) {
      return true;
    }

    Long nullValueCount = nullValueCounts.get(field.fieldId());
    if (nullValueCount == null) {
      return true;
    }

    return nullValueCount > 0;
  }

  static Builder builderFor(FileIO io, Iterable<ManifestFile> deleteManifests) {
    return new Builder(io, Sets.newHashSet(deleteManifests));
  }

  // changed to public method.
  public static Builder builderFor(Iterable<DeleteFile> deleteFiles) {
    return new Builder(deleteFiles);
  }

  // changed to public class.
  public static class Builder {
    private final FileIO io;
    private final Set<ManifestFile> deleteManifests;
    private final Iterable<DeleteFile> deleteFiles;
    private long minSequenceNumber = 0L;
    private Map<Integer, PartitionSpec> specsById = null;
    private Expression dataFilter = Expressions.alwaysTrue();
    private Expression partitionFilter = Expressions.alwaysTrue();
    private PartitionSet partitionSet = null;
    private boolean caseSensitive = true;
    private ExecutorService executorService = null;
    private ScanMetrics scanMetrics = ScanMetrics.noop();
    private boolean ignoreResiduals = false;

    Builder(FileIO io, Set<ManifestFile> deleteManifests) {
      this.io = io;
      this.deleteManifests = Sets.newHashSet(deleteManifests);
      this.deleteFiles = null;
    }

    Builder(Iterable<DeleteFile> deleteFiles) {
      this.io = null;
      this.deleteManifests = null;
      this.deleteFiles = deleteFiles;
    }

    Builder afterSequenceNumber(long seq) {
      this.minSequenceNumber = seq;
      return this;
    }

    public Builder specsById(Map<Integer, PartitionSpec> newSpecsById) {
      this.specsById = newSpecsById;
      return this;
    }

    Builder filterData(Expression newDataFilter) {
      Preconditions.checkArgument(
          deleteFiles == null, "Index constructed from files does not support data filters");
      this.dataFilter = Expressions.and(dataFilter, newDataFilter);
      return this;
    }

    Builder filterPartitions(Expression newPartitionFilter) {
      Preconditions.checkArgument(
          deleteFiles == null, "Index constructed from files does not support partition filters");
      this.partitionFilter = Expressions.and(partitionFilter, newPartitionFilter);
      return this;
    }

    Builder filterPartitions(PartitionSet newPartitionSet) {
      Preconditions.checkArgument(
          deleteFiles == null, "Index constructed from files does not support partition filters");
      this.partitionSet = newPartitionSet;
      return this;
    }

    public Builder caseSensitive(boolean newCaseSensitive) {
      this.caseSensitive = newCaseSensitive;
      return this;
    }

    Builder planWith(ExecutorService newExecutorService) {
      this.executorService = newExecutorService;
      return this;
    }

    Builder scanMetrics(ScanMetrics newScanMetrics) {
      this.scanMetrics = newScanMetrics;
      return this;
    }

    Builder ignoreResiduals() {
      this.ignoreResiduals = true;
      return this;
    }

    private Iterable<DeleteFile> filterDeleteFiles() {
      return Iterables.filter(deleteFiles, file -> file.dataSequenceNumber() > minSequenceNumber);
    }

    private Collection<DeleteFile> loadDeleteFiles() {
      // read all of the matching delete manifests in parallel and accumulate the matching files in
      // a queue
      Queue<DeleteFile> files = new ConcurrentLinkedQueue<>();
      Tasks.foreach(deleteManifestReaders())
          .stopOnFailure()
          .throwFailureWhenFinished()
          .executeWith(executorService)
          .run(
              deleteFile -> {
                try (CloseableIterable<ManifestEntry<DeleteFile>> reader = deleteFile) {
                  for (ManifestEntry<DeleteFile> entry : reader) {
                    if (entry.dataSequenceNumber() > minSequenceNumber) {
                      // copy with stats for better filtering against data file stats
                      files.add(entry.file().copy());
                    }
                  }
                } catch (IOException e) {
                  throw new RuntimeIOException(e, "Failed to close");
                }
              });
      return files;
    }

    public DeleteFileIndex build() {
      Iterable<DeleteFile> files = deleteFiles != null ? filterDeleteFiles() : loadDeleteFiles();

      EqualityDeletes globalDeletes = new EqualityDeletes();
      PartitionMap<EqualityDeletes> eqDeletesByPartition = PartitionMap.create(specsById);
      PartitionMap<PositionDeletes> posDeletesByPartition = PartitionMap.create(specsById);
      Map<String, PositionDeletes> posDeletesByPath = Maps.newHashMap();
      Map<String, DeleteFile> dvByPath = Maps.newHashMap();

      for (DeleteFile file : files) {
        switch (file.content()) {
          case POSITION_DELETES:
            if (ContentFileUtil.isDV(file)) {
              add(dvByPath, file);
            } else {
              add(posDeletesByPath, posDeletesByPartition, file);
            }
            break;
          case EQUALITY_DELETES:
            add(globalDeletes, eqDeletesByPartition, file);
            break;
          default:
            throw new UnsupportedOperationException("Unsupported content: " + file.content());
        }
        ScanMetricsUtil.indexedDeleteFile(scanMetrics, file);
      }

      return new DeleteFileIndex(
          globalDeletes.isEmpty() ? null : globalDeletes,
          eqDeletesByPartition.isEmpty() ? null : eqDeletesByPartition,
          posDeletesByPartition.isEmpty() ? null : posDeletesByPartition,
          posDeletesByPath.isEmpty() ? null : posDeletesByPath,
          dvByPath.isEmpty() ? null : dvByPath);
    }

    private void add(Map<String, DeleteFile> dvByPath, DeleteFile dv) {
      String path = dv.referencedDataFile();
      DeleteFile existingDV = dvByPath.putIfAbsent(path, dv);
      if (existingDV != null) {
        throw new ValidationException(
            "Can't index multiple DVs for %s: %s and %s",
            path, ContentFileUtil.dvDesc(dv), ContentFileUtil.dvDesc(existingDV));
      }
    }

    private void add(
        Map<String, PositionDeletes> deletesByPath,
        PartitionMap<PositionDeletes> deletesByPartition,
        DeleteFile file) {
      String path = ContentFileUtil.referencedDataFileLocation(file);

      PositionDeletes deletes;
      if (path != null) {
        deletes = deletesByPath.computeIfAbsent(path, ignored -> new PositionDeletes());
      } else {
        int specId = file.specId();
        StructLike partition = file.partition();
        deletes = deletesByPartition.computeIfAbsent(specId, partition, PositionDeletes::new);
      }

      deletes.add(file);
    }

    private void add(
        EqualityDeletes globalDeletes,
        PartitionMap<EqualityDeletes> deletesByPartition,
        DeleteFile file) {
      PartitionSpec spec = specsById.get(file.specId());

      EqualityDeletes deletes;
      if (spec.isUnpartitioned()) {
        deletes = globalDeletes;
      } else {
        int specId = spec.specId();
        StructLike partition = file.partition();
        deletes = deletesByPartition.computeIfAbsent(specId, partition, EqualityDeletes::new);
      }

      deletes.add(spec, file);
    }

    private Iterable<CloseableIterable<ManifestEntry<DeleteFile>>> deleteManifestReaders() {
      Expression entryFilter = ignoreResiduals ? Expressions.alwaysTrue() : dataFilter;

      LoadingCache<Integer, Expression> partExprCache =
          specsById == null
              ? null
              : Caffeine.newBuilder()
                  .build(
                      specId -> {
                        PartitionSpec spec = specsById.get(specId);
                        return Projections.inclusive(spec, caseSensitive).project(dataFilter);
                      });

      LoadingCache<Integer, ManifestEvaluator> evalCache =
          specsById == null
              ? null
              : Caffeine.newBuilder()
                  .build(
                      specId -> {
                        PartitionSpec spec = specsById.get(specId);
                        return ManifestEvaluator.forPartitionFilter(
                            Expressions.and(partitionFilter, partExprCache.get(specId)),
                            spec,
                            caseSensitive);
                      });

      CloseableIterable<ManifestFile> closeableDeleteManifests =
          CloseableIterable.withNoopClose(deleteManifests);
      CloseableIterable<ManifestFile> matchingManifests =
          evalCache == null
              ? closeableDeleteManifests
              : CloseableIterable.filter(
                  scanMetrics.skippedDeleteManifests(),
                  closeableDeleteManifests,
                  manifest ->
                      manifest.content() == ManifestContent.DELETES
                          && (manifest.hasAddedFiles() || manifest.hasExistingFiles())
                          && evalCache.get(manifest.partitionSpecId()).eval(manifest));

      matchingManifests =
          CloseableIterable.count(scanMetrics.scannedDeleteManifests(), matchingManifests);
      return Iterables.transform(
          matchingManifests,
          manifest ->
              ManifestFiles.readDeleteManifest(manifest, io, specsById)
                  .filterRows(entryFilter)
                  .filterPartitions(
                      Expressions.and(
                          partitionFilter, partExprCache.get(manifest.partitionSpecId())))
                  .filterPartitions(partitionSet)
                  .caseSensitive(caseSensitive)
                  .scanMetrics(scanMetrics)
                  .liveEntries());
    }
  }

  /**
   * Finds an index in the sorted array of sequence numbers where the given sequence number should
   * be inserted or is found.
   *
   * <p>If the sequence number is present in the array, this method returns the index of the first
   * occurrence of the sequence number. If the sequence number is not present, the method returns
   * the index where the sequence number would be inserted while maintaining the sorted order of the
   * array. This returned index ranges from 0 (inclusive) to the length of the array (inclusive).
   *
   * <p>This method is used to determine the subset of delete files that apply to a given data file.
   *
   * @param seqs an array of sequence numbers sorted in ascending order
   * @param seq the sequence number to search for
   * @return the index of the first occurrence or the insertion point
   */
  private static int findStartIndex(long[] seqs, long seq) {
    int pos = Arrays.binarySearch(seqs, seq);
    int start;
    if (pos < 0) {
      // the sequence number was not found, where it would be inserted is -(pos + 1)
      start = -(pos + 1);
    } else {
      // the sequence number was found, but may not be the first
      // find the first delete file with the given sequence number by decrementing the position
      start = pos;
      while (start > 0 && seqs[start - 1] >= seq) {
        start -= 1;
      }
    }

    return start;
  }

  private static DeleteFile[] concat(DeleteFile[]... deletes) {
    return ArrayUtil.concat(DeleteFile.class, deletes);
  }

  // a group of position delete files sorted by the sequence number they apply to
  static class PositionDeletes {
    private static final Comparator<DeleteFile> SEQ_COMPARATOR =
        Comparator.comparingLong(DeleteFile::dataSequenceNumber);

    // indexed state
    private long[] seqs = null;
    private DeleteFile[] files = null;

    // a buffer that is used to hold files before indexing
    private volatile List<DeleteFile> buffer = Lists.newArrayList();

    public void add(DeleteFile file) {
      Preconditions.checkState(buffer != null, "Can't add files upon indexing");
      buffer.add(file);
    }

    public DeleteFile[] filter(long seq) {
      indexIfNeeded();

      int start = findStartIndex(seqs, seq);

      if (start >= files.length) {
        return EMPTY_DELETES;
      }

      if (start == 0) {
        return files;
      }

      int matchingFilesCount = files.length - start;
      DeleteFile[] matchingFiles = new DeleteFile[matchingFilesCount];
      System.arraycopy(files, start, matchingFiles, 0, matchingFilesCount);
      return matchingFiles;
    }

    public Iterable<DeleteFile> referencedDeleteFiles() {
      indexIfNeeded();
      return Arrays.asList(files);
    }

    public boolean isEmpty() {
      indexIfNeeded();
      return files.length == 0;
    }

    private void indexIfNeeded() {
      if (buffer != null) {
        synchronized (this) {
          if (buffer != null) {
            this.files = indexFiles(buffer);
            this.seqs = indexSeqs(files);
            this.buffer = null;
          }
        }
      }
    }

    private static DeleteFile[] indexFiles(List<DeleteFile> list) {
      DeleteFile[] array = list.toArray(EMPTY_DELETES);
      Arrays.sort(array, SEQ_COMPARATOR);
      return array;
    }

    private static long[] indexSeqs(DeleteFile[] files) {
      long[] seqs = new long[files.length];

      for (int index = 0; index < files.length; index++) {
        seqs[index] = files[index].dataSequenceNumber();
      }

      return seqs;
    }
  }

  // a group of equality delete files sorted by the sequence number they apply to
  static class EqualityDeletes {
    private static final Comparator<EqualityDeleteFile> SEQ_COMPARATOR =
        Comparator.comparingLong(EqualityDeleteFile::applySequenceNumber);
    private static final EqualityDeleteFile[] EMPTY_EQUALITY_DELETES = new EqualityDeleteFile[0];

    // indexed state
    private long[] seqs = null;
    private EqualityDeleteFile[] files = null;

    // a buffer that is used to hold files before indexing
    private volatile List<EqualityDeleteFile> buffer = Lists.newArrayList();

    public void add(PartitionSpec spec, DeleteFile file) {
      Preconditions.checkState(buffer != null, "Can't add files upon indexing");
      buffer.add(new EqualityDeleteFile(spec, file));
    }

    public DeleteFile[] filter(long seq, DataFile dataFile) {
      indexIfNeeded();

      int start = findStartIndex(seqs, seq);

      if (start >= files.length) {
        return EMPTY_DELETES;
      }

      List<DeleteFile> matchingFiles = Lists.newArrayList();

      for (int index = start; index < files.length; index++) {
        EqualityDeleteFile file = files[index];
        if (canContainEqDeletesForFile(dataFile, file)) {
          matchingFiles.add(file.wrapped());
        }
      }

      return matchingFiles.toArray(EMPTY_DELETES);
    }

    public Iterable<DeleteFile> referencedDeleteFiles() {
      indexIfNeeded();
      return Iterables.transform(Arrays.asList(files), EqualityDeleteFile::wrapped);
    }

    public boolean isEmpty() {
      indexIfNeeded();
      return files.length == 0;
    }

    private void indexIfNeeded() {
      if (buffer != null) {
        synchronized (this) {
          if (buffer != null) {
            this.files = indexFiles(buffer);
            this.seqs = indexSeqs(files);
            this.buffer = null;
          }
        }
      }
    }

    private static EqualityDeleteFile[] indexFiles(List<EqualityDeleteFile> list) {
      EqualityDeleteFile[] array = list.toArray(EMPTY_EQUALITY_DELETES);
      Arrays.sort(array, SEQ_COMPARATOR);
      return array;
    }

    private static long[] indexSeqs(EqualityDeleteFile[] files) {
      long[] seqs = new long[files.length];

      for (int index = 0; index < files.length; index++) {
        seqs[index] = files[index].applySequenceNumber();
      }

      return seqs;
    }
  }

  // an equality delete file wrapper that caches the converted boundaries for faster boundary checks
  // this class is not meant to be exposed beyond the delete file index
  private static class EqualityDeleteFile {
    private final PartitionSpec spec;
    private final DeleteFile wrapped;
    private final long applySequenceNumber;
    private volatile List<Types.NestedField> equalityFields = null;
    private volatile Map<Integer, Object> convertedLowerBounds = null;
    private volatile Map<Integer, Object> convertedUpperBounds = null;

    EqualityDeleteFile(PartitionSpec spec, DeleteFile file) {
      this.spec = spec;
      this.wrapped = file;
      this.applySequenceNumber = wrapped.dataSequenceNumber() - 1;
    }

    public DeleteFile wrapped() {
      return wrapped;
    }

    public long applySequenceNumber() {
      return applySequenceNumber;
    }

    public List<Types.NestedField> equalityFields() {
      if (equalityFields == null) {
        synchronized (this) {
          if (equalityFields == null) {
            List<Types.NestedField> fields = Lists.newArrayList();
            for (int id : wrapped.equalityFieldIds()) {
              Types.NestedField field = spec.schema().findField(id);
              fields.add(field);
            }
            this.equalityFields = fields;
          }
        }
      }

      return equalityFields;
    }

    public Map<Integer, Long> valueCounts() {
      return wrapped.valueCounts();
    }

    public Map<Integer, Long> nullValueCounts() {
      return wrapped.nullValueCounts();
    }

    public boolean hasLowerAndUpperBounds() {
      return wrapped.lowerBounds() != null && wrapped.upperBounds() != null;
    }

    @SuppressWarnings("unchecked")
    public <T> T lowerBound(int id) {
      return (T) lowerBounds().get(id);
    }

    private Map<Integer, Object> lowerBounds() {
      if (convertedLowerBounds == null) {
        synchronized (this) {
          if (convertedLowerBounds == null) {
            this.convertedLowerBounds = convertBounds(wrapped.lowerBounds());
          }
        }
      }

      return convertedLowerBounds;
    }

    @SuppressWarnings("unchecked")
    public <T> T upperBound(int id) {
      return (T) upperBounds().get(id);
    }

    private Map<Integer, Object> upperBounds() {
      if (convertedUpperBounds == null) {
        synchronized (this) {
          if (convertedUpperBounds == null) {
            this.convertedUpperBounds = convertBounds(wrapped.upperBounds());
          }
        }
      }

      return convertedUpperBounds;
    }

    private Map<Integer, Object> convertBounds(Map<Integer, ByteBuffer> bounds) {
      Map<Integer, Object> converted = Maps.newHashMap();

      if (bounds != null) {
        for (Types.NestedField field : equalityFields()) {
          int id = field.fieldId();
          Type type = spec.schema().findField(id).type();
          if (type.isPrimitiveType()) {
            ByteBuffer bound = bounds.get(id);
            if (bound != null) {
              converted.put(id, Conversions.fromByteBuffer(type, bound));
            }
          }
        }
      }

      return converted;
    }
  }
}
