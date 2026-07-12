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

package org.apache.doris.connector.hudi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobPattern;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * Selects the base files a COW {@code @incr(...)} read must scan over a resolved {@code (begin, end]} window.
 * Connector-internal port of legacy {@code datasource.hudi.source.COWIncrementalRelation}, with the window-parse
 * prologue removed (the window is resolved ONCE by {@link HudiConnectorMetadata#resolveTimeTravel} and consumed
 * here, see {@link IncrementalRelation}) and the split type re-homed from fe-core {@code HudiSplit} to
 * {@link HudiScanRange}. Everything from the archived-flag computation onward is byte-faithful to legacy.
 *
 * <p>{@link #collectSplits()} yields native ranges directly (COW has only base files); {@link #collectFileSlices()}
 * is unsupported (the MOR shape).
 */
final class COWIncrementalRelation implements IncrementalRelation {

    private final Map<String, String> optParams;
    private final HoodieTableMetaClient metaClient;
    private final HollowCommitHandling hollowCommitHandling;
    private final boolean startInstantArchived;
    private final boolean endInstantArchived;
    private final boolean fullTableScan;
    private final FileSystem fs;
    private final Map<String, HoodieWriteStat> fileToWriteStat;
    private final Collection<String> filteredRegularFullPaths;
    private final Collection<String> filteredMetaBootstrapFullPaths;

    private final String startTs;
    private final String endTs;

    COWIncrementalRelation(HoodieTableMetaClient metaClient, Configuration configuration,
            String startTs, String endTs, HollowCommitHandling hollowCommitHandling,
            Map<String, String> optParams) throws IOException {
        this.optParams = optParams;
        this.metaClient = metaClient;
        this.hollowCommitHandling = hollowCommitHandling;
        this.startTs = startTs;
        this.endTs = endTs;
        HoodieTimeline commitTimeline = TimelineUtils.handleHollowCommitIfNeeded(
                metaClient.getCommitTimeline().filterCompletedInstants(), metaClient, hollowCommitHandling);
        // Meta-fields guard (ported from INC-1's deferral list): fail loud before any file selection. It is the
        // first guard because the empty-completed-timeline case routes to EmptyIncrementalRelation upstream, so
        // this relation is built only for a non-empty timeline (legacy's "non-empty only" semantics).
        IncrementalRelation.checkIncrementalMetaFields(metaClient.getTableConfig().populateMetaFields());

        startInstantArchived = commitTimeline.isBeforeTimelineStarts(startTs);
        endInstantArchived = commitTimeline.isBeforeTimelineStarts(endTs);

        HoodieTimeline commitsTimelineToReturn;
        if (hollowCommitHandling == HollowCommitHandling.USE_TRANSITION_TIME) {
            commitsTimelineToReturn = commitTimeline.findInstantsInRangeByCompletionTime(startTs, endTs);
        } else {
            commitsTimelineToReturn = commitTimeline.findInstantsInRange(startTs, endTs);
        }
        List<HoodieInstant> commitsToReturn = commitsTimelineToReturn.getInstants();

        // todo: support configuration hoodie.datasource.read.incr.filters
        StoragePath basePath = metaClient.getBasePath();
        Map<String, String> regularFileIdToFullPath = new HashMap<>();
        Map<String, String> metaBootstrapFileIdToFullPath = new HashMap<>();
        HoodieTimeline replacedTimeline = commitsTimelineToReturn.getCompletedReplaceTimeline();
        Map<String, String> replacedFile = new HashMap<>();
        for (HoodieInstant instant : replacedTimeline.getInstants()) {
            HoodieReplaceCommitMetadata metadata = metaClient.getActiveTimeline()
                    .readReplaceCommitMetadata(instant);
            metadata.getPartitionToReplaceFileIds().forEach(
                    (key, value) -> value.forEach(
                            e -> replacedFile.put(e, FSUtils.constructAbsolutePath(basePath, key).toString())));
        }

        fileToWriteStat = new HashMap<>();
        for (HoodieInstant commit : commitsToReturn) {
            HoodieCommitMetadata metadata = metaClient.getActiveTimeline().readCommitMetadata(commit);
            metadata.getPartitionToWriteStats().forEach((partition, stats) -> {
                for (HoodieWriteStat stat : stats) {
                    fileToWriteStat.put(FSUtils.constructAbsolutePath(basePath, stat.getPath()).toString(), stat);
                }
            });
            if (HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS.equals(commit.requestedTime())) {
                metadata.getFileIdAndFullPaths(basePath).forEach((k, v) -> {
                    if (!(replacedFile.containsKey(k) && v.startsWith(replacedFile.get(k)))) {
                        metaBootstrapFileIdToFullPath.put(k, v);
                    }
                });
            } else {
                metadata.getFileIdAndFullPaths(basePath).forEach((k, v) -> {
                    if (!(replacedFile.containsKey(k) && v.startsWith(replacedFile.get(k)))) {
                        regularFileIdToFullPath.put(k, v);
                    }
                });
            }
        }

        if (!metaBootstrapFileIdToFullPath.isEmpty()) {
            // filer out meta bootstrap files that have had more commits since metadata bootstrap
            metaBootstrapFileIdToFullPath.entrySet().removeIf(e -> regularFileIdToFullPath.containsKey(e.getKey()));
        }
        String pathGlobPattern = optParams.getOrDefault("hoodie.datasource.read.incr.path.glob", "");
        if ("".equals(pathGlobPattern)) {
            filteredRegularFullPaths = regularFileIdToFullPath.values();
            filteredMetaBootstrapFullPaths = metaBootstrapFileIdToFullPath.values();
        } else {
            GlobPattern globMatcher = new GlobPattern("*" + pathGlobPattern);
            filteredRegularFullPaths = regularFileIdToFullPath.values().stream().filter(globMatcher::matches)
                    .collect(Collectors.toList());
            filteredMetaBootstrapFullPaths = metaBootstrapFileIdToFullPath.values().stream()
                    .filter(globMatcher::matches).collect(Collectors.toList());
        }

        fs = new Path(basePath.toUri().getPath()).getFileSystem(configuration);
        fullTableScan = shouldFullTableScan();
    }

    private boolean shouldFullTableScan() throws IOException {
        boolean fallbackToFullTableScan = Boolean.parseBoolean(
                optParams.getOrDefault("hoodie.datasource.read.incr.fallback.fulltablescan.enable", "false"));
        if (IncrementalRelation.decideArchivalFullTableScan(
                fallbackToFullTableScan, startInstantArchived, endInstantArchived, hollowCommitHandling)) {
            return true;
        }
        if (fallbackToFullTableScan) {
            for (String path : filteredMetaBootstrapFullPaths) {
                if (!fs.exists(new Path(path))) {
                    return true;
                }
            }
            for (String path : filteredRegularFullPaths) {
                if (!fs.exists(new Path(path))) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public List<FileSlice> collectFileSlices() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<HudiScanRange> collectSplits(UnaryOperator<String> nativePathNormalizer) {
        IncrementalRelation.checkNotFullTableScan(fullTableScan);
        if (filteredRegularFullPaths.isEmpty() && filteredMetaBootstrapFullPaths.isEmpty()) {
            return Collections.emptyList();
        }
        List<HudiScanRange> splits = new ArrayList<>();
        // Partition-column NAMES come from the hudi table config (byte-faithful to legacy COW:212), NOT the
        // HMS-sourced handle.partitionKeyNames the snapshot path uses; the two coincide for hive-synced tables.
        Option<String[]> partitionColumns = metaClient.getTableConfig().getPartitionFields();
        List<String> partitionNames = partitionColumns.isPresent() ? Arrays.asList(partitionColumns.get())
                : Collections.emptyList();

        Consumer<String> generatorSplit = baseFile -> {
            HoodieWriteStat stat = fileToWriteStat.get(baseFile);
            splits.add(new HudiScanRange.Builder()
                    // Native COW @incr range: normalize scheme (s3a->s3) for BE's native reader. The raw baseFile
                    // is a full HMS path anchored on metaClient.getBasePath(); BE's S3URI rejects s3a.
                    .path(nativePathNormalizer.apply(baseFile))
                    .start(0)
                    // length + fileSize both from the write stat, matching legacy HudiSplit(0, size, size, ...).
                    .length(stat.getFileSizeInBytes())
                    .fileSize(stat.getFileSizeInBytes())
                    .fileFormat(HudiScanPlanProvider.detectFileFormat(baseFile))
                    .partitionValues(
                            HudiScanPlanProvider.parsePartitionValues(stat.getPartitionPath(), partitionNames))
                    .build());
        };

        for (String baseFile : filteredMetaBootstrapFullPaths) {
            generatorSplit.accept(baseFile);
        }
        for (String baseFile : filteredRegularFullPaths) {
            generatorSplit.accept(baseFile);
        }
        return splits;
    }

    @Override
    public boolean fallbackFullTableScan() {
        return fullTableScan;
    }

    @Override
    public String getEndTs() {
        return endTs;
    }
}
