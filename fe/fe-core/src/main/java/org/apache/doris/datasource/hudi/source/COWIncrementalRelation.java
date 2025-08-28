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

package org.apache.doris.datasource.hudi.source;

import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.spi.Split;

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
import org.apache.hudi.exception.HoodieException;
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
import java.util.stream.Collectors;

public class COWIncrementalRelation implements IncrementalRelation {
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

    private final boolean includeStartTime;
    private final String startTs;
    private final String endTs;

    public COWIncrementalRelation(Map<String, String> optParams, Configuration configuration,
            HoodieTableMetaClient metaClient)
            throws HoodieException, IOException {
        this.optParams = optParams;
        this.metaClient = metaClient;
        hollowCommitHandling = HollowCommitHandling.valueOf(
                optParams.getOrDefault("hoodie.read.timeline.holes.resolution.policy", "FAIL"));
        HoodieTimeline commitTimeline = TimelineUtils.handleHollowCommitIfNeeded(
                metaClient.getCommitTimeline().filterCompletedInstants(), metaClient, hollowCommitHandling);
        if (commitTimeline.empty()) {
            throw new HoodieException("No instants to incrementally pull");
        }
        if (!metaClient.getTableConfig().populateMetaFields()) {
            throw new HoodieException("Incremental queries are not supported when meta fields are disabled");
        }

        String startInstantTime = optParams.get("hoodie.datasource.read.begin.instanttime");
        if (startInstantTime == null) {
            throw new HoodieException("Specify the begin instant time to pull from using "
                    + "option hoodie.datasource.read.begin.instanttime");
        }
        if (EARLIEST_TIME.equals(startInstantTime)) {
            startInstantTime = "000";
        }
        String endInstantTime = optParams.getOrDefault("hoodie.datasource.read.end.instanttime",
                hollowCommitHandling == HollowCommitHandling.USE_TRANSITION_TIME
                        ? commitTimeline.lastInstant().get().getCompletionTime()
                        : commitTimeline.lastInstant().get().requestedTime());
        startInstantArchived = commitTimeline.isBeforeTimelineStarts(startInstantTime);
        endInstantArchived = commitTimeline.isBeforeTimelineStarts(endInstantTime);

        HoodieTimeline commitsTimelineToReturn;
        if (hollowCommitHandling == HollowCommitHandling.USE_TRANSITION_TIME) {
            commitsTimelineToReturn = commitTimeline.findInstantsInRangeByCompletionTime(startInstantTime,
                    endInstantTime);
        } else {
            commitsTimelineToReturn = commitTimeline.findInstantsInRange(startInstantTime, endInstantTime);
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
        includeStartTime = !fullTableScan;
        if (fullTableScan || commitsToReturn.isEmpty()) {
            startTs = startInstantTime;
            endTs = endInstantTime;
        } else {
            startTs = commitsToReturn.get(0).requestedTime();
            endTs = commitsToReturn.get(commitsToReturn.size() - 1).requestedTime();
        }
    }

    private boolean shouldFullTableScan() throws HoodieException, IOException {
        boolean fallbackToFullTableScan = Boolean.parseBoolean(
                optParams.getOrDefault("hoodie.datasource.read.incr.fallback.fulltablescan.enable", "false"));
        if (fallbackToFullTableScan && (startInstantArchived || endInstantArchived)) {
            if (hollowCommitHandling == HollowCommitHandling.USE_TRANSITION_TIME) {
                throw new HoodieException("Cannot use stateTransitionTime while enables full table scan");
            }
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
    public List<FileSlice> collectFileSlices() throws HoodieException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Split> collectSplits() throws HoodieException {
        if (fullTableScan) {
            throw new HoodieException("Fallback to full table scan");
        }
        if (filteredRegularFullPaths.isEmpty() && filteredMetaBootstrapFullPaths.isEmpty()) {
            return Collections.emptyList();
        }
        List<Split> splits = new ArrayList<>();
        Option<String[]> partitionColumns = metaClient.getTableConfig().getPartitionFields();
        List<String> partitionNames = partitionColumns.isPresent() ? Arrays.asList(partitionColumns.get())
                : Collections.emptyList();

        Consumer<String> generatorSplit =  baseFile -> {
            HoodieWriteStat stat = fileToWriteStat.get(baseFile);
            LocationPath locationPath = LocationPath.of(baseFile);
            HudiSplit hudiSplit = new HudiSplit(locationPath, 0,
                    stat.getFileSizeInBytes(), stat.getFileSizeInBytes(), new String[0],
                    HudiPartitionProcessor.parsePartitionValues(partitionNames, stat.getPartitionPath()));
            hudiSplit.setTableFormatType(TableFormatType.HUDI);
            splits.add(hudiSplit);
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
    public Map<String, String> getHoodieParams() {
        optParams.put("hoodie.datasource.read.incr.operation", "true");
        optParams.put("hoodie.datasource.read.begin.instanttime", startTs);
        optParams.put("hoodie.datasource.read.end.instanttime", endTs);
        optParams.put("hoodie.datasource.read.incr.includeStartTime", includeStartTime ? "true" : "false");
        return optParams;
    }

    @Override
    public boolean fallbackFullTableScan() {
        return fullTableScan;
    }

    @Override
    public boolean isIncludeStartTime() {
        return includeStartTime;
    }

    @Override
    public String getStartTs() {
        return startTs;
    }

    @Override
    public String getEndTs() {
        return endTs;
    }
}
