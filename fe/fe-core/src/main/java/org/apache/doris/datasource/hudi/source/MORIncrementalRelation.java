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

import org.apache.doris.spi.Split;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.GlobPattern;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MORIncrementalRelation implements IncrementalRelation {
    private final Map<String, String> optParams;
    private final HoodieTableMetaClient metaClient;
    private final HoodieTimeline timeline;
    private final HollowCommitHandling hollowCommitHandling;
    private String startTimestamp;
    private final String endTimestamp;
    private final boolean startInstantArchived;
    private final boolean endInstantArchived;
    private final List<HoodieInstant> includedCommits;
    private final List<HoodieCommitMetadata> commitsMetadata;
    private final FileStatus[] affectedFilesInCommits;
    private final boolean fullTableScan;
    private final String globPattern;
    private final boolean includeStartTime;
    private final String startTs;
    private final String endTs;


    public MORIncrementalRelation(Map<String, String> optParams, Configuration configuration,
            HoodieTableMetaClient metaClient)
            throws HoodieException, IOException {
        this.optParams = optParams;
        this.metaClient = metaClient;
        timeline = metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants();
        if (timeline.empty()) {
            throw new HoodieException("No instants to incrementally pull");
        }
        if (!metaClient.getTableConfig().populateMetaFields()) {
            throw new HoodieException("Incremental queries are not supported when meta fields are disabled");
        }
        hollowCommitHandling = HollowCommitHandling.valueOf(
                optParams.getOrDefault("hoodie.read.timeline.holes.resolution.policy", "FAIL"));

        startTimestamp = optParams.get("hoodie.datasource.read.begin.instanttime");
        if (startTimestamp == null) {
            throw new HoodieException("Specify the begin instant time to pull from using "
                    + "option hoodie.datasource.read.begin.instanttime");
        }
        if (EARLIEST_TIME.equals(startTimestamp)) {
            startTimestamp = "000";
        }
        endTimestamp = optParams.getOrDefault("hoodie.datasource.read.end.instanttime",
                hollowCommitHandling == HollowCommitHandling.USE_TRANSITION_TIME
                        ? timeline.lastInstant().get().getStateTransitionTime()
                        : timeline.lastInstant().get().getTimestamp());

        startInstantArchived = timeline.isBeforeTimelineStarts(startTimestamp);
        endInstantArchived = timeline.isBeforeTimelineStarts(endTimestamp);

        includedCommits = getIncludedCommits();
        commitsMetadata = getCommitsMetadata();
        affectedFilesInCommits = HoodieInputFormatUtils.listAffectedFilesForCommits(configuration,
                new Path(metaClient.getBasePath()), commitsMetadata);
        fullTableScan = shouldFullTableScan();
        if (hollowCommitHandling == HollowCommitHandling.USE_TRANSITION_TIME && fullTableScan) {
            throw new HoodieException("Cannot use stateTransitionTime while enables full table scan");
        }
        globPattern = optParams.getOrDefault("hoodie.datasource.read.incr.path.glob", "");

        if (startInstantArchived) {
            includeStartTime = false;
            startTs = startTimestamp;
        } else {
            includeStartTime = true;
            startTs = includedCommits.isEmpty() ? startTimestamp : includedCommits.get(0).getTimestamp();
        }
        endTs = endInstantArchived || includedCommits.isEmpty() ? endTimestamp
                : includedCommits.get(includedCommits.size() - 1).getTimestamp();
    }

    @Override
    public Map<String, String> getHoodieParams() {
        optParams.put("hoodie.datasource.read.incr.operation", "true");
        optParams.put("hoodie.datasource.read.begin.instanttime", startTs);
        optParams.put("hoodie.datasource.read.end.instanttime", endTs);
        optParams.put("hoodie.datasource.read.incr.includeStartTime", includeStartTime ? "true" : "false");
        return optParams;
    }

    private List<HoodieInstant> getIncludedCommits() {
        if (!startInstantArchived || !endInstantArchived) {
            // If endTimestamp commit is not archived, will filter instants
            // before endTimestamp.
            if (hollowCommitHandling == HollowCommitHandling.USE_TRANSITION_TIME) {
                return timeline.findInstantsInRangeByStateTransitionTime(startTimestamp, endTimestamp).getInstants();
            } else {
                return timeline.findInstantsInRange(startTimestamp, endTimestamp).getInstants();
            }
        } else {
            return timeline.getInstants();
        }
    }

    private List<HoodieCommitMetadata> getCommitsMetadata() throws IOException {
        List<HoodieCommitMetadata> result = new ArrayList<>();
        for (HoodieInstant commit : includedCommits) {
            result.add(TimelineUtils.getCommitMetadata(commit, timeline));
        }
        return result;
    }

    private boolean shouldFullTableScan() throws IOException {
        boolean should = Boolean.parseBoolean(
                optParams.getOrDefault("hoodie.datasource.read.incr.fallback.fulltablescan.enable", "false")) && (
                startInstantArchived || endInstantArchived);
        if (should) {
            return true;
        }
        for (FileStatus fileStatus : affectedFilesInCommits) {
            if (!metaClient.getFs().exists(fileStatus.getPath())) {
                return true;
            }
        }
        return false;
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

    @Override
    public List<FileSlice> collectFileSlices() throws HoodieException {
        if (includedCommits.isEmpty()) {
            return Collections.emptyList();
        } else if (fullTableScan) {
            throw new HoodieException("Fallback to full table scan");
        }
        HoodieTimeline scanTimeline;
        if (hollowCommitHandling == HollowCommitHandling.USE_TRANSITION_TIME) {
            scanTimeline = metaClient.getCommitsAndCompactionTimeline()
                    .findInstantsInRangeByStateTransitionTime(startTimestamp, endTimestamp);
        } else {
            scanTimeline = TimelineUtils.handleHollowCommitIfNeeded(
                            metaClient.getCommitsAndCompactionTimeline(), metaClient, hollowCommitHandling)
                    .findInstantsInRange(startTimestamp, endTimestamp);
        }
        String latestCommit = includedCommits.get(includedCommits.size() - 1).getTimestamp();
        HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, scanTimeline,
                affectedFilesInCommits);
        Stream<FileSlice> fileSlices = HoodieInputFormatUtils.getWritePartitionPaths(commitsMetadata)
                .stream().flatMap(relativePartitionPath ->
                        fsView.getLatestMergedFileSlicesBeforeOrOn(relativePartitionPath, latestCommit));
        if ("".equals(globPattern)) {
            return fileSlices.collect(Collectors.toList());
        }
        GlobPattern globMatcher = new GlobPattern("*" + globPattern);
        return fileSlices.filter(fileSlice -> globMatcher.matches(fileSlice.getBaseFile().map(BaseFile::getPath)
                .or(fileSlice.getLatestLogFile().map(f -> f.getPath().toString())).get())).collect(Collectors.toList());
    }

    @Override
    public List<Split> collectSplits() throws HoodieException {
        throw new UnsupportedOperationException();
    }
}
