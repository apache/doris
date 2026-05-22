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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobPattern;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.storage.HoodieStorageStrategyFactory;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MORIncrementalRelation implements IncrementalRelation {
    private final Map<String, String> optParams;
    private final HoodieTableMetaClient metaClient;
    private final HoodieTimeline timeline;
    private final boolean useStateTransitionTime;
    private String startTimestamp;
    private String endTimestamp;
    private final boolean startInstantArchived;
    private final boolean endInstantArchived;
    private final List<HoodieInstant> includedCommits;
    private final List<HoodieCommitMetadata> commitsMetadata;
    private final List<FileStatus> affectedFilesInCommits;
    private final boolean fullTableScan;
    private final String globPattern;
    private final String startTs;
    private final String endTs;


    public MORIncrementalRelation(Map<String, String> optParams, Configuration configuration,
            HoodieTableMetaClient metaClient)
            throws HoodieException, IOException {
        this.optParams = optParams;
        this.metaClient = metaClient;
        timeline = metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants();
        String holesPolicy = optParams.getOrDefault(
                "hoodie.read.timeline.holes.resolution.policy", "FAIL");
        this.useStateTransitionTime = "USE_TRANSITION_TIME".equalsIgnoreCase(holesPolicy);
        if (timeline.empty()) {
            throw new HoodieException("No instants to incrementally pull");
        }
        if (!metaClient.getTableConfig().populateMetaFields()) {
            throw new HoodieException("Incremental queries are not supported when meta fields are disabled");
        }

        startTimestamp = optParams.get("hoodie.datasource.read.begin.instanttime");
        if (startTimestamp == null) {
            throw new HoodieException("Specify the begin instant time to pull from using "
                    + "option hoodie.datasource.read.begin.instanttime");
        }
        if (EARLIEST_TIME.equals(startTimestamp)) {
            startTimestamp = "000";
        }

        HoodieInstant lastInstant = timeline.lastInstant().get();
        String latestTime = useStateTransitionTime
                ? lastInstant.getStateTransitionTime()
                : lastInstant.getTimestamp();
        endTimestamp = optParams.getOrDefault("hoodie.datasource.read.end.instanttime", latestTime);
        if (LATEST_TIME.equals(latestTime)) {
            endTimestamp = latestTime;
        }

        startInstantArchived = timeline.isBeforeTimelineStarts(startTimestamp);
        endInstantArchived = timeline.isBeforeTimelineStarts(endTimestamp);

        includedCommits = getIncludedCommits();
        commitsMetadata = getCommitsMetadata();
        Map<HoodieInstant, HoodieCommitMetadata> commitMetadataMap = new LinkedHashMap<>();
        for (int i = 0; i < includedCommits.size(); i++) {
            commitMetadataMap.put(includedCommits.get(i), commitsMetadata.get(i));
        }
        FileStatus[] affected = HoodieInputFormatUtils.listAffectedFilesForCommits(configuration,
                new Path(metaClient.getBasePath()),
                commitMetadataMap,
                HoodieStorageStrategyFactory.getInstant(metaClient));
        affectedFilesInCommits = Arrays.asList(affected);
        fullTableScan = shouldFullTableScan(configuration);
        if (useStateTransitionTime && fullTableScan) {
            throw new HoodieException("Cannot use stateTransitionTime while enables full table scan");
        }
        globPattern = optParams.getOrDefault("hoodie.datasource.read.incr.path.glob", "");

        startTs = startTimestamp;
        endTs = endTimestamp;
    }

    @Override
    public Map<String, String> getHoodieParams() {
        optParams.put("hoodie.datasource.read.begin.instanttime", startTs);
        optParams.put("hoodie.datasource.read.end.instanttime", endTs);
        return optParams;
    }

    private List<HoodieInstant> getIncludedCommits() {
        if (!startInstantArchived || !endInstantArchived) {
            // If endTimestamp commit is not archived, will filter instants
            // before endTimestamp.
            if (useStateTransitionTime) {
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

    private boolean shouldFullTableScan(Configuration configuration) throws IOException {
        boolean should = Boolean.parseBoolean(
                optParams.getOrDefault("hoodie.datasource.read.incr.fallback.fulltablescan.enable", "false")) && (
                startInstantArchived || endInstantArchived);
        if (should) {
            return true;
        }
        FileSystem fs = metaClient.getRawFs(new Path(metaClient.getBasePath()));
        for (FileStatus fileStatus : affectedFilesInCommits) {
            if (!fs.exists(fileStatus.getPath())) {
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
        HoodieTimeline rawCommits = metaClient.getCommitsAndCompactionTimeline();
        HoodieTimeline scanTimeline;
        if (useStateTransitionTime) {
            scanTimeline = rawCommits.findInstantsInRangeByStateTransitionTime(startTimestamp, endTimestamp);
        } else {
            scanTimeline = rawCommits.findInstantsInRange(startTimestamp, endTimestamp);
        }
        String latestCommit = includedCommits.get(includedCommits.size() - 1).getTimestamp();
        FileStatus[] affected = affectedFilesInCommits.toArray(new FileStatus[0]);
        HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, scanTimeline, affected);
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
