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
import org.apache.hadoop.fs.GlobPattern;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.StoragePathInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Selects the merged file slices a MOR {@code @incr(...)} read must scan over a resolved {@code (begin, end]}
 * window. Connector-internal port of legacy {@code datasource.hudi.source.MORIncrementalRelation}, with the
 * window-parse prologue removed (the window is resolved ONCE by
 * {@link HudiConnectorMetadata#resolveTimeTravel} and consumed here, see {@link IncrementalRelation}). The dead
 * {@code MORIncrementalRelation:92} sentinel bug (which tested {@code latestTime} instead of the end and so never
 * fired) is gone by construction — no sentinel re-resolution survives in the relation.
 *
 * <p>{@link #collectFileSlices()} yields the merged slices for the scan planner to turn into JNI ranges at
 * {@code endTs}; {@link #collectSplits()} is unsupported (the COW shape).
 */
final class MORIncrementalRelation implements IncrementalRelation {

    private final Map<String, String> optParams;
    private final HoodieTableMetaClient metaClient;
    private final HoodieTimeline timeline;
    private final HollowCommitHandling hollowCommitHandling;
    private final String startTimestamp;
    private final String endTimestamp;
    private final boolean startInstantArchived;
    private final boolean endInstantArchived;
    private final List<HoodieInstant> includedCommits;
    private final List<HoodieCommitMetadata> commitsMetadata;
    private final List<StoragePathInfo> affectedFilesInCommits;
    private final boolean fullTableScan;
    private final String globPattern;

    MORIncrementalRelation(HoodieTableMetaClient metaClient, Configuration configuration,
            String startTs, String endTs, HollowCommitHandling hollowCommitHandling,
            Map<String, String> optParams) throws IOException {
        this.optParams = optParams;
        this.metaClient = metaClient;
        this.hollowCommitHandling = hollowCommitHandling;
        this.startTimestamp = startTs;
        this.endTimestamp = endTs;
        timeline = metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants();
        // Meta-fields guard (ported from INC-1's deferral list): fail loud before any file selection. First guard
        // because the empty-completed-timeline case routes to EmptyIncrementalRelation upstream, so this relation
        // is built only for a non-empty timeline (legacy's "non-empty only" semantics).
        IncrementalRelation.checkIncrementalMetaFields(metaClient.getTableConfig().populateMetaFields());

        startInstantArchived = timeline.isBeforeTimelineStarts(startTimestamp);
        endInstantArchived = timeline.isBeforeTimelineStarts(endTimestamp);

        includedCommits = getIncludedCommits();
        commitsMetadata = getCommitsMetadata();
        affectedFilesInCommits = HoodieInputFormatUtils.listAffectedFilesForCommits(configuration,
                metaClient.getBasePath(), commitsMetadata);
        fullTableScan = shouldFullTableScan();
        IncrementalRelation.checkStateTransitionTimeFullTableScan(hollowCommitHandling, fullTableScan);
        globPattern = optParams.getOrDefault("hoodie.datasource.read.incr.path.glob", "");
    }

    private List<HoodieInstant> getIncludedCommits() {
        if (!startInstantArchived || !endInstantArchived) {
            // If endTimestamp commit is not archived, will filter instants
            // before endTimestamp.
            if (hollowCommitHandling == HollowCommitHandling.USE_TRANSITION_TIME) {
                return timeline.findInstantsInRangeByCompletionTime(startTimestamp, endTimestamp).getInstants();
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
        for (StoragePathInfo fileStatus : affectedFilesInCommits) {
            if (!metaClient.getStorage().exists(fileStatus.getPath())) {
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
    public String getEndTs() {
        return endTimestamp;
    }

    @Override
    public List<FileSlice> collectFileSlices() {
        if (includedCommits.isEmpty()) {
            return Collections.emptyList();
        }
        IncrementalRelation.checkNotFullTableScan(fullTableScan);
        HoodieTimeline scanTimeline;
        if (hollowCommitHandling == HollowCommitHandling.USE_TRANSITION_TIME) {
            scanTimeline = metaClient.getCommitsAndCompactionTimeline()
                    .findInstantsInRangeByCompletionTime(startTimestamp, endTimestamp);
        } else {
            scanTimeline = TimelineUtils.handleHollowCommitIfNeeded(
                            metaClient.getCommitsAndCompactionTimeline(), metaClient, hollowCommitHandling)
                    .findInstantsInRange(startTimestamp, endTimestamp);
        }
        String latestCommit = includedCommits.get(includedCommits.size() - 1).requestedTime();
        HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, scanTimeline,
                affectedFilesInCommits);
        Stream<FileSlice> fileSlices = HoodieTableMetadataUtil.getWritePartitionPaths(commitsMetadata)
                .stream().flatMap(relativePartitionPath ->
                        fsView.getLatestMergedFileSlicesBeforeOrOn(relativePartitionPath, latestCommit));
        if ("".equals(globPattern)) {
            return fileSlices.collect(Collectors.toList());
        }
        GlobPattern globMatcher = new GlobPattern("*" + globPattern);
        return fileSlices.filter(fileSlice -> globMatcher.matches(fileSlice.getBaseFile().map(BaseFile::getPath)
                .or(fileSlice.getLatestLogFile().map(f -> f.getPath().toString())).get()))
                .collect(Collectors.toList());
    }

    @Override
    public List<HudiScanRange> collectSplits(UnaryOperator<String> nativePathNormalizer) {
        // MOR emits ranges via collectFileSlices()/buildMorRange, not here; the normalizer is irrelevant.
        throw new UnsupportedOperationException();
    }
}
