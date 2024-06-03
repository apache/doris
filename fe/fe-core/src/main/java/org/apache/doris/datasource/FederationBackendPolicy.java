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
// This file is referenced from
// https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/execution/scheduler/UniformNodeSelector.java
// and modified by Doris

package org.apache.doris.datasource;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.IndexedPriorityQueue;
import org.apache.doris.common.ResettableRandomizedIterator;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.ConsistentHash;
import org.apache.doris.mysql.privilege.UserProperty;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.system.SystemInfoService;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.hash.Funnel;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class FederationBackendPolicy {
    private static final Logger LOG = LogManager.getLogger(FederationBackendPolicy.class);
    protected final List<Backend> backends = Lists.newArrayList();
    private final Map<String, List<Backend>> backendMap = Maps.newHashMap();

    public Map<Backend, Long> getAssignedWeightPerBackend() {
        return assignedWeightPerBackend;
    }

    private Map<Backend, Long> assignedWeightPerBackend = Maps.newHashMap();

    protected ConsistentHash<Split, Backend> consistentHash;

    private int nextBe = 0;
    private boolean initialized = false;

    private NodeSelectionStrategy nodeSelectionStrategy;
    private boolean enableSplitsRedistribution = true;

    // Create a ConsistentHash ring may be a time-consuming operation, so we cache it.
    private static LoadingCache<HashCacheKey, ConsistentHash<Split, Backend>> consistentHashCache;

    static {
        consistentHashCache = CacheBuilder.newBuilder().maximumSize(5)
                .build(new CacheLoader<HashCacheKey, ConsistentHash<Split, Backend>>() {
                    @Override
                    public ConsistentHash<Split, Backend> load(HashCacheKey key) {
                        return new ConsistentHash<>(Hashing.murmur3_128(), new SplitHash(),
                                new BackendHash(), key.bes, Config.split_assigner_virtual_node_number);
                    }
                });
    }

    private static class HashCacheKey {
        // sorted backend ids as key
        private List<String> beHashKeys;
        // backends is not part of key, just an attachment
        private List<Backend> bes;

        HashCacheKey(List<Backend> backends) {
            this.bes = backends;
            this.beHashKeys = backends.stream().map(b ->
                            String.format("id: %d, host: %s, port: %d", b.getId(), b.getHost(), b.getHeartbeatPort()))
                    .sorted()
                    .collect(Collectors.toList());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof HashCacheKey)) {
                return false;
            }
            return Objects.equals(beHashKeys, ((HashCacheKey) obj).beHashKeys);
        }

        @Override
        public int hashCode() {
            return Objects.hash(beHashKeys);
        }

        @Override
        public String toString() {
            return "HashCache{" + "beHashKeys=" + beHashKeys + '}';
        }
    }

    public FederationBackendPolicy(NodeSelectionStrategy nodeSelectionStrategy) {
        this.nodeSelectionStrategy = nodeSelectionStrategy;
    }

    public FederationBackendPolicy() {
        this(NodeSelectionStrategy.ROUND_ROBIN);
    }

    public void init() throws UserException {
        if (!initialized) {
            init(Collections.emptyList());
            initialized = true;
        }
    }

    public void init(List<String> preLocations) throws UserException {
        Set<Tag> tags = Sets.newHashSet();
        if (ConnectContext.get() != null && ConnectContext.get().getCurrentUserIdentity() != null) {
            String qualifiedUser = ConnectContext.get().getCurrentUserIdentity().getQualifiedUser();
            // Some request from stream load(eg, mysql load) may not set user info in ConnectContext
            // just ignore it.
            if (!Strings.isNullOrEmpty(qualifiedUser)) {
                tags = Env.getCurrentEnv().getAuth().getResourceTags(qualifiedUser);
                if (tags == UserProperty.INVALID_RESOURCE_TAGS) {
                    throw new UserException("No valid resource tag for user: " + qualifiedUser);
                }
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("user info in ExternalFileScanNode should not be null, add log to observer");
            }
        }

        // scan node is used for query
        BeSelectionPolicy policy = new BeSelectionPolicy.Builder()
                .needQueryAvailable()
                .needLoadAvailable()
                .addTags(tags)
                .preferComputeNode(Config.prefer_compute_node_for_external_table)
                .assignExpectBeNum(Config.min_backend_num_for_external_table)
                .addPreLocations(preLocations)
                .build();
        init(policy);
    }

    public void init(BeSelectionPolicy policy) throws UserException {
        backends.addAll(policy.getCandidateBackends(Env.getCurrentSystemInfo().getBackendsByCurrentCluster()));
        if (backends.isEmpty()) {
            throw new UserException("No available backends");
        }
        for (Backend backend : backends) {
            assignedWeightPerBackend.put(backend, 0L);
        }

        backendMap.putAll(backends.stream().collect(Collectors.groupingBy(Backend::getHost)));
        try {
            consistentHash = consistentHashCache.get(new HashCacheKey(backends));
        } catch (ExecutionException e) {
            throw new UserException("failed to get consistent hash", e);
        }
    }

    public Backend getNextBe() {
        Backend selectedBackend = backends.get(nextBe++);
        nextBe = nextBe % backends.size();
        return selectedBackend;
    }

    @VisibleForTesting
    public void setEnableSplitsRedistribution(boolean enableSplitsRedistribution) {
        this.enableSplitsRedistribution = enableSplitsRedistribution;
    }

    /**
     * Assign splits to each backend. Ensure that each backend receives a similar amount of data.
     * In order to make sure backends utilize the os page cache as much as possible, and all backends read splits
     * in the order of partitions(reading data in partition order can reduce the memory usage of backends),
     * splits should be sorted by path.
     * Fortunately, the process of obtaining splits ensures that the splits have been sorted according to the path.
     * If the splits are unordered, it is strongly recommended to sort them before calling this function.
     */
    public Multimap<Backend, Split> computeScanRangeAssignment(List<Split> splits) throws UserException {
        ListMultimap<Backend, Split> assignment = ArrayListMultimap.create();

        List<Split> remainingSplits;

        List<Backend> backends = new ArrayList<>();
        for (List<Backend> backendList : backendMap.values()) {
            backends.addAll(backendList);
        }
        ResettableRandomizedIterator<Backend> randomCandidates = new ResettableRandomizedIterator<>(backends);

        boolean splitsToBeRedistributed = false;

        // optimizedLocalScheduling enables prioritized assignment of splits to local nodes when splits contain
        // locality information
        if (Config.split_assigner_optimized_local_scheduling) {
            remainingSplits = new ArrayList<>(splits.size());
            for (Split split : splits) {
                if (split.isRemotelyAccessible() && (split.getHosts() != null && split.getHosts().length > 0)) {
                    List<Backend> candidateNodes = selectExactNodes(backendMap, split.getHosts());

                    Optional<Backend> chosenNode = candidateNodes.stream()
                            .min(Comparator.comparingLong(ownerNode -> assignedWeightPerBackend.get(ownerNode)));

                    if (chosenNode.isPresent()) {
                        Backend selectedBackend = chosenNode.get();
                        assignment.put(selectedBackend, split);
                        assignedWeightPerBackend.put(selectedBackend,
                                assignedWeightPerBackend.get(selectedBackend) + split.getSplitWeight().getRawValue());
                        splitsToBeRedistributed = true;
                        continue;
                    }
                }
                remainingSplits.add(split);
            }
        } else {
            remainingSplits = splits;
        }

        for (Split split : remainingSplits) {
            List<Backend> candidateNodes;
            if (!split.isRemotelyAccessible()) {
                candidateNodes = selectExactNodes(backendMap, split.getHosts());
            } else {
                switch (nodeSelectionStrategy) {
                    case ROUND_ROBIN: {
                        Backend selectedBackend = backends.get(nextBe++);
                        nextBe = nextBe % backends.size();
                        candidateNodes = ImmutableList.of(selectedBackend);
                        break;
                    }
                    case RANDOM: {
                        randomCandidates.reset();
                        candidateNodes = selectNodes(Config.split_assigner_min_random_candidate_num, randomCandidates);
                        break;
                    }
                    case CONSISTENT_HASHING: {
                        candidateNodes = consistentHash.getNode(split,
                                Config.split_assigner_min_consistent_hash_candidate_num);
                        splitsToBeRedistributed = true;
                        break;
                    }
                    default: {
                        throw new RuntimeException();
                    }
                }
            }
            if (candidateNodes.isEmpty()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("No nodes available to schedule {}. Available nodes {}", split,
                            backends);
                }
                throw new UserException(SystemInfoService.NO_SCAN_NODE_BACKEND_AVAILABLE_MSG);
            }

            Backend selectedBackend = chooseNodeForSplit(candidateNodes);
            List<Backend> alternativeBackends = new ArrayList<>(candidateNodes);
            alternativeBackends.remove(selectedBackend);
            split.setAlternativeHosts(
                    alternativeBackends.stream().map(each -> each.getHost()).collect(Collectors.toList()));
            assignment.put(selectedBackend, split);
            assignedWeightPerBackend.put(selectedBackend,
                    assignedWeightPerBackend.get(selectedBackend) + split.getSplitWeight().getRawValue());
        }

        if (enableSplitsRedistribution && splitsToBeRedistributed) {
            equateDistribution(assignment);
        }
        return assignment;
    }

    /**
     * The method tries to make the distribution of splits more uniform. All nodes are arranged into a maxHeap and
     * a minHeap based on the number of splits that are assigned to them. Splits are redistributed, one at a time,
     * from a maxNode to a minNode until we have as uniform a distribution as possible.
     *
     * @param assignment the node-splits multimap after the first and the second stage
     */
    private void equateDistribution(ListMultimap<Backend, Split> assignment) {
        if (assignment.isEmpty()) {
            return;
        }

        List<Backend> allNodes = new ArrayList<>();
        for (List<Backend> backendList : backendMap.values()) {
            allNodes.addAll(backendList);
        }
        Collections.sort(allNodes, Comparator.comparing(Backend::getId));

        if (allNodes.size() < 2) {
            return;
        }

        IndexedPriorityQueue<Backend> maxNodes = new IndexedPriorityQueue<>();
        for (Backend node : allNodes) {
            maxNodes.addOrUpdate(node, assignedWeightPerBackend.get(node));
        }

        IndexedPriorityQueue<Backend> minNodes = new IndexedPriorityQueue<>();
        for (Backend node : allNodes) {
            minNodes.addOrUpdate(node, Long.MAX_VALUE - assignedWeightPerBackend.get(node));
        }

        while (true) {
            if (maxNodes.isEmpty()) {
                return;
            }

            // fetch min and max node
            Backend maxNode = maxNodes.poll();
            Backend minNode = minNodes.poll();

            // Allow some degree of non uniformity when assigning splits to nodes. Usually data distribution
            // among nodes in a cluster won't be fully uniform (e.g. because hash function with non-uniform
            // distribution is used like consistent hashing). In such case it makes sense to assign splits to nodes
            // with data because of potential savings in network throughput and CPU time.
            if (assignedWeightPerBackend.get(maxNode) - assignedWeightPerBackend.get(minNode)
                    <= SplitWeight.rawValueForStandardSplitCount(Config.split_assigner_max_split_num_variance)) {
                return;
            }

            // move split from max to min
            Split redistributedSplit = redistributeSplit(assignment, maxNode, minNode);

            assignedWeightPerBackend.put(maxNode,
                    assignedWeightPerBackend.get(maxNode) - redistributedSplit.getSplitWeight().getRawValue());
            assignedWeightPerBackend.put(minNode, Math.addExact(
                    assignedWeightPerBackend.get(minNode), redistributedSplit.getSplitWeight().getRawValue()));

            // add max back into maxNodes only if it still has assignments
            if (assignment.containsKey(maxNode)) {
                maxNodes.addOrUpdate(maxNode, assignedWeightPerBackend.get(maxNode));
            }

            // Add or update both the Priority Queues with the updated node priorities
            maxNodes.addOrUpdate(minNode, assignedWeightPerBackend.get(minNode));
            minNodes.addOrUpdate(minNode, Long.MAX_VALUE - assignedWeightPerBackend.get(minNode));
            minNodes.addOrUpdate(maxNode, Long.MAX_VALUE - assignedWeightPerBackend.get(maxNode));
        }
    }

    /**
     * The method selects and removes a split from the fromNode and assigns it to the toNode. There is an attempt to
     * redistribute a Non-local split if possible. This case is possible when there are multiple queries running
     * simultaneously. If a Non-local split cannot be found in the maxNode, next split is selected and reassigned.
     */
    @VisibleForTesting
    public static Split redistributeSplit(Multimap<Backend, Split> assignment, Backend fromNode,
            Backend toNode) {
        Iterator<Split> splitIterator = assignment.get(fromNode).iterator();
        Split splitToBeRedistributed = null;
        while (splitIterator.hasNext()) {
            Split split = splitIterator.next();
            // Try to select non-local split for redistribution
            if (split.getHosts() != null && !isSplitLocal(
                    split.getHosts(), fromNode.getHost())) {
                splitToBeRedistributed = split;
                break;
            }
        }
        // Select split if maxNode has no non-local splits in the current batch of assignment
        if (splitToBeRedistributed == null) {
            splitIterator = assignment.get(fromNode).iterator();
            while (splitIterator.hasNext()) {
                splitToBeRedistributed = splitIterator.next();
                // if toNode has split replication, transfer this split firstly
                if (splitToBeRedistributed.getHosts() != null && isSplitLocal(
                        splitToBeRedistributed.getHosts(), toNode.getHost())) {
                    break;
                }
                // if toNode is split alternative host, transfer this split firstly
                if (splitToBeRedistributed.getAlternativeHosts() != null && isSplitLocal(
                        splitToBeRedistributed.getAlternativeHosts(), toNode.getHost())) {
                    break;
                }
            }
        }
        splitIterator.remove();
        assignment.put(toNode, splitToBeRedistributed);
        return splitToBeRedistributed;
    }

    private static boolean isSplitLocal(String[] splitHosts, String host) {
        for (String splitHost : splitHosts) {
            if (splitHost.equals(host)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isSplitLocal(List<String> splitHosts, String host) {
        for (String splitHost : splitHosts) {
            if (splitHost.equals(host)) {
                return true;
            }
        }
        return false;
    }

    public static List<Backend> selectExactNodes(Map<String, List<Backend>> backendMap, String[] hosts) {
        Set<Backend> chosen = new LinkedHashSet<>();

        for (String host : hosts) {
            if (backendMap.containsKey(host)) {
                backendMap.get(host).stream()
                        .forEach(chosen::add);
            }
        }
        return ImmutableList.copyOf(chosen);
    }

    public static List<Backend> selectNodes(int limit, Iterator<Backend> candidates) {
        Preconditions.checkArgument(limit > 0, "limit must be at least 1");

        List<Backend> selected = new ArrayList<>(limit);
        while (selected.size() < limit && candidates.hasNext()) {
            selected.add(candidates.next());
        }

        return selected;
    }

    private Backend chooseNodeForSplit(List<Backend> candidateNodes) {
        Backend chosenNode = null;
        long minWeight = Long.MAX_VALUE;

        for (Backend node : candidateNodes) {
            long queuedWeight = assignedWeightPerBackend.get(node);
            if (queuedWeight <= minWeight) {
                chosenNode = node;
                minWeight = queuedWeight;
            }
        }

        return chosenNode;
    }

    public int numBackends() {
        return backends.size();
    }

    public Collection<Backend> getBackends() {
        return CollectionUtils.unmodifiableCollection(backends);
    }

    private static class BackendHash implements Funnel<Backend> {
        @Override
        public void funnel(Backend backend, PrimitiveSink primitiveSink) {
            primitiveSink.putLong(backend.getId());
        }
    }

    private static class SplitHash implements Funnel<Split> {
        @Override
        public void funnel(Split split, PrimitiveSink primitiveSink) {
            primitiveSink.putBytes(split.getPathString().getBytes(StandardCharsets.UTF_8));
            primitiveSink.putLong(split.getStart());
            primitiveSink.putLong(split.getLength());
        }
    }
}
