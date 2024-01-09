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

package org.apache.doris.planner.external;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.ConsistentHash;
import org.apache.doris.mysql.privilege.UserProperty;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.Funnel;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class FederationBackendPolicy {
    private static final Logger LOG = LogManager.getLogger(FederationBackendPolicy.class);
    private final List<Backend> backends = Lists.newArrayList();
    private final Map<String, List<Backend>> backendMap = Maps.newHashMap();
    private final SecureRandom random = new SecureRandom();
    private ConsistentHash<TScanRangeLocations, Backend> consistentHash;

    private int nextBe = 0;
    private boolean initialized = false;

    // Create a ConsistentHash ring may be a time-consuming operation, so we cache it.
    private static LoadingCache<HashCacheKey, ConsistentHash<TScanRangeLocations, Backend>> consistentHashCache;

    static {
        consistentHashCache = CacheBuilder.newBuilder().maximumSize(5)
                .build(new CacheLoader<HashCacheKey, ConsistentHash<TScanRangeLocations, Backend>>() {
                    @Override
                    public ConsistentHash<TScanRangeLocations, Backend> load(HashCacheKey key) {
                        return new ConsistentHash<>(Hashing.murmur3_128(), new ScanRangeHash(),
                                new BackendHash(), key.bes, Config.virtual_node_number);
                    }
                });
    }

    private static class HashCacheKey {
        // sorted backend ids as key
        private List<Long> beIds;
        // backends is not part of key, just an attachment
        private List<Backend> bes;

        HashCacheKey(List<Backend> backends) {
            this.bes = backends;
            this.beIds = backends.stream().map(b -> b.getId()).sorted().collect(Collectors.toList());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof HashCacheKey)) {
                return false;
            }
            return Objects.equals(beIds, ((HashCacheKey) obj).beIds);
        }

        @Override
        public int hashCode() {
            return Objects.hash(beIds);
        }

        @Override
        public String toString() {
            return "HashCache{" + "beIds=" + beIds + '}';
        }
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
            LOG.debug("user info in ExternalFileScanNode should not be null, add log to observer");
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
        backends.addAll(policy.getCandidateBackends(Env.getCurrentSystemInfo().getIdToBackend().values()));
        if (backends.isEmpty()) {
            throw new UserException("No available backends");
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

    public Backend getNextConsistentBe(TScanRangeLocations scanRangeLocations) {
        return consistentHash.getNode(scanRangeLocations);
    }

    // Try to find a local BE, if not exists, use `getNextBe` instead
    public Backend getNextLocalBe(List<String> hosts, TScanRangeLocations scanRangeLocations) {
        List<Backend> candidateBackends = Lists.newArrayListWithCapacity(hosts.size());
        for (String host : hosts) {
            List<Backend> backends = backendMap.get(host);
            if (CollectionUtils.isNotEmpty(backends)) {
                candidateBackends.add(backends.get(random.nextInt(backends.size())));
            }
        }

        return CollectionUtils.isEmpty(candidateBackends)
                    ? getNextConsistentBe(scanRangeLocations)
                    : candidateBackends.get(random.nextInt(candidateBackends.size()));
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

    private static class ScanRangeHash implements Funnel<TScanRangeLocations> {
        @Override
        public void funnel(TScanRangeLocations scanRange, PrimitiveSink primitiveSink) {
            Preconditions.checkState(scanRange.scan_range.isSetExtScanRange());
            for (TFileRangeDesc desc : scanRange.scan_range.ext_scan_range.file_scan_range.ranges) {
                primitiveSink.putBytes(desc.path.getBytes(StandardCharsets.UTF_8));
                primitiveSink.putLong(desc.start_offset);
                primitiveSink.putLong(desc.size);
            }
        }
    }
}
