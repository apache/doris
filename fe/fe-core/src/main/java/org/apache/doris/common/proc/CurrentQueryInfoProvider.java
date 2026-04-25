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

package org.apache.doris.common.proc;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.profile.Counter;
import org.apache.doris.common.profile.RuntimeProfile;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.qe.QueryStatisticsItem;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Provide running query's statistics.
 */
public class CurrentQueryInfoProvider {
    public static final String FRAGMENT_INSTANCE_ID = "FragmentInstanceId";

    public CurrentQueryInfoProvider() {
    }

    public QueryStatistics getQueryStatistics(QueryStatisticsItem item) throws AnalysisException {
        return new QueryStatistics(item.getQueryProfile());
    }

    public Map<String, QueryStatistics> getQueryStatistics(Collection<QueryStatisticsItem> items) {
        final Map<String, QueryStatistics> queryStatisticsMap = Maps.newHashMap();
        for (QueryStatisticsItem item : items) {
            queryStatisticsMap.put(item.getQueryId(), new QueryStatistics(item.getQueryProfile()));
        }
        return queryStatisticsMap;
    }

    public Collection<InstanceStatistics> getInstanceStatistics(QueryStatisticsItem item) throws AnalysisException {
        final Map<String, List<RuntimeProfile>> instanceProfiles = collectInstanceProfile(item.getQueryProfile());
        final List<InstanceStatistics> instanceStatisticsList = Lists.newArrayList();
        for (QueryStatisticsItem.FragmentInstanceInfo instanceInfo : item.getFragmentInstanceInfos()) {
            List<RuntimeProfile> instanceProfile =
                    instanceProfiles.get(DebugUtil.printId(instanceInfo.getInstanceId()));
            // Running queries may reach here before BE has reported task profiles.
            if (instanceProfile == null) {
                instanceProfile = Lists.newArrayList();
            }
            final InstanceStatistics statistics =
                    new InstanceStatistics(
                            instanceInfo.getFragmentId(),
                            instanceInfo.getInstanceId(),
                            instanceInfo.getAddress(),
                            instanceProfile);
            instanceStatisticsList.add(statistics);
        }
        return instanceStatisticsList;
    }

    private Map<String, List<RuntimeProfile>> collectInstanceProfile(RuntimeProfile queryProfile) {
        final Map<String, List<RuntimeProfile>> instanceProfiles = Maps.newHashMap();
        collectInstanceProfile(queryProfile, instanceProfiles);
        return instanceProfiles;
    }

    private void collectInstanceProfile(RuntimeProfile profile, Map<String, List<RuntimeProfile>> instanceProfiles) {
        final String instanceId = profile.getInfoString(FRAGMENT_INSTANCE_ID);
        if (!Strings.isNullOrEmpty(instanceId)) {
            List<RuntimeProfile> profiles = instanceProfiles.get(instanceId);
            if (profiles == null) {
                profiles = Lists.newArrayList();
                instanceProfiles.put(instanceId, profiles);
            }
            profiles.add(profile);
        } else {
            final String legacyInstanceId = parseLegacyInstanceId(profile.getName());
            if (legacyInstanceId != null) {
                List<RuntimeProfile> profiles = instanceProfiles.get(legacyInstanceId);
                Preconditions.checkState(profiles == null);
                profiles = Lists.newArrayList();
                profiles.add(profile);
                instanceProfiles.put(legacyInstanceId, profiles);
            }
        }

        for (RuntimeProfile child : profile.getChildMap().values()) {
            collectInstanceProfile(child, instanceProfiles);
        }
    }

    private String parseLegacyInstanceId(String str) {
        if (!str.startsWith("Instance ")) {
            return null;
        }
        final String[] elements = str.split("\\s+");
        if (elements.length < 2) {
            return null;
        }
        try {
            DebugUtil.parseTUniqueIdFromString(elements[1]);
        } catch (NumberFormatException e) {
            return null;
        }
        return elements[1];
    }

    public static class QueryStatistics {
        final List<Map<String, Counter>> counterMaps;

        public QueryStatistics(RuntimeProfile profile) {
            counterMaps = Lists.newArrayList();
            collectCounters(profile, counterMaps);
        }

        public QueryStatistics(Collection<RuntimeProfile> profiles) {
            counterMaps = Lists.newArrayList();
            for (RuntimeProfile profile : profiles) {
                collectCounters(profile, counterMaps);
            }
        }

        private void collectCounters(RuntimeProfile profile, List<Map<String, Counter>> counterMaps) {
            for (Map.Entry<String, RuntimeProfile> entry : profile.getChildMap().entrySet()) {
                counterMaps.add(entry.getValue().getCounterMap());
                collectCounters(entry.getValue(), counterMaps);
            }
        }

        public long getScanBytes() {
            long scanBytes = 0;
            for (Map<String, Counter> counters : counterMaps) {
                final Counter counter = counters.get("CompressedBytesRead");
                scanBytes += counter == null ? 0 : counter.getValue();
            }
            return scanBytes;
        }

        public long getRowsReturned() {
            long rowsReturned = 0;
            for (Map<String, Counter> counters : counterMaps) {
                final Counter counter = counters.get("RowsReturned");
                rowsReturned += counter == null ? 0 : counter.getValue();
            }
            return rowsReturned;
        }
    }

    public static class InstanceStatistics {
        private final String fragmentId;
        private final TUniqueId instanceId;
        private final TNetworkAddress address;
        private final QueryStatistics statistics;

        public InstanceStatistics(
                String fragmentId,
                TUniqueId instanceId,
                TNetworkAddress address,
                Collection<RuntimeProfile> profile) {
            this.fragmentId = fragmentId;
            this.instanceId = instanceId;
            this.address = address;
            this.statistics = new QueryStatistics(profile);
        }

        public String getFragmentId() {
            return fragmentId;
        }

        public TUniqueId getInstanceId() {
            return instanceId;
        }

        public TNetworkAddress getAddress() {
            return address;
        }

        public long getRowsReturned() {
            return statistics.getRowsReturned();
        }

        public long getScanBytes() {
            return statistics.getScanBytes();
        }
    }
}
