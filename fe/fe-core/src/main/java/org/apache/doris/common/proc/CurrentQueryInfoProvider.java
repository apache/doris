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
import org.apache.doris.common.util.Counter;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.qe.QueryStatisticsItem;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Provide running query's statistics.
 */
public class CurrentQueryInfoProvider {
    private static final Logger LOG = LogManager.getLogger(CurrentQueryInfoProvider.class);

    public CurrentQueryInfoProvider() {
    }

    /**
     * get Counters from Coordinator's RuntimeProfile and return query's statistics.
     *
     * @param item
     * @return
     * @throws AnalysisException
     */
    public QueryStatistics getQueryStatistics(QueryStatisticsItem item) throws AnalysisException {
        return new QueryStatistics(item.getQueryProfile());
    }

    /**
     *
     * @param items
     * @return
     * @throws AnalysisException
     */
    public Map<String, QueryStatistics> getQueryStatistics(Collection<QueryStatisticsItem> items) {
        final Map<String, QueryStatistics> queryStatisticsMap = Maps.newHashMap();
        for (QueryStatisticsItem item : items) {
            queryStatisticsMap.put(item.getQueryId(), new QueryStatistics(item.getQueryProfile()));
        }
        return queryStatisticsMap;
    }

    /**
     * Return query's instances statistics.
     *
     * @param item
     * @return
     * @throws AnalysisException
     */
    public Collection<InstanceStatistics> getInstanceStatistics(QueryStatisticsItem item) throws AnalysisException {
        final Map<String, RuntimeProfile> instanceProfiles = collectInstanceProfile(item.getQueryProfile());
        final List<InstanceStatistics> instanceStatisticsList = Lists.newArrayList();
        for (QueryStatisticsItem.FragmentInstanceInfo instanceInfo : item.getFragmentInstanceInfos()) {
            final RuntimeProfile instanceProfile = instanceProfiles.get(DebugUtil.printId(instanceInfo.getInstanceId()));
            Preconditions.checkNotNull(instanceProfile);
            final InstanceStatistics Statistics =
                    new InstanceStatistics(
                            instanceInfo.getFragmentId(),
                            instanceInfo.getInstanceId(),
                            instanceInfo.getAddress(),
                            instanceProfile);
            instanceStatisticsList.add(Statistics);
        }
        return instanceStatisticsList;
    }

    /**
     * Profile trees is query profile -> fragment profile -> instance profile ....
     * @param queryProfile
     * @return instanceProfiles
     */
    private Map<String, RuntimeProfile> collectInstanceProfile(RuntimeProfile queryProfile) {
        final Map<String, RuntimeProfile> instanceProfiles = Maps.newHashMap();
        for (RuntimeProfile fragmentProfile : queryProfile.getChildMap().values()) {
            for (Map.Entry<String, RuntimeProfile> entry: fragmentProfile.getChildMap().entrySet()) {
                Preconditions.checkState(instanceProfiles.put(parseInstanceId(entry.getKey()), entry.getValue()) == null);
            }
        }
        return instanceProfiles;
    }

    /**
     * Instance profile key is "Instance ${instance_id} (host=$host $port)"
     * @param str
     * @return
     */
    private String parseInstanceId(String str) {
        final String[] elements = str.split(" ");
        if (elements.length == 4) {
            return elements[1];
        } else {
            Preconditions.checkState(false);
            return "";
        }
    }

    public static class QueryStatistics {
        final List<Map<String, Counter>> counterMaps;

        public QueryStatistics(RuntimeProfile profile) {
            counterMaps = Lists.newArrayList();
            collectCounters(profile, counterMaps);
        }

        private void collectCounters(RuntimeProfile profile,
                                                List<Map<String, Counter>> counterMaps) {
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
                RuntimeProfile profile) {
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
