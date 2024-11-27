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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.nereids.trees.plans.algebra.OlapScan;
import org.apache.doris.nereids.trees.plans.algebra.TopN;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.SortNode;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * topN runtime filter context
 */
public class TopnFilterContext {
    private final Map<TopN, List<OlapScan>> filters = Maps.newHashMap();
    private final Set<TopN> sources = Sets.newHashSet();
    private final Set<OlapScan> targets = Sets.newHashSet();
    private final Map<OlapScan, OlapScanNode> legacyTargetsMap = Maps.newHashMap();
    private final Map<TopN, SortNode> legacySourceMap = Maps.newHashMap();

    /**
     * add topN filter
     */
    public void addTopnFilter(TopN topn, OlapScan scan) {
        targets.add(scan);
        sources.add(topn);

        List<OlapScan> targets = filters.get(topn);
        if (targets == null) {
            filters.put(topn, Lists.newArrayList(scan));
        } else {
            targets.add(scan);
        }
    }

    /**
     * find the corresponding sortNode for topn filter
     */
    public Optional<OlapScanNode> getLegacyScanNode(OlapScan scan) {
        return legacyTargetsMap.keySet().contains(scan)
                ? Optional.of(legacyTargetsMap.get(scan))
                : Optional.empty();
    }

    public Optional<SortNode> getLegacySortNode(TopN topn) {
        return legacyTargetsMap.keySet().contains(topn)
                ? Optional.of(legacySourceMap.get(topn))
                : Optional.empty();
    }

    public boolean isTopnFilterSource(TopN topn) {
        return sources.contains(topn);
    }

    public boolean isTopnFilterTarget(OlapScan scan) {
        return targets.contains(scan);
    }

    public void addLegacySource(TopN topn, SortNode sort) {
        legacySourceMap.put(topn, sort);
    }

    public void addLegacyTarget(OlapScan olapScan, OlapScanNode legacy) {
        legacyTargetsMap.put(olapScan, legacy);
    }

    public List<OlapScan> getTargets(TopN topn) {
        return filters.get(topn);
    }
}
