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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.catalog.stream.AbstractTableStreamUpdate;
import org.apache.doris.catalog.stream.OlapTableStreamUpdate;
import org.apache.doris.catalog.stream.OlapTableStreamWrapper;
import org.apache.doris.catalog.stream.TableStreamUpdateInfo;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;

import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Extract table stream consumption info from analyzed plan.
 */
public class StreamConsumptionInfoExtractor {

    /**
     * Extract table stream consumption info from a plan.
     *
     * @param analyzedPlan the plan to extract table stream consumption info from
     * @return the extracted merged table stream consumption info
     */
    public static List<TableStreamUpdateInfo> extract(Plan analyzedPlan) {
        if (analyzedPlan == null) {
            return new ArrayList<>();
        }
        Map<Pair<Long, Long>, AbstractTableStreamUpdate> distinctUpdate = Maps.newHashMap();
        analyzedPlan.collectToList(LogicalOlapTableStreamScan.class::isInstance)
                .forEach(scan -> {
                    LogicalOlapTableStreamScan streamScan = (LogicalOlapTableStreamScan) scan;
                    if (!streamScan.isSnapshot()) {
                        OlapTableStreamWrapper wrapper = streamScan.getTable();
                        OlapTableStreamUpdate update = toOlapTableStreamUpdate(wrapper);
                        if (!update.getNext().isEmpty()) {
                            // key -> (dbId, streamId)
                            Pair<Long, Long> key = Pair.of(wrapper.getStreamDbId(), wrapper.getStreamId());
                            if (!distinctUpdate.containsKey(key)) {
                                distinctUpdate.put(key, new OlapTableStreamUpdate());
                            }
                            distinctUpdate.get(key).merge(update);
                        }
                    }
                });

        List<TableStreamUpdateInfo> infos = new ArrayList<>(distinctUpdate.size());
        distinctUpdate.forEach((key, value) -> infos.add(new TableStreamUpdateInfo(key.first, key.second, value)));
        return infos;
    }

    private static OlapTableStreamUpdate toOlapTableStreamUpdate(OlapTableStreamWrapper wrapper) {
        Map<Long, Long> prev = Maps.newHashMapWithExpectedSize(wrapper.getOutputUpdateMap().size());
        Map<Long, Long> next = Maps.newHashMapWithExpectedSize(wrapper.getOutputUpdateMap().size());
        for (Map.Entry<Long, Pair<Long, Long>> entry : wrapper.getOutputUpdateMap().entrySet()) {
            Pair<Long, Long> update = entry.getValue();
            if (update.first != null) {
                if (wrapper.isHistoryPartition(entry.getKey())) {
                    // use negative value to mark history offset
                    prev.put(entry.getKey(), -update.first);
                } else {
                    prev.put(entry.getKey(), update.first);
                }
            }
            if (update.second != null) {
                next.put(entry.getKey(), update.second);
            } else {
                next.put(entry.getKey(), wrapper.getPartition(entry.getKey()).getTso());
            }
        }
        return new OlapTableStreamUpdate(prev, next);
    }
}
