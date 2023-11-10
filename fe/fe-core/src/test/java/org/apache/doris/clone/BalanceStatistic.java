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

package org.apache.doris.clone;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Replica;
import org.apache.doris.system.Backend;

import com.google.common.collect.Maps;
import com.google.common.collect.Table;

import java.util.List;
import java.util.Map;

public class BalanceStatistic {
    public Map<Long, Long> backendTotalDataSize;
    public Map<Long, Integer> backendTotalReplicaNum;

    private BalanceStatistic(Map<Long, Long> backendTotalDataSize,
            Map<Long, Integer> backendTotalReplicaNum) {
        this.backendTotalDataSize = backendTotalDataSize;
        this.backendTotalReplicaNum = backendTotalReplicaNum;
    }

    public static BalanceStatistic getCurrentBalanceStatistic() {
        Map<Long, Long> backendTotalDataSize = Maps.newHashMap();
        Map<Long, Integer> backendTotalReplicaNum = Maps.newHashMap();
        List<Backend> backends = Env.getCurrentSystemInfo().getIdToBackend().values().asList();
        backends.forEach(be -> {
            backendTotalDataSize.put(be.getId(), 0L);
            backendTotalReplicaNum.put(be.getId(), 0);
        });

        Table<Long, Long, Replica> replicaMetaTable =
                Env.getCurrentInvertedIndex().getReplicaMetaTable();
        for (Table.Cell<Long, Long, Replica> cell : replicaMetaTable.cellSet()) {
            long beId = cell.getColumnKey();
            Replica replica = cell.getValue();
            backendTotalDataSize.put(beId, backendTotalDataSize.get(beId) + replica.getDataSize());
            backendTotalReplicaNum.put(beId, backendTotalReplicaNum.get(beId) + 1);
        }

        return new BalanceStatistic(backendTotalDataSize, backendTotalReplicaNum);
    }

    public Map<Long, Long> getBackendTotalDataSize() {
        return backendTotalDataSize;
    }

    public Map<Long, Integer> getBackendTotalReplicaNum() {
        return backendTotalReplicaNum;
    }

    public long getBeMinTotalDataSize() {
        return backendTotalDataSize.values().stream().min(Long::compare).get();
    }

    public long getBeMaxTotalDataSize() {
        return backendTotalDataSize.values().stream().max(Long::compare).get();
    }

    public int getBeMinTotalReplicaNum() {
        return backendTotalReplicaNum.values().stream().min(Integer::compare).get();
    }

    public int getBeMaxTotalReplicaNum() {
        return backendTotalReplicaNum.values().stream().max(Integer::compare).get();
    }

    public void printToStdout() {
        int minTotalReplicaNum = getBeMinTotalReplicaNum();
        int maxTotalReplicaNum = getBeMaxTotalReplicaNum();
        long minTotalDataSize = getBeMinTotalDataSize();
        long maxTotalDataSize = getBeMaxTotalDataSize();

        System.out.println("");
        System.out.println("=== backend min total replica num: " + minTotalReplicaNum);
        System.out.println("=== backend max total replica num: " + maxTotalReplicaNum);
        System.out.println("=== max / min : " + (maxTotalReplicaNum / (double) minTotalReplicaNum));

        System.out.println("");
        System.out.println("=== min total data size: " + minTotalDataSize);
        System.out.println("=== max total data size: " + maxTotalDataSize);
        System.out.println("=== max / min : " + (maxTotalDataSize / (double) minTotalDataSize));
    }
}

