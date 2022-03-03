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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/*
 * show proc "/cluster_health";
 */

public class ClusterHealthProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("DbId").add("DbName").add("TabletNum").add("HealthyNum").add("ReplicaMissingNum")
            .add("VersionIncompleteNum").add("ReplicaRelocatingNum").add("RedundantNum")
            .add("ReplicaMissingInClusterNum").add("ReplicaMissingForTagNum")
            .add("ForceRedundantNum").add("ColocateMismatchNum").add("ColocateRedundantNum")
            .add("NeedFurtherRepairNum").add("UnrecoverableNum").add("ReplicaCompactionTooSlowNum")
            .build();

    private Catalog catalog;

    public ClusterHealthProcDir(Catalog catalog) {
        Preconditions.checkNotNull(catalog);
        this.catalog = catalog;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String dbIdStr) throws AnalysisException {
        try {
            long dbId = Long.parseLong(dbIdStr);
            return catalog.getDb(dbId).map(IncompleteTabletsProcNode::new).orElse(null);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid db id format: " + dbIdStr);
        }
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        List<StatisticProcDir.DBStatistic> statistics = catalog.getDbIds().parallelStream()
                // skip information_schema database
                .flatMap(id -> Stream.of(id == 0 ? null : catalog.getDbNullable(id)))
                .filter(Objects::nonNull).map(StatisticProcDir.DBStatistic::new)
                // sort by dbName
                .sorted(Comparator.comparing(db -> db.db.getFullName()))
                .collect(Collectors.toList());

        List<List<String>> rows = new ArrayList<>(statistics.size() + 1);
        for (StatisticProcDir.DBStatistic statistic : statistics) {
            rows.add(statistic.toRow());
        }
        rows.add(statistics.stream().reduce(new StatisticProcDir.DBStatistic(), StatisticProcDir.DBStatistic::reduce).toRow());

        return new BaseProcResult(TITLE_NAMES, rows);
    }
}
