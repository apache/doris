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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.Collections;

public class IncompleteTabletsProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("ReplicaMissingTablets").add("VersionIncompleteTablets").add("ReplicaRelocatingTablets")
            .add("RedundantTablets").add("ReplicaMissingForTagTablets")
            .add("ForceRedundantTablets").add("ColocateMismatchTablets").add("ColocateRedundantTablets")
            .add("NeedFurtherRepairTablets").add("UnrecoverableTablets").add("ReplicaCompactionTooSlowTablets")
            .add("InconsistentTablets").add("OversizeTablets")
            .build();
    private static final Joiner JOINER = Joiner.on(",");

    final DatabaseIf<TableIf> db;

    public IncompleteTabletsProcNode(DatabaseIf db) {
        this.db = db;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        TabletHealthProcDir.DBTabletStatistic statistic = new TabletHealthProcDir.DBTabletStatistic(db);
        return new BaseProcResult(TITLE_NAMES, Collections.singletonList(Arrays.asList(
                JOINER.join(statistic.replicaMissingTabletIds),
                JOINER.join(statistic.versionIncompleteTabletIds),
                JOINER.join(statistic.replicaRelocatingTabletIds),
                JOINER.join(statistic.redundantTabletIds),
                JOINER.join(statistic.replicaMissingForTagTabletIds),
                JOINER.join(statistic.forceRedundantTabletIds),
                JOINER.join(statistic.colocateMismatchTabletIds),
                JOINER.join(statistic.colocateRedundantTabletIds),
                JOINER.join(statistic.needFurtherRepairTabletIds),
                JOINER.join(statistic.unrecoverableTabletIds),
                JOINER.join(statistic.replicaCompactionTooSlowTabletIds),
                JOINER.join(statistic.inconsistentTabletIds),
                JOINER.join(statistic.oversizeTabletIds)
        )));
    }

}
