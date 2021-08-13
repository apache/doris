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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class IncompleteTabletsProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("UnhealthyTablets").add("InconsistentTablets").add("CloningTablets").add("BadTablets")
            .build();
    private static final Joiner JOINER = Joiner.on(",");

    Collection<Long> unhealthyTabletIds;
    Collection<Long> inconsistentTabletIds;
    Collection<Long> cloningTabletIds;
    Collection<Long> unrecoverableTabletIds;

    public IncompleteTabletsProcNode(Collection<Long> unhealthyTabletIds,
                                     Collection<Long> inconsistentTabletIds,
                                     Collection<Long> cloningTabletIds,
                                     Collection<Long> unrecoverableTabletIds) {
        this.unhealthyTabletIds = unhealthyTabletIds;
        this.inconsistentTabletIds = inconsistentTabletIds;
        this.cloningTabletIds = cloningTabletIds;
        this.unrecoverableTabletIds = unrecoverableTabletIds;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();

        result.setNames(TITLE_NAMES);

        List<String> row = new ArrayList<String>(1);

        String incompleteTablets = JOINER.join(Arrays.asList(unhealthyTabletIds));
        String inconsistentTablets = JOINER.join(Arrays.asList(inconsistentTabletIds));
        String cloningTablets = JOINER.join(Arrays.asList(cloningTabletIds));
        String unrecoverableTablets = JOINER.join(Arrays.asList(unrecoverableTabletIds));
        row.add(incompleteTablets);
        row.add(inconsistentTablets);
        row.add(cloningTablets);
        row.add(unrecoverableTablets);

        result.addRow(row);

        return result;
    }

}
