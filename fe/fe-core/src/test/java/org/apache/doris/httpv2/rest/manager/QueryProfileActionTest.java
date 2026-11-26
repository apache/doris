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

package org.apache.doris.httpv2.rest.manager;

import org.apache.doris.common.proc.CurrentQueryStatisticsProcDir;
import org.apache.doris.common.util.QueryStatisticsFormatter;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

class QueryProfileActionTest {
    @Test
    void normalizesCurrentQueryRowsFromOldAndNewFrontends() {
        List<String> currentTitles = Lists.newArrayList(CurrentQueryStatisticsProcDir.TITLE_NAMES);
        currentTitles.add(0, "Frontend");
        List<String> currentRow = Lists.newArrayList(currentTitles);
        NodeAction.NodeInfo current = new NodeAction.NodeInfo(currentTitles, List.of(currentRow));
        Assertions.assertEquals(List.of(currentRow), QueryProfileAction.normalizeCurrentQueryRows(current));

        List<String> oldTitles = currentTitles.subList(0, currentTitles.size() - 2);
        List<String> oldRow = Lists.newArrayList(oldTitles);
        NodeAction.NodeInfo old = new NodeAction.NodeInfo(oldTitles, List.of(oldRow));
        List<String> normalizedOld = QueryProfileAction.normalizeCurrentQueryRows(old).get(0);
        Assertions.assertEquals(currentTitles.size(), normalizedOld.size());
        Assertions.assertEquals(oldRow, normalizedOld.subList(0, oldRow.size()));
        Assertions.assertEquals(QueryStatisticsFormatter.getScanBytes(0),
                normalizedOld.get(currentTitles.size() - 2));
        Assertions.assertEquals(QueryStatisticsFormatter.getScanBytes(0),
                normalizedOld.get(currentTitles.size() - 1));

        List<String> reversedTitles = Lists.newArrayList(currentTitles);
        Collections.reverse(reversedTitles);
        NodeAction.NodeInfo reversed = new NodeAction.NodeInfo(reversedTitles, List.of(reversedTitles));
        Assertions.assertEquals(currentRow, QueryProfileAction.normalizeCurrentQueryRows(reversed).get(0));
    }

    @Test
    void rejectsMalformedCurrentQueryRows() {
        List<String> titles = Lists.newArrayList(CurrentQueryStatisticsProcDir.TITLE_NAMES);
        titles.add(0, "Frontend");
        NodeAction.NodeInfo malformed = new NodeAction.NodeInfo(titles, List.of(List.of("too short")));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> QueryProfileAction.normalizeCurrentQueryRows(malformed));
    }
}
