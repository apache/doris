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

package org.apache.doris.statistics;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.thrift.TQueryColumn;

import mockit.Mock;
import mockit.MockUp;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Queue;
import java.util.Set;

public class FollowerColumnSenderTest {

    @Test
    public void testGetNeedAnalyzeColumns() {
        new MockUp<OlapTable>() {
            @Mock
            public Column getColumn(String name) {
                return new Column("col", PrimitiveType.INT);
            }

            @Mock
            public Set<Pair<String, String>> getColumnIndexPairs(Set<String> columns) {
                return Collections.singleton(Pair.of("mockIndex", "mockCol"));
            }
        };

        new MockUp<StatisticsUtil>() {
            boolean[] result = {false, true, false, true, true};
            int i = 0;
            @Mock
            public boolean needAnalyzeColumn(TableIf table, Pair<String, String> column) {
                return result[i++];
            }

            @Mock
            public TableIf findTable(long catalogId, long dbId, long tblId) {
                return new OlapTable();
            }
        };
        QueryColumn column1 = new QueryColumn(1, 2, 3, "col1");
        QueryColumn column2 = new QueryColumn(1, 2, 3, "col2");
        QueryColumn column3 = new QueryColumn(1, 2, 3, "col3");
        QueryColumn column4 = new QueryColumn(1, 2, 3, "col4");
        Queue<QueryColumn> queue = new BlockingArrayQueue<>();
        queue.add(column1);
        queue.add(column2);
        queue.add(column3);
        queue.add(column4);
        queue.add(column4);
        Assertions.assertEquals(5, queue.size());

        FollowerColumnSender sender = new FollowerColumnSender();
        Set<TQueryColumn> needAnalyzeColumns = sender.getNeedAnalyzeColumns(queue);
        Assertions.assertEquals(2, needAnalyzeColumns.size());
        Assertions.assertFalse(needAnalyzeColumns.contains(column1.toThrift()));
        Assertions.assertTrue(needAnalyzeColumns.contains(column2.toThrift()));
        Assertions.assertFalse(needAnalyzeColumns.contains(column3.toThrift()));
        Assertions.assertTrue(needAnalyzeColumns.contains(column4.toThrift()));
    }

}
