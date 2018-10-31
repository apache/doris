// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.planner;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;

import org.junit.Test;

import java.util.List;

import mockit.Injectable;
import mockit.Mocked;
import mockit.NonStrictExpectations;

public class StreamLoadPlannerTest {
    @Injectable
    Database db;

    @Injectable
    OlapTable destTable;

    @Mocked
    StreamLoadScanNode scanNode;

    @Mocked
    OlapTableSink sink;

    @Test
    public void testNormalPlan() throws UserException {
        List<Column> columns = Lists.newArrayList();
        Column c1 = new Column("c1", PrimitiveType.BIGINT, false);
        columns.add(c1);
        Column c2 = new Column("c2", PrimitiveType.BIGINT, true);
        columns.add(c2);
        new NonStrictExpectations() {
            {
                destTable.getBaseSchema();
                result = columns;
                scanNode.init((Analyzer) any);
                scanNode.getChildren();
                result = Lists.newArrayList();
                scanNode.getId();
                result = new PlanNodeId(5);
            }
        };
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setTxnId(1);
        request.setLoadId(new TUniqueId(2, 3));
        StreamLoadPlanner planner = new StreamLoadPlanner(db, destTable, request);
        planner.plan();
    }
}