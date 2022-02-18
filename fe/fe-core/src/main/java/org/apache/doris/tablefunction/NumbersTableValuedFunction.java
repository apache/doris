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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;

import java.util.ArrayList;
import java.util.List;

// Table function that generate int64 numbers
// have a single column number
public class NumbersTableValuedFunction extends TableValuedFunctionInf {

    public static final String NAME = "numbers";

    private static final String SCANNODE_NAME = "NUMBERS";

    private long totalNumbers;
    private int tabletsNum = 1;

    public NumbersTableValuedFunction(List<String> params) throws UserException {
        if (params.size() < 1 || params.size() > 2) {
            throw new UserException(
                    "numbers table function only support numbers(10000 /*total numbers*/) or numbers(10000, 2 /*number of tablets to run*/)");
        }
        totalNumbers = Long.parseLong(params.get(0));
        if (params.size() == 2) {
            tabletsNum = Integer.parseInt(params.get(1));
        }
    }

    @Override
    public String getTableName() {
        return "NumbersTableValuedFunction";
    }

    @Override
    public List<Column> getTableColumns() {
        List<Column> resColumns = new ArrayList<>();
        resColumns.add(new Column("number", PrimitiveType.BIGINT, false));
        return resColumns;
    }

    @Override
    public ScanNode getScanNode(PlanNodeId id, TupleDescriptor desc) {
        return new NumbersTableValuedFunctionScanNode(id, desc, SCANNODE_NAME, totalNumbers, tabletsNum);
    }

}
