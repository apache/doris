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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNumbersTVFScanRange;
import org.apache.doris.thrift.TScanRange;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// Table function that generate int64 numbers
// have a single column number
public class NumbersTableValuedFunction extends TableValuedFunctionInf {

    public static final String NAME = "numbers";

    private long totalNumbers;
    private int tabletsNum;

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
    public String getFuncName() {
        return NAME;
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
    public List<TableValuedFunctionTask> getTasks() throws AnalysisException {
        List<Backend> backendList = Lists.newArrayList();
        for (Backend be : Catalog.getCurrentSystemInfo().getIdToBackend().values()) {
            if (be.isAlive()) {
                backendList.add(be);
            }
        }
        if (backendList.isEmpty()) {
            throw new AnalysisException("No Alive backends");
        }
        Collections.shuffle(backendList);
        List<TableValuedFunctionTask> res = Lists.newArrayList();
        for (int i = 0; i < tabletsNum; ++i) {
            TScanRange scanRange = new TScanRange();
            TNumbersTVFScanRange numbers_tvf_scan_range = new TNumbersTVFScanRange();
            numbers_tvf_scan_range.setTotalNumbers(totalNumbers);
            scanRange.setNumbersTvfScanRange(numbers_tvf_scan_range);
            res.add(new TableValuedFunctionTask(backendList.get(i % backendList.size()), scanRange));
        }
        return null;
    }
}
