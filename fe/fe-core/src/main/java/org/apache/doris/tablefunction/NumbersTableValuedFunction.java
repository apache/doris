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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.DataGenScanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TDataGenFunctionName;
import org.apache.doris.thrift.TDataGenScanRange;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TTVFNumbersScanRange;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

// Table function that generate int64 numbers
// have a single column number

/**
 * The Implement of table valued function——numbers("number" = "N", "backend_num" = "M").
 */
public class NumbersTableValuedFunction extends DataGenTableValuedFunction {
    public static final String NAME = "numbers";
    public static final String NUMBER = "number";
    public static final String BACKEND_NUM = "backend_num";
    private static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(NUMBER)
            .add(BACKEND_NUM)
            .build();
    // The total numbers will be generated.
    private long totalNumbers;
    // The total backends will server it.
    private int tabletsNum;

    /**
     * Constructor.
     * @param params params from user
     * @throws UserException exception
     */
    public NumbersTableValuedFunction(Map<String, String> params) throws UserException {
        Map<String, String> validParams = Maps.newHashMap();
        for (String key : params.keySet()) {
            if (!PROPERTIES_SET.contains(key.toLowerCase())) {
                throw new AnalysisException(key + " is invalid property");
            }
            validParams.put(key.toLowerCase(), params.get(key));
        }

        try {
            tabletsNum = Integer.parseInt(validParams.getOrDefault(BACKEND_NUM, "1"));
        } catch (NumberFormatException e) {
            throw new UserException("can not parse `backend_num` param to natural number");
        }
        String numberStr = validParams.get(NUMBER);
        if (!Strings.isNullOrEmpty(numberStr)) {
            try {
                totalNumbers = Long.parseLong(numberStr);
            } catch (NumberFormatException e) {
                throw new UserException("can not parse `number` param to natural number");
            }
        } else {
            throw new UserException(
                    "can not find `number` param, please specify `number`, like: numbers(\"number\" = \"10\")");
        }
    }

    public long getTotalNumbers() {
        return totalNumbers;
    }

    public int getTabletsNum() {
        return tabletsNum;
    }

    @Override
    public TDataGenFunctionName getDataGenFunctionName() {
        return TDataGenFunctionName.NUMBERS;
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
        for (Backend be : Env.getCurrentSystemInfo().getIdToBackend().values()) {
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
            TDataGenScanRange dataGenScanRange = new TDataGenScanRange();
            TTVFNumbersScanRange tvfNumbersScanRange = new TTVFNumbersScanRange();
            tvfNumbersScanRange.setTotalNumbers(totalNumbers);
            dataGenScanRange.setNumbersParams(tvfNumbersScanRange);
            scanRange.setDataGenScanRange(dataGenScanRange);
            res.add(new TableValuedFunctionTask(backendList.get(i % backendList.size()), scanRange));
        }
        return res;
    }

    @Override
    public ScanNode getScanNode(PlanNodeId id, TupleDescriptor desc) {
        return new DataGenScanNode(id, desc, "DataGenScanNode", this);
    }
}
