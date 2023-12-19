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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TDataGenFunctionName;
import org.apache.doris.thrift.TDataGenScanRange;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TTVFNumbersScanRange;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

// Table function that generate int64 numbers
// have a single column number

/**
 * The Implement of table valued function——numbers("number" = "N").
 */
public class NumbersTableValuedFunction extends DataGenTableValuedFunction {
    public static final String NAME = "numbers";
    public static final String NUMBER = "number";
    public static final String CONST_VALUE = "const_value";
    private static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(NUMBER)
            .add(CONST_VALUE)
            .build();
    // The total numbers will be generated.
    private long totalNumbers;
    private boolean useConst = false;
    private long constValue;

    /**
     * Constructor.
     * @param params params from user
     * @throws AnalysisException exception
     */
    public NumbersTableValuedFunction(Map<String, String> params) throws AnalysisException {
        if (!params.containsKey(NUMBER)) {
            throw new AnalysisException("number not set");
        }
        for (String key : params.keySet()) {
            if (PROPERTIES_SET.contains(key)) {
                try {
                    switch (key) {
                        case NUMBER:
                            totalNumbers = Long.parseLong(params.get(key));
                            break;
                        case CONST_VALUE:
                            useConst = true;
                            constValue = Long.parseLong(params.get(key));
                            break;
                        default:
                            break;
                    }
                } catch (NumberFormatException e) {
                    throw new AnalysisException("cannot parse param value " + params.get(key));
                }
            }
        }
    }

    public long getTotalNumbers() {
        return totalNumbers;
    }

    public boolean getUseConst() {
        return useConst;
    }

    public long getConstValue() {
        return constValue;
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
        TScanRange scanRange = new TScanRange();
        TDataGenScanRange dataGenScanRange = new TDataGenScanRange();
        TTVFNumbersScanRange tvfNumbersScanRange = new TTVFNumbersScanRange().setTotalNumbers(totalNumbers)
                .setUseConst(useConst).setConstValue(constValue);
        dataGenScanRange.setNumbersParams(tvfNumbersScanRange);
        scanRange.setDataGenScanRange(dataGenScanRange);
        res.add(new TableValuedFunctionTask(backendList.get(0), scanRange));
        return res;
    }
}
