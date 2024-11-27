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

package org.apache.doris.datasource.hudi.source;

import org.apache.doris.spi.Split;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.exception.HoodieException;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class EmptyIncrementalRelation implements IncrementalRelation {
    private static final String EMPTY_TS = "000";

    private final Map<String, String> optParams;

    public EmptyIncrementalRelation(Map<String, String> optParams) {
        this.optParams = optParams;
    }

    @Override
    public List<FileSlice> collectFileSlices() throws HoodieException {
        return Collections.emptyList();
    }

    @Override
    public List<Split> collectSplits() throws HoodieException {
        return Collections.emptyList();
    }

    @Override
    public Map<String, String> getHoodieParams() {
        optParams.put("hoodie.datasource.read.incr.operation", "true");
        optParams.put("hoodie.datasource.read.begin.instanttime", EMPTY_TS);
        optParams.put("hoodie.datasource.read.end.instanttime", EMPTY_TS);
        optParams.put("hoodie.datasource.read.incr.includeStartTime", "true");
        return optParams;
    }

    @Override
    public boolean fallbackFullTableScan() {
        return false;
    }

    @Override
    public boolean isIncludeStartTime() {
        return true;
    }

    @Override
    public String getStartTs() {
        return EMPTY_TS;
    }

    @Override
    public String getEndTs() {
        return EMPTY_TS;
    }
}
