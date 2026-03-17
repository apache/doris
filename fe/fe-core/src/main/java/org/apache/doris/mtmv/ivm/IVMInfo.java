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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.mtmv.BaseTableInfo;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.Map;

/**
 * Thin persistent IVM metadata stored on MTMV.
 */
public class IVMInfo {
    @SerializedName("bb")
    private boolean binlogBroken = false;

    @SerializedName("bs")
    private Map<BaseTableInfo, IVMStreamRef> baseTableStreams;

    public IVMInfo() {
        this.baseTableStreams = Maps.newHashMap();
    }

    public boolean isBinlogBroken() {
        return binlogBroken;
    }

    public void setBinlogBroken(boolean binlogBroken) {
        this.binlogBroken = binlogBroken;
    }

    public Map<BaseTableInfo, IVMStreamRef> getBaseTableStreams() {
        return baseTableStreams;
    }

    public void setBaseTableStreams(Map<BaseTableInfo, IVMStreamRef> baseTableStreams) {
        this.baseTableStreams = baseTableStreams;
    }

    @Override
    public String toString() {
        return "IVMInfo{"
                + "binlogBroken=" + binlogBroken
                + ", baseTableStreams=" + baseTableStreams
                + '}';
    }
}
