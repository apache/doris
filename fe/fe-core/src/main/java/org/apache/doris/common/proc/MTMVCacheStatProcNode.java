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
import org.apache.doris.mtmv.MTMVCacheManager;
import org.apache.doris.mtmv.MTMVCacheManager.Snapshot;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class MTMVCacheStatProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Name").add("Value").build();

    private final MTMVCacheManager manager;

    public MTMVCacheStatProcNode(MTMVCacheManager manager) {
        this.manager = manager;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        Snapshot s = manager.snapshot();
        result.addRow(Lists.newArrayList("size", String.valueOf(s.size)));
        result.addRow(Lists.newArrayList("hitCount", String.valueOf(s.hitCount)));
        result.addRow(Lists.newArrayList("missCount", String.valueOf(s.missCount)));
        result.addRow(Lists.newArrayList("evictionCount", String.valueOf(s.evictionCount)));
        result.addRow(Lists.newArrayList("hitRate", String.format("%.4f", s.hitRate)));
        return result;
    }
}
