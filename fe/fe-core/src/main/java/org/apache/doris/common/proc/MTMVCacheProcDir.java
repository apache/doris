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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mtmv.MTMVCacheManager;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/** Two-level proc dir for '/mtmv_cache': "stat" and "hot" child nodes. */
public class MTMVCacheProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Name").add("Info").build();

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        result.addRow(Lists.newArrayList("stat", "Global cache stats"));
        result.addRow(Lists.newArrayList("hot", "Top hot mtmv cache entries"));
        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String name) throws AnalysisException {
        if (Strings.isNullOrEmpty(name)) {
            throw new AnalysisException("mtmv_cache child name is empty");
        }
        MTMVCacheManager manager = Env.getCurrentEnv().getMtmvCacheManager();
        if (name.equalsIgnoreCase("stat")) {
            return new MTMVCacheStatProcNode(manager);
        }
        if (name.equalsIgnoreCase("hot")) {
            return new MTMVCacheHotProcNode(manager);
        }
        throw new AnalysisException("unknown mtmv_cache child: " + name);
    }
}
