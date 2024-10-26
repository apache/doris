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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.common.AnalysisException;

import java.util.Map;
import java.util.Set;

public class MTMVRefreshContext {
    private MTMV mtmv;
    private Map<String, Set<String>> partitionMappings;
    private MTMVBaseVersions baseVersions;

    public MTMVRefreshContext(MTMV mtmv) {
        this.mtmv = mtmv;
    }

    public MTMV getMtmv() {
        return mtmv;
    }

    public Map<String, Set<String>> getPartitionMappings() {
        return partitionMappings;
    }

    public MTMVBaseVersions getBaseVersions() {
        return baseVersions;
    }

    public static MTMVRefreshContext buildContext(MTMV mtmv) throws AnalysisException {
        MTMVRefreshContext context = new MTMVRefreshContext(mtmv);
        context.partitionMappings = mtmv.calculatePartitionMappings();
        context.baseVersions = MTMVPartitionUtil.getBaseVersions(mtmv);
        return context;
    }

}
