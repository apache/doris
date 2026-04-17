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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.qe.ConnectContext;

import java.util.Map;
import java.util.Objects;

/**
 * Context passed to {@link IvmDeltaRewriter} during delta command construction.
 */
public class IvmDeltaRewriteContext {
    private final MTMV mtmv;
    private final ConnectContext connectContext;
    private final IvmNormalizeResult normalizeResult;
    /** Per-base-table stream refs with TSO info. Required for IvmDeltaRewriter. */
    private final Map<BaseTableInfo, IvmStreamRef> baseTableStreams;

    public IvmDeltaRewriteContext(MTMV mtmv, ConnectContext connectContext, IvmNormalizeResult normalizeResult) {
        this(mtmv, connectContext, normalizeResult, null);
    }

    public IvmDeltaRewriteContext(MTMV mtmv, ConnectContext connectContext,
            IvmNormalizeResult normalizeResult, Map<BaseTableInfo, IvmStreamRef> baseTableStreams) {
        this.mtmv = Objects.requireNonNull(mtmv, "mtmv can not be null");
        this.connectContext = Objects.requireNonNull(connectContext, "connectContext can not be null");
        this.normalizeResult = normalizeResult;
        this.baseTableStreams = baseTableStreams;
    }

    public MTMV getMtmv() {
        return mtmv;
    }

    public ConnectContext getConnectContext() {
        return connectContext;
    }

    /** Returns the IVM normalize result, or null if this is a non-agg scan-only MV. */
    public IvmNormalizeResult getNormalizeResult() {
        return normalizeResult;
    }

    /** Returns the per-base-table stream refs. */
    public Map<BaseTableInfo, IvmStreamRef> getBaseTableStreams() {
        return baseTableStreams;
    }
}
