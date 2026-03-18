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

import java.util.Objects;

/**
 * Context passed to {@link IvmDeltaRewriter} during per-node delta plan rewriting.
 * Minimal skeleton — will be extended with driving-table info in future PRs.
 */
public class IvmDeltaRewriteContext {
    private final MTMV mtmv;

    public IvmDeltaRewriteContext(MTMV mtmv) {
        this.mtmv = Objects.requireNonNull(mtmv, "mtmv can not be null");
    }

    public MTMV getMtmv() {
        return mtmv;
    }
}
