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
 * Statement-level input for internal IVM rewrite flows.
 */
public class IvmRewriteContext {
    public enum Mode {
        CREATE,
        INCREMENTAL,
        FULL
    }

    private final Mode mode;
    private final MTMV mtmv;
    private final boolean includeUpToDateStreams;
    private final boolean dryRun;

    public IvmRewriteContext(Mode mode, MTMV mtmv, boolean includeUpToDateStreams, boolean dryRun) {
        this.mode = Objects.requireNonNull(mode, "mode can not be null");
        this.mtmv = mode == Mode.CREATE ? mtmv : Objects.requireNonNull(mtmv, "mtmv can not be null");
        this.includeUpToDateStreams = includeUpToDateStreams;
        this.dryRun = dryRun;
    }

    public static IvmRewriteContext incremental(MTMV mtmv, boolean includeUpToDateStreams, boolean dryRun) {
        return new IvmRewriteContext(Mode.INCREMENTAL, mtmv, includeUpToDateStreams, dryRun);
    }

    public Mode getMode() {
        return mode;
    }

    public MTMV getMtmv() {
        return mtmv;
    }

    public boolean isIncludeUpToDateStreams() {
        return includeUpToDateStreams;
    }

    public boolean isDryRun() {
        return dryRun;
    }
}
